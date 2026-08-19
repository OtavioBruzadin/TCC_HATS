"""
HATS report generator, com aceleração opcional por numpy.

Mesma interface, mesmas saídas e mesmos relatórios do hats_report_v4.py. A única
diferença é o caminho quente da leitura do .rbd: quando numpy está disponível, a
decodificação, a estatística e a demodulação rodam vetorizadas; quando não está,
cai automaticamente no caminho stdlib do v4, sem perder nenhuma funcionalidade.

    python3 hats_report_v5.py --from-data-dir --export-csv
    python3 hats_report_v5.py --from-data-dir --backend stdlib     # força o v4
    python3 hats_report_v5.py --backends                           # o que há disponível

Todo o resto — parsing dos XML, leitura do .aux, estação meteorológica, exportação
CSV/JSON, correções de unidade, filtro de registros defasados — é reaproveitado do
v4. Este módulo não duplica nada disso.

Por que isso é mais rápido que o pipeline de referência do CRAAM
---------------------------------------------------------------
O getFFT() do HATS.py grava o sinal calibrado e os husec em dois arquivos temporários
(cerca de 58 MB por hora de dados), lança o binário HATS_fft, que relê tudo, calcula
e regrava o resultado, e então volta a ler. O vai-e-volta em disco custa mais que a
conta em si. Aqui as janelas sobrepostas são montadas com sliding_window_view e a
projeção vira uma multiplicação matriz-vetor em BLAS, tudo em memória.

Equivalência numérica
---------------------
Verificada contra o HATS.py de referência e contra o caminho stdlib. Ver a seção
"Desempenho" e "Validação" do README, e tools/compare_with_reference.py.
"""

import argparse
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import hats_report_v4 as base


# Registros lidos por bloco no caminho numpy. O v4 usa 16384, calibrado para o
# interpretador; numpy prefere blocos bem maiores porque o custo por chamada é o
# que domina. Medido numa hora de dados: 65536 -> 0,40 s / 58 MB;
# 262144 -> 0,26 s / 117 MB; 1048576 -> 0,33 s / 327 MB. O joelho está em 262144.
NUMPY_CHUNK_RECORDS = 262144

NUMPY_FORMAT_MAP = {
    "i": "<i4",
    "I": "<u4",
    "H": "<u2",
    "Q": "<u8",
    "d": "<f8",
}


def numpy_available():
    try:
        import numpy  # noqa: F401
    except ImportError:
        return False
    return True


def numpy_dtype_from_schema(schema):
    """
    Constrói o dtype estruturado equivalente ao struct_format do esquema.

    numpy monta dtypes de lista sem alinhamento, igual ao struct com prefixo '<',
    então o itemsize tem de bater com o record_size. A conferência é explícita
    porque um desencontro aqui deslocaria silenciosamente todos os campos.
    """
    import numpy as np

    fields = []
    for field in schema["fields"]:
        numpy_format = NUMPY_FORMAT_MAP.get(field["fmt"])
        if numpy_format is None:
            raise ValueError("Sem mapeamento numpy para o formato struct '{}'".format(field["fmt"]))
        dimension = field.get("dim", 1)
        if dimension == 1:
            fields.append((field["name"], numpy_format))
        else:
            fields.append((field["name"], numpy_format, dimension))

    dtype = np.dtype(fields)
    if dtype.itemsize != schema["record_size"]:
        raise ValueError(
            "dtype numpy tem itemsize {} mas o esquema diz {}".format(dtype.itemsize, schema["record_size"]))
    return dtype


def numpy_decode_ad7770(column):
    """
    24 bits significativos num inteiro de 4 bytes, sinal no bit 23.

    Sem ramo: quando o bit 23 está ligado, (v & 0x800000) << 1 vale exatamente o
    excesso 0x1000000. Os dois termos cabem em int32, então não há overflow.
    """
    return (column & 0x00FFFFFF) - ((column & 0x800000) << 1)


def numpy_projection(window_size, target_frequency, sampling_frequency, bin_mode):
    """Janela flat-top já multiplicada pelos twiddles, como vetor complexo."""
    import numpy as np

    indices = np.arange(window_size, dtype=np.float64)
    denominator = float(window_size - 1) if window_size > 1 else 1.0
    window = base.FLATTOP_CORRECTION * (
        1.0
        - 1.9330 * np.cos(2.0 * np.pi * indices / denominator)
        + 1.2860 * np.cos(4.0 * np.pi * indices / denominator)
        - 0.3880 * np.cos(6.0 * np.pi * indices / denominator)
        + 0.0322 * np.cos(8.0 * np.pi * indices / denominator)
    )
    k = base.goertzel_bin(target_frequency, window_size, sampling_frequency, bin_mode)
    return window * np.exp(-1j * 2.0 * np.pi * k * indices / window_size)


def numpy_analyse_rbd(path, schema, options):
    """
    Equivalente vetorizado de base.analyse_rbd, com a mesma estrutura de retorno.

    Lê em blocos e mantém apenas a cauda necessária para as janelas sobrepostas,
    então o pico de memória não acompanha o tamanho do arquivo.
    """
    import numpy as np
    from numpy.lib.stride_tricks import sliding_window_view

    record_size = schema["record_size"]
    file_size = path.stat().st_size
    if file_size % record_size != 0:
        raise ValueError("File {} has size {}, not divisible by record size {}".format(
            path.name, file_size, record_size))

    date_str = base.detect_date_from_name(path)
    hour_str = base.detect_hour_from_name(path)
    hour_start_husec = int(hour_str[:2]) * base.HUSEC_PER_HOUR if hour_str and hour_str[:2].isdigit() else None

    total_records = file_size // record_size
    if options["record_limit"] is not None:
        total_records = min(total_records, options["record_limit"])

    dtype = numpy_dtype_from_schema(schema)
    converted_fields = [f for f in schema["fields"] if f.get("convert") == "yes" or f.get("origin") == "ad7770"]
    accumulators = {f["name"]: [None, None, 0.0, 0.0, 0] for f in converted_fields}

    golay_field = next((f for f in schema["fields"] if f["name"] == "golay"), None)
    has_husec = "husec" in dtype.names
    has_sample = "sample" in dtype.names

    window_size = options["window_size"]
    steps = options["steps"]
    demodulating = (options["demodulate"] and golay_field is not None and has_husec
                    and total_records >= window_size)
    if demodulating:
        # HATS_fft.c: num_windows = floor((nrec - window_size + 1) / steps).
        num_windows = (total_records - window_size + 1) // steps
        demodulating = num_windows > 0
    if demodulating:
        projection = numpy_projection(window_size, options["target_frequency"],
                                      options["sampling_frequency"], options["bin_mode"])
        half = window_size // 2
        amplitude_chunks = []
        husec_chunks = []
        carry_signal = np.empty(0, dtype=np.float64)
        carry_husec = np.empty(0, dtype=np.uint64)
        consumed = 0
        window_start = 0
        emitted = 0

    total = 0
    before_hour = 0
    sample_leaps = 0
    husec_leaps = 0
    last_sample = None
    last_husec = None
    first_husec = None
    final_husec = None

    with path.open("rb") as handle:
        while total < total_records:
            wanted = min(NUMPY_CHUNK_RECORDS, total_records - total)
            block = np.fromfile(handle, dtype=dtype, count=wanted)
            if block.size == 0:
                break
            count = block.size
            total += count

            if has_husec:
                husec_column = block["husec"]
                if first_husec is None:
                    first_husec = int(husec_column[0])
                final_husec = int(husec_column[-1])
                if hour_start_husec is not None:
                    before_hour += int(np.count_nonzero(husec_column < hour_start_husec))
                if count > 1:
                    husec_leaps += int(np.count_nonzero(np.diff(husec_column.astype(np.int64)) != base.HUSEC_PER_SAMPLE))
                if last_husec is not None and int(husec_column[0]) - last_husec != base.HUSEC_PER_SAMPLE:
                    husec_leaps += 1
                last_husec = int(husec_column[-1])

            if has_sample:
                sample_column = block["sample"]
                if count > 1:
                    sample_leaps += int(np.count_nonzero(np.diff(sample_column.astype(np.int64)) != 1))
                if last_sample is not None and int(sample_column[0]) - last_sample != 1:
                    sample_leaps += 1
                last_sample = int(sample_column[-1])

            for field in converted_fields:
                column = block[field["name"]]
                if field.get("origin") == "ad7770":
                    column = numpy_decode_ad7770(column)
                as_float = column.astype(np.float64)
                accumulator = accumulators[field["name"]]
                lowest = float(as_float.min())
                highest = float(as_float.max())
                accumulator[0] = lowest if accumulator[0] is None else min(accumulator[0], lowest)
                accumulator[1] = highest if accumulator[1] is None else max(accumulator[1], highest)
                accumulator[2] += float(as_float.sum())
                accumulator[3] += float(np.dot(as_float, as_float))
                accumulator[4] += count

                if demodulating and field is golay_field:
                    calibrated = as_float * field.get("slope", 1.0) + field.get("offset", 0.0)
                    carry_signal = np.concatenate((carry_signal, calibrated))
                    carry_husec = np.concatenate((carry_husec, block["husec"].astype(np.uint64)))

            if demodulating and carry_signal.size >= window_size:
                # Todas as janelas inteiramente contidas no que já foi lido, de uma
                # vez só: uma multiplicação matriz-vetor em vez de um laço.
                last_start = total - window_size
                if window_start <= last_start:
                    available = (last_start - window_start) // steps + 1
                    available = min(available, num_windows - emitted)
                    if available > 0:
                        local = window_start - consumed
                        span = (available - 1) * steps + window_size
                        views = sliding_window_view(carry_signal[local:local + span], window_size)[::steps]
                        amplitude_chunks.append(np.abs(views @ projection) / window_size)
                        centres = local + half + steps * np.arange(available)
                        husec_chunks.append(carry_husec[centres])
                        window_start += available * steps
                        emitted += available
                if window_start > consumed:
                    drop = window_start - consumed
                    carry_signal = carry_signal[drop:]
                    carry_husec = carry_husec[drop:]
                    consumed = window_start

    result = {
        "total_records": total,
        "first_time_utc": base.dt_from_husec(date_str, first_husec) if first_husec is not None else None,
        "last_time_utc": base.dt_from_husec(date_str, final_husec) if final_husec is not None else None,
        "integrity": {
            "sample_number_leaps": sample_leaps,
            "husec_leaps": husec_leaps,
            "records_before_nominal_hour": before_hour,
            "note": "HATS.py drops records with husec < hour*36000000; they are kept here and only counted.",
        },
        "statistics_adcu": {},
        "statistics_calibrated": {},
        "units_calibrated": {f["name"]: f.get("converted_unit") for f in converted_fields},
    }

    for field in converted_fields:
        name = field["name"]
        result["statistics_adcu"][name] = base.summarize_accumulator(accumulators[name])
        if field.get("convert") == "yes":
            result["statistics_calibrated"][name] = base.scale_summary(
                result["statistics_adcu"][name], field.get("slope", 1.0), field.get("offset", 0.0))

    if demodulating and amplitude_chunks:
        import numpy as np
        amplitudes = np.concatenate(amplitude_chunks)
        husecs = np.concatenate(husec_chunks)
        exact_bin = options["target_frequency"] * window_size / options["sampling_frequency"]
        used_bin = base.goertzel_bin(options["target_frequency"], window_size,
                                     options["sampling_frequency"], options["bin_mode"])
        mean = float(amplitudes.mean())
        result["demodulation"] = {
            "method": "flattop_single_bin_dft_numpy",
            "target_frequency_hz": options["target_frequency"],
            "sampling_frequency_hz": options["sampling_frequency"],
            "window_size": window_size,
            "steps": steps,
            "output_rate_hz": options["sampling_frequency"] / steps,
            "bin_mode": options["bin_mode"],
            "bin_exact": exact_bin,
            "bin_used": used_bin,
            "bin_frequency_hz": used_bin * options["sampling_frequency"] / window_size,
            "windows": int(amplitudes.size),
            "unit": golay_field.get("converted_unit") if golay_field else None,
            "amplitude": {
                "count": int(amplitudes.size),
                "min": float(amplitudes.min()),
                "max": float(amplitudes.max()),
                "mean": mean,
                "sd": float(math.sqrt(max(0.0, float((amplitudes * amplitudes).mean()) - mean * mean))),
            },
            "first_time_utc": base.dt_from_husec(date_str, int(husecs[0])),
            "last_time_utc": base.dt_from_husec(date_str, int(husecs[-1])),
        }
        result["_deconv"] = (husecs.tolist(), amplitudes.tolist(), date_str)

    return result


def husec_to_isoformat(date_str, husec):
    """
    Mesma saída de base.dt_from_husec, sem construir um datetime por registro.

    Numa hora de dados são 3,6 milhões de chamadas; a construção do objeto e o
    isoformat dominam a exportação. Aritmética inteira faz o mesmo trabalho.
    Note que datetime.isoformat() omite a parte fracionária quando ela é zero, e
    isso é reproduzido aqui para as saídas continuarem idênticas.
    """
    hours = husec // 36000000
    if hours >= 24:
        # Vira o dia: raro, e o caminho lento já trata corretamente.
        return base.dt_from_husec(date_str, husec)
    remainder = husec % 36000000
    minutes = remainder // 600000
    remainder %= 600000
    seconds = remainder // 10000
    microseconds = (remainder % 10000) * 100
    if microseconds:
        return "{}T{:02d}:{:02d}:{:02d}.{:06d}+00:00".format(
            date_str, hours, minutes, seconds, microseconds)
    return "{}T{:02d}:{:02d}:{:02d}+00:00".format(date_str, hours, minutes, seconds)


def unix_second_formatter():
    """
    Devolve uma função sec -> 'YYYY-MM-DDTHH:MM:SS' com cache.

    Numa hora de dados há 3600 segundos distintos para 3,6 milhões de registros,
    então o datetime é construído uma vez a cada mil linhas.
    """
    from datetime import datetime

    cache = {}

    def format_second(second):
        text = cache.get(second)
        if text is None:
            text = datetime.fromtimestamp(second, tz=base.UTC).strftime("%Y-%m-%dT%H:%M:%S")
            cache[second] = text
        return text

    return format_second


def export_rbd_csv_numpy(path, out_csv, schema, limit=None):
    """
    Exportador dedicado do CSV do sinal bruto, acelerado por numpy.

    Só é chamado quando --export-rbd-csv é pedido explicitamente: são cerca de
    3,6 milhões de linhas e 767 MB por hora de dados, contra 0,4 s de análise.

    Produz exatamente as mesmas linhas que base.export_rbd_csv. As diferenças são
    de execução: decodificação e calibração vetorizadas, timestamps formatados por
    aritmética inteira em vez de objetos datetime, e writerows() em lote no lugar
    de uma chamada writerow() por registro.
    """
    import csv
    import numpy as np

    dtype = numpy_dtype_from_schema(schema)
    date_str = base.detect_date_from_name(path)
    format_second = unix_second_formatter()

    headers = []
    for field in schema["fields"]:
        headers.append(field["name"])
        if field.get("origin") == "ad7770" or field.get("convert") == "yes":
            headers.append(field["name"] + "_interpreted")
    headers.extend(["datetime_utc", "datetime_utc_from_husec"])

    total_records = path.stat().st_size // schema["record_size"]
    if limit is not None:
        total_records = min(total_records, limit)

    written = 0
    with path.open("rb") as handle, out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(headers)

        while written < total_records:
            wanted = min(NUMPY_CHUNK_RECORDS, total_records - written)
            block = np.fromfile(handle, dtype=dtype, count=wanted)
            if block.size == 0:
                break
            written += block.size

            columns = []
            for field in schema["fields"]:
                raw = block[field["name"]]
                # A primeira coluna é o valor cru, como no exportador do v4.
                columns.append(raw.tolist())
                if field.get("origin") == "ad7770" or field.get("convert") == "yes":
                    value = numpy_decode_ad7770(raw) if field.get("origin") == "ad7770" else raw
                    if field.get("convert") == "yes":
                        value = value.astype(np.float64) * field.get("slope", 1.0) + field.get("offset", 0.0)
                    columns.append(value.tolist())

            if "sec" in dtype.names and "ms" in dtype.names:
                seconds = block["sec"].tolist()
                milliseconds = block["ms"].tolist()
                columns.append([
                    "{}.{:06d}+00:00".format(format_second(sec), ms * 1000) if ms
                    else "{}+00:00".format(format_second(sec))
                    for sec, ms in zip(seconds, milliseconds)
                ])
            else:
                columns.append([None] * block.size)

            if "husec" in dtype.names:
                columns.append([husec_to_isoformat(date_str, int(h)) for h in block["husec"].tolist()])
            else:
                columns.append([None] * block.size)

            writer.writerows(zip(*columns))


def resolve_rbd_csv_exporter(backend_name):
    """O exportador dedicado só existe no caminho numpy; sem ele, usa o do v4."""
    if backend_name == "numpy":
        return export_rbd_csv_numpy
    return base.export_rbd_csv


def resolve_backend(requested):
    """Devolve (nome_do_backend, analisador). 'auto' prefere numpy quando existe."""
    if requested == "stdlib":
        return "stdlib", base.analyse_rbd
    if requested == "numpy":
        if not numpy_available():
            raise SystemExit("Backend 'numpy' pedido, mas numpy não está instalado.")
        return "numpy", numpy_analyse_rbd
    if numpy_available():
        return "numpy", numpy_analyse_rbd
    return "stdlib", base.analyse_rbd


def build_argument_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--data-dir", default="Data")
    parser.add_argument("--reports-dir", default="Reports")
    parser.add_argument("--xml-dir", default="XMLTables")
    parser.add_argument("--init-project", action="store_true")
    parser.add_argument("--sample-count", type=int, default=5)
    parser.add_argument("--export-csv", action="store_true")
    parser.add_argument("--csv-limit", type=int, default=None)
    parser.add_argument("--export-rbd-csv", action="store_true",
                        help="Também exporta o CSV do sinal bruto de 1 kHz, pelo exportador "
                             "dedicado. Desligado por padrão: ~3,6 M linhas e ~767 MB por hora.")
    parser.add_argument("--from-data-dir", action="store_true")
    parser.add_argument("--day", default=None)
    parser.add_argument("--record-limit", type=int, default=None)
    parser.add_argument("--no-demod", action="store_true")
    parser.add_argument("--fft-window", type=int, default=base.WINDOW_SIZE)
    parser.add_argument("--fft-steps", type=int, default=base.STEPS)
    parser.add_argument("--fft-target-hz", type=float, default=base.TARGET_FREQUENCY)
    parser.add_argument("--fft-sampling-hz", type=float, default=base.SAMPLING_FREQUENCY)
    parser.add_argument("--fft-bin-mode", choices=["reference", "exact"], default="reference")
    parser.add_argument("--backend", choices=["auto", "numpy", "stdlib"], default="auto",
                        help="auto usa numpy quando disponível e cai no stdlib quando não.")
    parser.add_argument("--backends", action="store_true", help="Mostra os backends disponíveis e sai.")
    return parser


def main():
    args = build_argument_parser().parse_args()

    if args.backends:
        print("stdlib : sempre disponível")
        print("numpy  : {}".format("disponível" if numpy_available() else "não instalado"))
        print("auto   -> {}".format(resolve_backend("auto")[0]))
        return

    backend_name, analyser = resolve_backend(args.backend)

    project_root = Path(args.project_root).resolve()
    paths = base.ensure_project_structure(project_root, args.data_dir, args.reports_dir, args.xml_dir)

    if args.init_project:
        print("Project initialized at {}".format(project_root))
        return

    if not args.from_data_dir:
        raise SystemExit("Use --from-data-dir to process the Data folder.")

    options = {
        "demodulate": not args.no_demod,
        "window_size": args.fft_window,
        "steps": args.fft_steps,
        "target_frequency": args.fft_target_hz,
        "sampling_frequency": args.fft_sampling_hz,
        "bin_mode": args.fft_bin_mode,
        "record_limit": args.record_limit,
        "analyser": analyser,
        "backend": backend_name,
        "export_rbd_csv": args.export_rbd_csv,
        "rbd_csv_exporter": resolve_rbd_csv_exporter(backend_name),
    }

    rbd_schema = base.load_schema(paths["xml_dir"], "rbd")
    aux_schema = base.load_schema(paths["xml_dir"], "aux")
    day_index = base.build_day_index(paths["data_dir"])

    if args.day:
        day_index = {k: v for k, v in day_index.items() if k == args.day}
    if not day_index:
        raise SystemExit("No day folders found inside {}.".format(paths["data_dir"]))

    print("Backend: {}".format(backend_name))
    reports = {}
    for day_key, day_info in day_index.items():
        reports[day_key] = base.process_day(day_key, day_info, paths["json_dir"], paths["csv_dir"],
                                            rbd_schema, aux_schema, args.sample_count,
                                            args.export_csv, args.csv_limit, options)

    serialisable_options = {k: v for k, v in options.items()
                            if k not in ("analyser", "rbd_csv_exporter")}
    summary = {
        "project_root": str(project_root),
        "data_dir": str(paths["data_dir"]),
        "reports_dir": str(paths["reports_dir"]),
        "xml_dir": str(paths["xml_dir"]),
        "rbd_schema_mode": rbd_schema.get("mode"),
        "aux_schema_mode": aux_schema.get("mode"),
        "rbd_schema_source": rbd_schema.get("source"),
        "aux_schema_source": aux_schema.get("source"),
        "backend": backend_name,
        "demodulation_options": serialisable_options,
        "aux_unit_corrections": {
            name: {"declared_in_xml": declared, "actual": actual, "corrected_field": new_name, "factor": factor}
            for name, (declared, actual, factor, new_name) in base.AUX_UNIT_FIXES.items()
        },
        "days_processed": list(reports.keys()),
        # Referências, não cópias: cada day_report já está no seu próprio arquivo.
        "day_reports": {day: "{}__day_report.json".format(day) for day in reports},
    }
    base.write_json(summary, paths["reports_dir"] / "summary.json")
    print("Processed {} day(s).".format(len(reports)))
    print("Summary written to: {}".format(paths["reports_dir"] / "summary.json"))


if __name__ == "__main__":
    main()
