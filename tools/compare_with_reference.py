"""
Compara a saída deste projeto com a do HATS.py original do CRAAM.

Roda o pipeline de referência (Docs/upstream/HATS.py) e o deste projeto
(hats_report_v4.py) sobre o mesmo par .rbd/.aux, e reporta as diferenças campo a
campo.

IMPORTANTE: este script precisa ser executado com um interpretador que tenha
numpy, scipy, pandas e astropy, porque o HATS.py de referência depende deles.
O projeto em si continua sem dependências — só esta comparação precisa delas.

    python3 -m venv /tmp/refvenv
    /tmp/refvenv/bin/pip install numpy scipy pandas astropy

    /tmp/refvenv/bin/python tools/compare_with_reference.py \
        --day-dir Data/2026-03-17 --date 2026-03-17 --hour 1800 \
        --fft-program /caminho/para/HATS_fft

O binário HATS_fft distribuído no zip é ELF Linux x86-64. Em macOS/arm64 compile
a partir de Docs/upstream (veja o README de lá).

O que esperar, com base na validação já feita:

  rData, cData, aux    diferença exatamente zero
  Deconv               ~3e-05 relativo, do off-by-one no Goertzel do windowed_dft.c
  contagem de registros o HATS.py descarta husec < hora*36000000; este projeto mantém
"""

import argparse
import json
import os
import sys
import tempfile
from array import array
from pathlib import Path


def build_argument_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--day-dir", required=True,
                        help="Pasta do dia, com o .rbd na raiz e o .aux em aux/.")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--hour", required=True, help="HHMM, por exemplo 1800")
    parser.add_argument("--xml-dir", default="XMLTables")
    parser.add_argument("--upstream-dir", default="Docs/upstream")
    parser.add_argument("--project-dir", default=".")
    parser.add_argument("--fft-program", default=None,
                        help="Caminho do binário HATS_fft. Default: <upstream-dir>/HATS_fft.")
    parser.add_argument("--json-out", default=None, help="Grava o resultado em JSON.")
    parser.add_argument("--fft-bin-mode", choices=["reference", "exact"], default="reference")
    parser.add_argument("--backend", choices=["auto", "numpy", "stdlib"], default="auto")
    return parser


def load_reference(upstream_dir, xml_dir, day_dir, fft_program, date, hour):
    """Importa e executa o HATS.py de referência num diretório de trabalho temporário."""
    try:
        import numpy  # noqa: F401
    except ImportError:
        raise SystemExit(
            "numpy não encontrado. Este script precisa de um interpretador com "
            "numpy/scipy/pandas/astropy (veja o cabeçalho do arquivo)."
        )

    os.environ["HATSXMLPATH"] = str(xml_dir)
    os.environ["HATS_DATA_InputPath"] = str(day_dir)
    os.environ["HATS_FFTProgram"] = str(fft_program)

    sys.path.insert(0, str(upstream_dir))
    import HATS

    # getFFT escreve hats_data_rbd.bin / hats_husec.bin no diretório corrente,
    # então roda num diretório descartável para não sujar o projeto.
    previous_cwd = Path.cwd()
    with tempfile.TemporaryDirectory() as scratch:
        os.chdir(scratch)
        try:
            reference = HATS.hats("{} {}".format(date, hour))
        finally:
            os.chdir(previous_cwd)

    return HATS, reference


def compare(args):
    import numpy as np

    project_dir = Path(args.project_dir).resolve()
    sys.path.insert(0, str(project_dir))

    from hats import backends, calibration, demodulation, records, schema as schema_module

    backend_name, analyser, _exporter = backends.resolve(args.backend)
    print("Comparando o pacote hats   backend: {}".format(backend_name))

    xml_dir = (project_dir / args.xml_dir) if not Path(args.xml_dir).is_absolute() else Path(args.xml_dir)
    upstream_dir = (project_dir / args.upstream_dir) if not Path(args.upstream_dir).is_absolute() else Path(args.upstream_dir)
    day_dir = Path(args.day_dir).resolve()
    fft_program = Path(args.fft_program).resolve() if args.fft_program else (upstream_dir / "HATS_fft")

    if not fft_program.exists():
        raise SystemExit("HATS_fft não encontrado em {}. Compile-o (veja {}/README.md).".format(fft_program, upstream_dir))

    stem = "hats-{}T{}".format(args.date, args.hour)
    rbd_path = day_dir / (stem + ".rbd")
    aux_path = day_dir / "aux" / (stem + ".aux")
    for path in (rbd_path, aux_path):
        if not path.exists():
            raise SystemExit("Arquivo não encontrado: {}".format(path))

    HATS, reference = load_reference(upstream_dir, xml_dir, day_dir, fft_program, args.date, args.hour)
    print("HATS.py de referência: {}".format(HATS.__version__))

    rbd_schema = schema_module.load(xml_dir, "rbd")
    aux_schema = schema_module.load(xml_dir, "aux")

    report = {
        "reference_version": HATS.__version__,
        "rbd_file": str(rbd_path),
        "aux_file": str(aux_path),
        "fft_bin_mode": args.fft_bin_mode,
        "backend": backend_name,
    }

    # ---------------------------------------------------------------- RBD ----
    field_index = {f["name"]: i for i, f in enumerate(rbd_schema["fields"])}
    mine = list(records.iter_records(rbd_path, rbd_schema))
    ref_raw = reference.rbd.rData
    ref_cal = reference.rbd.cData

    # A referência descarta registros do início, então alinha por husec em vez de
    # supor que o que sobrou é um prefixo.
    position_of_husec = {int(values[field_index["husec"]]): i for i, values in enumerate(mine)}
    aligned = [position_of_husec.get(int(h)) for h in ref_raw["husec"]]
    missing = sum(1 for a in aligned if a is None)

    print()
    print("=" * 72)
    print("RBD   referência = {} registros    projeto = {} registros".format(ref_raw.shape[0], len(mine)))
    print("=" * 72)
    if missing:
        print("  ATENÇÃO: {} husec da referência não existem na leitura do projeto".format(missing))

    rbd_diffs = {}
    print("  {:<12} {:>18} {:>18}".format("campo", "dif máx (ADCu)", "dif máx (calib)"))
    for field in rbd_schema["fields"]:
        name = field["name"]
        raw_delta = 0
        cal_delta = None
        for ref_position, my_position in enumerate(aligned):
            if my_position is None:
                continue
            value = mine[my_position][field_index[name]]
            if field.get("origin") == "ad7770":
                value = calibration.decode_ad7770(value)
            raw_delta = max(raw_delta, abs(int(value) - int(ref_raw[name][ref_position])))
            if field.get("convert") == "yes":
                calibrated = value * field["slope"] + field["offset"]
                delta = abs(calibrated - float(ref_cal[name][ref_position]))
                cal_delta = delta if cal_delta is None else max(cal_delta, delta)
        rbd_diffs[name] = {"raw_max_abs_diff": raw_delta, "calibrated_max_abs_diff": cal_delta}
        print("  {:<12} {:>18} {:>18}".format(name, raw_delta, "-" if cal_delta is None else "{:.3e}".format(cal_delta)))

    # confirma que o analisador escolhido lê o arquivo com os mesmos totais
    analysis = analyser(rbd_path, rbd_schema, {
        "demodulate": False, "window_size": 128, "steps": 32, "target_frequency": 20.0,
        "sampling_frequency": 1000.0, "bin_mode": args.fft_bin_mode, "record_limit": None})
    print()
    print("  analisador {}: total={} registros, {} anteriores à hora nominal".format(
        backend_name, analysis["total_records"], analysis["integrity"]["records_before_nominal_hour"]))

    report["rbd"] = {
        "analyser_total_records": analysis["total_records"],
        "reference_records": int(ref_raw.shape[0]),
        "project_records": len(mine),
        "records_dropped_by_reference": len(mine) - int(ref_raw.shape[0]),
        "husec_not_found": missing,
        "fields": rbd_diffs,
    }

    # ------------------------------------------------------------- Deconv ----
    # A demodulação é comparada sobre exatamente as amostras que a referência
    # manteve, então usa-se a função de janela deslizante diretamente. Os dois
    # backends foram verificados equivalentes a 5e-16; ver README.
    signal = array("d", [float(x) for x in ref_cal["golay"]])
    husec = array("Q", [int(x) for x in ref_raw["husec"]])
    my_husec, my_amplitude = demodulation.demodulate(signal, husec, bin_mode=args.fft_bin_mode)
    ref_husec = reference.rbd.Deconv["husec"]
    ref_amplitude = reference.rbd.Deconv["amplitude"]

    print()
    print("=" * 72)
    print("DECONV   (mesma entrada: as {} amostras que a referência manteve)".format(len(signal)))
    print("=" * 72)
    same_timestamps = [int(x) for x in my_husec] == [int(x) for x in ref_husec]
    print("  janelas: referência = {}   projeto = {}".format(len(ref_amplitude), len(my_amplitude)))
    print("  husec idênticos: {}".format(same_timestamps))

    deconv = {"reference_windows": int(len(ref_amplitude)), "project_windows": len(my_amplitude),
              "husec_identical": same_timestamps}
    if len(my_amplitude) == len(ref_amplitude) and len(ref_amplitude):
        errors = [abs(mine_value - ref_value) / abs(ref_value)
                  for mine_value, ref_value in zip(my_amplitude, ref_amplitude) if ref_value]
        deconv["relative_error_max"] = max(errors)
        deconv["relative_error_mean"] = sum(errors) / len(errors)
        print("  erro relativo: máx = {:.3e}   médio = {:.3e}".format(deconv["relative_error_max"],
                                                                     deconv["relative_error_mean"]))
        print("  (~3e-05 é o off-by-one do Goertzel no windowed_dft.c; ver Docs/upstream/README.md)")
    else:
        print("  contagens diferentes, erro relativo não calculado")
    report["deconv"] = deconv

    # ---------------------------------------------------------------- AUX ----
    aux_index = {f["name"]: i for i, f in enumerate(aux_schema["fields"])}
    my_aux = list(records.iter_records(aux_path, aux_schema))
    ref_aux = reference.aux.Data

    print()
    print("=" * 72)
    print("AUX   referência = {} registros    projeto = {} registros".format(ref_aux.shape[0], len(my_aux)))
    print("=" * 72)

    aux_diffs = {}
    count = min(len(my_aux), int(ref_aux.shape[0]))
    for field in aux_schema["fields"]:
        name = field["name"]
        if name not in ref_aux.dtype.names:
            continue
        delta = max(abs(float(my_aux[i][aux_index[name]]) - float(ref_aux[name][i])) for i in range(count))
        aux_diffs[name] = delta
        print("  {:<18} dif máx = {:.3e}".format(name, delta))

    stale = sum(1 for values in my_aux if float(values[aux_index["jd"]]) == 0.0)
    print()
    print("  registros com jd == 0: {} de {} ({:.1f}%)".format(stale, len(my_aux), 100.0 * stale / len(my_aux)))
    print("  a referência não os filtra; este projeto marca record_valid = False")
    if "right_ascension" in aux_index:
        raw_ra = float(my_aux[0][aux_index["right_ascension"]])
        print("  right_ascension[0]: {:.6f} (bruto, horas) -> {:.6f} graus".format(raw_ra, raw_ra * 15.0))
        print("  o HATS.py entrega o valor bruto e o documenta como 'degrees'")

    report["aux"] = {
        "reference_records": int(ref_aux.shape[0]),
        "project_records": len(my_aux),
        "max_abs_diff": aux_diffs,
        "records_with_jd_zero": stale,
    }

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print()
        print("JSON gravado em {}".format(args.json_out))

    return report


if __name__ == "__main__":
    compare(build_argument_parser().parse_args())
