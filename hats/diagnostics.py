"""
Relatórios de diagnóstico, em JSON.

Não fazem parte da saída reproduzida: o `toCSV()` da referência não os produz, e
por isso eles são gravados numa pasta separada, fora do que o `diff` compara. São
uma leitura sobre os dados, e nada aqui altera as tabelas de `Saida/`.

O que eles carregam, e que não existe no original:

  - estatística por canal e verificação de integridade da sequência: saltos em
    `sample` e `husec`, e a contagem de registros anteriores à hora nominal, que
    a referência descarta em silêncio;

  - a análise do apontamento, com as unidades corrigidas e a marcação dos
    registros defasados, incluindo a defasagem estimada;

  - a leitura da estação meteorológica, com a contagem de linhas rejeitadas e de
    carimbos de tempo recuperados.
"""

import json

from hats import calibration, constants, pointing, records, schema as schema_module, statistics, timebase, weather


def write_json(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def rbd_diagnostics(path, schema, options, offset, deconv):
    """
    Percorre o arquivo para estatística e integridade.

    É uma passada a mais sobre o disco, que existe só para o diagnóstico. A
    demodulação já aconteceu antes e não depende disto.
    """
    index_of = schema_module.field_index(schema)
    fields = schema_module.converted_fields(schema)
    accumulators = {f["name"]: statistics.Accumulator() for f in fields}

    hour = timebase.hour_from_filename(path)
    threshold = int(hour[:2]) * constants.HUSEC_PER_HOUR if hour and hour[:2].isdigit() else None

    total = 0
    sample_leaps = 0
    husec_leaps = 0
    previous_sample = None
    previous_husec = None
    import operator
    subtract = operator.sub

    for columns, count in records.iter_columns(path, schema, options["record_limit"], offset=offset):
        total += count
        if "husec" in index_of:
            column = columns[index_of["husec"]]
            deltas = list(map(subtract, column[1:], column))
            husec_leaps += len(deltas) - deltas.count(constants.HUSEC_PER_SAMPLE)
            if previous_husec is not None and column[0] - previous_husec != constants.HUSEC_PER_SAMPLE:
                husec_leaps += 1
            previous_husec = column[-1]
        if "sample" in index_of:
            column = columns[index_of["sample"]]
            deltas = list(map(subtract, column[1:], column))
            sample_leaps += len(deltas) - deltas.count(1)
            if previous_sample is not None and column[0] - previous_sample != 1:
                sample_leaps += 1
            previous_sample = column[-1]
        for field in fields:
            column = columns[index_of[field["name"]]]
            if field.get("origin") == "ad7770":
                column = calibration.decode_column(column)
            accumulators[field["name"]].add_column(column)

    report = {
        "file": path.name,
        "total_records_read": total,
        "records_dropped_before_hour": offset,
        "dropped_note": ("A referência descarta os registros anteriores à hora nominal. "
                         "Eles têm `sample` e `husec` contínuos, ou seja são dados bons; "
                         "o descarte é reproduzido por fidelidade, e contado aqui."),
        "integrity": {"sample_number_leaps": sample_leaps, "husec_leaps": husec_leaps},
        "statistics_adcu": {},
        "statistics_calibrated": {},
        "units_calibrated": {f["name"]: f.get("converted_unit") for f in fields},
    }
    for field in fields:
        name = field["name"]
        report["statistics_adcu"][name] = accumulators[name].summary()
        if field.get("convert") == "yes":
            report["statistics_calibrated"][name] = calibration.scale_summary(
                report["statistics_adcu"][name], field.get("slope", 1.0), field.get("offset", 0.0))

    if deconv:
        husecs, amplitudes = deconv
        report["demodulation"] = {
            "windows": len(amplitudes),
            "output_rate_hz": options["sampling_frequency"] / options["steps"],
            "amplitude": statistics.summarize(amplitudes),
            "unit": next((f.get("converted_unit") for f in fields if f["name"] == "golay"), None),
            "first_husec": int(husecs[0]) if husecs else None,
            "last_husec": int(husecs[-1]) if husecs else None,
        }
    return report


def aux_diagnostics(path, schema, options):
    report = {"file": path.name}
    report.update(pointing.analyse(path, schema, options))
    report["unit_corrections"] = {
        name: {"declared_in_xml": declared, "actual": actual,
               "corrected_field": corrected_name, "factor": factor}
        for name, (declared, actual, factor, corrected_name)
        in schema_module.AUX_UNIT_FIXES.items()
    }
    return report


def ws_diagnostics(path, sample_count=5):
    report = {"file": path.name}
    report.update(weather.analyse(path, sample_count))
    return report
