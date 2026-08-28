"""
Escrita das saídas em CSV e JSON.

Todos os CSV são gravados com UTF-8 explícito, para não depender do locale.

O CSV do sinal bruto é caso à parte: são cerca de 3,6 milhões de linhas e 767 MB
por hora de dados, contra 0,4 s da análise inteira. Por isso ele é opcional, e
quando pedido usa um exportador dedicado.
"""

import csv
import json

from hats import calibration, constants, records, schema as schema_module, timebase
from hats.pointing import corrected_values, state


def write_json(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _rbd_headers(schema):
    headers = []
    for field in schema["fields"]:
        headers.append(field["name"])
        if field.get("origin") == "ad7770" or field.get("convert") == "yes":
            headers.append(field["name"] + "_interpreted")
    headers.extend(["datetime_utc", "datetime_utc_from_husec"])
    return headers


def export_rbd_csv(path, out_csv, schema, limit=None):
    """
    Exportador do sinal bruto usando só a biblioteca padrão.

    Cerca de 53 s por hora de dados. Quando numpy está disponível o pacote usa
    export_rbd_csv_numpy, que produz saída byte-idêntica em ~24 s.
    """
    date_str = timebase.date_from_filename(path)
    index_of = schema_module.field_index(schema)
    format_husec = timebase.fast_husec_formatter(date_str)
    format_unix = timebase.fast_unix_formatter()

    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(_rbd_headers(schema))
        for values in records.iter_records(path, schema, limit):
            row = []
            for field in schema["fields"]:
                raw = values[index_of[field["name"]]]
                row.append(raw)
                if field.get("origin") == "ad7770" or field.get("convert") == "yes":
                    row.append(calibration.apply(field, raw))
            row.append(format_unix(values[index_of["sec"]], values[index_of["ms"]])
                       if "sec" in index_of and "ms" in index_of else None)
            row.append(format_husec(values[index_of["husec"]]) if "husec" in index_of else None)
            writer.writerow(row)


def export_rbd_csv_numpy(path, out_csv, schema, limit=None):
    """
    Exportador dedicado do sinal bruto, acelerado por numpy.

    Produz exatamente as mesmas linhas que export_rbd_csv — verificado byte a
    byte. O que muda é o caminho: decodificação e calibração vetorizadas,
    e writerows() em lote no lugar de um writerow() por registro.

    O ganho veio menos do numpy e mais dos carimbos de tempo: construir um
    datetime por registro custava 22,3 s por hora só na coluna do husec, contra
    3,1 s com aritmética inteira. O que resta — cerca de 6 s de str() nas colunas
    float e 11 s de writerows — é serialização de texto, e não cai sem mudar a
    saída.
    """
    import numpy as np

    date_str = timebase.date_from_filename(path)
    dtype_names = schema_module.numpy_dtype(schema).names
    format_husec = timebase.fast_husec_formatter(date_str)
    format_unix = timebase.fast_unix_formatter()

    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(_rbd_headers(schema))

        for block in records.iter_numpy_blocks(path, schema, limit):
            columns = []
            for field in schema["fields"]:
                raw = block[field["name"]]
                # A primeira coluna é o valor cru, como no exportador stdlib.
                columns.append(raw.tolist())
                if field.get("origin") == "ad7770" or field.get("convert") == "yes":
                    value = calibration.numpy_decode_column(raw) if field.get("origin") == "ad7770" else raw
                    if field.get("convert") == "yes":
                        value = value.astype(np.float64) * field.get("slope", 1.0) + field.get("offset", 0.0)
                    columns.append(value.tolist())

            if "sec" in dtype_names and "ms" in dtype_names:
                columns.append([format_unix(second, millisecond) for second, millisecond
                                in zip(block["sec"].tolist(), block["ms"].tolist())])
            else:
                columns.append([None] * block.size)

            if "husec" in dtype_names:
                columns.append([format_husec(husec) for husec in block["husec"].tolist()])
            else:
                columns.append([None] * block.size)

            writer.writerows(zip(*columns))


def export_aux_csv(path, out_csv, schema, limit=None):
    """Apontamento, com as colunas corrigidas e a marcação de registro defasado."""
    date_str = timebase.date_from_filename(path)
    index_of = schema_module.field_index(schema)
    format_husec = timebase.fast_husec_formatter(date_str)

    headers = [field["name"] for field in schema["fields"]]
    headers.extend(fix[3] for fix in schema_module.AUX_UNIT_FIXES.values())
    headers.extend(["opmode_name", "datetime_utc", "record_valid", "pointing_valid", "pointing_zeroed"])

    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(headers)
        for values in records.iter_records(path, schema, limit):
            record = records.Record({name: values[position] for name, position in index_of.items()})
            row = [values[index_of[field["name"]]] for field in schema["fields"]]
            corrected = corrected_values(record)
            row.extend(corrected.get(fix[3]) for fix in schema_module.AUX_UNIT_FIXES.values())
            opmode = getattr(record, "opmode", None)
            row.append(constants.OPMODE_NAMES.get(opmode, "unknown") if opmode is not None else None)
            row.append(format_husec(getattr(record, "husec")) if "husec" in index_of else None)
            situation = state(record)
            row.extend([situation["record_valid"], situation["pointing_valid"], situation["pointing_zeroed"]])
            writer.writerow(row)


def export_deconv_csv(deconv, out_csv, unit):
    """A amplitude demodulada — a série que interessa para análise científica."""
    husecs, amplitudes, date_str = deconv
    format_husec = timebase.fast_husec_formatter(date_str)
    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["husec", "datetime_utc", "amplitude_{}".format(unit or "raw")])
        for husec, amplitude in zip(husecs, amplitudes):
            writer.writerow([husec, format_husec(husec), amplitude])


def export_ws_csv(path, out_csv):
    from hats import weather

    rows, _rejected, _repaired = weather.read(path)
    with out_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(["time", "station_code", "temperature_c", "humidity", "pressure_hpa"])
        for row in rows:
            writer.writerow([row["time"], row["station_code"], row["temperature_c"],
                             row["humidity"], row["pressure_hpa"]])


# ---------------------------------------------------------------------------
# Reprodução dos CSV do HATS.py
# ---------------------------------------------------------------------------
#
# O toCSV() da referência grava três arquivos com pandas. Reproduzi-los exige
# cuidado com detalhes que não são óbvios:
#
#   - os nomes das colunas e a ordem vêm dos dtypes das estruturas dele, não do
#     XML: o deconv sai como time,husec,amplitude;
#
#   - os carimbos de tempo usam o husec2dt() dele, cujo cálculo de microssegundos
#     passa por ponto flutuante e trunca — ver timebase.craam_datetime();
#
#   - os floats saem com o repr de round-trip mais curto, que é o que o Python
#     produz nativamente;
#
#   - e o arquivo '-rbd_adcu.csv' recebe DUAS escritas na mesma chamada: primeiro
#     os dados brutos do detector, depois o apontamento por cima, porque o nome
#     está repetido no código. O conteúdo final é o apontamento, e os dados
#     brutos não sobrevivem. O comportamento é reproduzido; escrever e descartar
#     não, já que o resultado é o mesmo e custaria centenas de MB por hora.


def _craam_cell(value):
    """Formata um valor como o pandas faria ao gravar o CSV."""
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _write_craam_table(destination, headers, rows):
    with destination.open("w", newline="", encoding="utf-8") as out:
        out.write(",".join(headers) + "\n")
        for row in rows:
            out.write(",".join(_craam_cell(value) for value in row) + "\n")


def export_craam_csv(destination_dir, rootname, rbd_path, rbd_schema,
                     aux_path, aux_schema, deconv, limit=None):
    """
    Grava os mesmos três CSV que o toCSV() do HATS.py grava.

    Devolve a lista de arquivos escritos.
    """
    destination_dir.mkdir(parents=True, exist_ok=True)
    written = []

    date_str = timebase.date_from_filename(rbd_path)
    index_of = schema_module.field_index(rbd_schema)
    converted = schema_module.converted_fields(rbd_schema)
    calibrated_fields = [f for f in converted if f.get("convert") == "yes"]

    hour = timebase.hour_from_filename(rbd_path)
    threshold = int(hour[:2]) * constants.HUSEC_PER_HOUR if hour and hour[:2].isdigit() else None
    offset = records.first_index_at_or_after(rbd_path, rbd_schema, "husec", threshold) if threshold else 0

    # -rbd_cal.csv: os canais calibrados, na ordem em que a referência os monta
    raw_rows = []
    calibrated_rows = []
    for values in records.iter_records(rbd_path, rbd_schema, limit, offset=offset):
        decoded = {}
        for field in converted:
            value = values[index_of[field["name"]]]
            decoded[field["name"]] = (calibration.decode_ad7770(value)
                                      if field.get("origin") == "ad7770" else value)
        raw_rows.append([decoded.get(f["name"], values[index_of[f["name"]]])
                         for f in rbd_schema["fields"]])
        calibrated_rows.append([decoded[f["name"]] * f.get("slope", 1.0) + f.get("offset", 0.0)
                                for f in calibrated_fields])

    if calibrated_rows:
        target = destination_dir / "{}-rbd_cal.csv".format(rootname)
        _write_craam_table(target, [f["name"] for f in calibrated_fields], calibrated_rows)
        written.append(target)

    # -deconv.csv: time, husec, amplitude
    if deconv:
        husecs, amplitudes, _date = deconv
        if len(amplitudes):
            target = destination_dir / "{}-deconv.csv".format(rootname)
            _write_craam_table(
                target, ["time", "husec", "amplitude"],
                ([timebase.craam_datetime(date_str, int(h)), int(h), float(a)]
                 for h, a in zip(husecs, amplitudes)))
            written.append(target)

    # -rbd_adcu.csv: o apontamento sobrescreve os dados brutos, como na referência
    aux_rows = []
    if aux_path and aux_path.exists():
        aux_index = schema_module.field_index(aux_schema)
        aux_date = timebase.date_from_filename(aux_path)
        for values in records.iter_records(aux_path, aux_schema, limit):
            row = [values[aux_index[f["name"]]] for f in aux_schema["fields"]]
            row.append(timebase.craam_datetime(aux_date, values[aux_index["husec"]]))
            aux_rows.append(row)

    target = destination_dir / "{}-rbd_adcu.csv".format(rootname)
    if aux_rows:
        headers = [f["name"] for f in aux_schema["fields"]] + ["time"]
        _write_craam_table(target, headers, aux_rows)
        written.append(target)
    elif raw_rows:
        _write_craam_table(target, [f["name"] for f in rbd_schema["fields"]], raw_rows)
        written.append(target)

    return written
