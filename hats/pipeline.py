"""
Orquestra o processamento e grava as tabelas.

A saída é exatamente a do `toCSV()` do HATS.py do CRAAM: os mesmos três arquivos,
com os mesmos nomes, colunas, ordem, valores e formatação.

Reproduzir isso exige três coisas que não são óbvias:

  - Os carimbos de tempo usam o `husec2dt()` da referência, cujo cálculo dos
    microssegundos passa por ponto flutuante e trunca. Ver `timebase.craam_datetime`.

  - Os floats saem no repr de round-trip mais curto, que é o que o Python produz
    nativamente: `50.690450199999994`, não `50.69045020`.

  - O arquivo `-rbd_adcu.csv` recebe duas escritas na mesma chamada do `toCSV()`,
    porque o nome está repetido no código da referência: primeiro o sinal bruto
    do detector, depois o apontamento por cima. O conteúdo final é o apontamento,
    e o sinal bruto não sobrevive. O resultado é reproduzido; a escrita
    descartada não, já que o conteúdo final é o mesmo e custaria centenas de MB
    por hora de dados.
"""

from hats import calibration, constants, records, schema as schema_module, timebase


def _cell(value):
    """Formata um valor como o pandas faz ao gravar o CSV da referência."""
    return repr(value) if isinstance(value, float) else str(value)


def _write_table(destination, headers, rows):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as out:
        out.write(",".join(headers) + "\n")
        for row in rows:
            out.write(",".join(_cell(value) for value in row) + "\n")
    return destination


def write_calibrated(destination, rbd_path, schema, offset, limit=None):
    """`-rbd_cal.csv`: os canais convertidos para unidades físicas."""
    fields = [f for f in schema_module.converted_fields(schema) if f.get("convert") == "yes"]
    index_of = schema_module.field_index(schema)

    def rows():
        for values in records.iter_records(rbd_path, schema, limit, offset=offset):
            row = []
            for field in fields:
                value = values[index_of[field["name"]]]
                if field.get("origin") == "ad7770":
                    value = calibration.decode_ad7770(value)
                row.append(value * field.get("slope", 1.0) + field.get("offset", 0.0))
            yield row

    return _write_table(destination, [f["name"] for f in fields], rows())


def write_deconvolved(destination, deconv, date_str):
    """`-deconv.csv`: a amplitude demodulada, com o tempo no centro da janela."""
    husecs = [int(h) for h in deconv[0]]
    amplitudes = deconv[1]
    tempos = timebase.craam_datetime_column(date_str, husecs)
    return _write_table(
        destination, ["time", "husec", "amplitude"],
        ([tempo, husec, float(a)]
         for tempo, husec, a in zip(tempos, husecs, amplitudes)))


def write_pointing(destination, aux_path, schema, limit=None):
    """
    `-rbd_adcu.csv`: o apontamento.

    O nome diz `adcu`, que seriam as contagens do conversor A/D, mas o conteúdo
    é o apontamento — ver a nota no topo do módulo.

    Os registros anteriores à hora nominal são descartados, como o `aux.from_file`
    da referência faz. Isso é fácil de esquecer porque o filtro aparece duas vezes
    no HATS.py, uma para cada tipo de arquivo, e porque há horas em que o aux não
    tem registro nenhum antes da hora — a de 18:00 de 2026-03-17 é uma delas, e
    por isso a omissão passou despercebida até a comparação varrer o dia inteiro.

    Aqui o filtro é por registro, e não por deslocamento inicial: o `np.delete`
    da referência remove todo registro que casa, esteja ele onde estiver.
    """
    index_of = schema_module.field_index(schema)
    date_str = timebase.date_from_filename(aux_path)
    hour = timebase.hour_from_filename(aux_path)
    threshold = (int(hour[:2]) * constants.HUSEC_PER_HOUR
                 if hour and hour[:2].isdigit() else None)

    mantidos = [values for values in records.iter_records(aux_path, schema, limit)
                if threshold is None or values[index_of["husec"]] >= threshold]
    tempos = timebase.craam_datetime_column(
        date_str, [values[index_of["husec"]] for values in mantidos])

    def rows():
        for values, tempo in zip(mantidos, tempos):
            yield [values[index_of[f["name"]]] for f in schema["fields"]] + [tempo]

    headers = [f["name"] for f in schema["fields"]] + ["time"]
    return _write_table(destination, headers, rows())


def process_hour(destination_dir, rootname, rbd_path, rbd_schema,
                 aux_path, aux_schema, options, analyser):
    """Processa uma hora e grava as tabelas.

    Devolve (arquivos escritos, série demodulada, offset de leitura). Os dois
    últimos existem para o diagnóstico não precisar refazer o mesmo trabalho.
    """
    written = []
    deconv = None
    offset = 0

    if rbd_path and rbd_path.exists():
        result = analyser(rbd_path, rbd_schema, options)
        deconv = result.get("deconv")
        offset = result["offset"]
        written.append(write_calibrated(
            destination_dir / "{}-rbd_cal.csv".format(rootname),
            rbd_path, rbd_schema, offset, options["record_limit"]))
        if deconv and len(deconv[1]):
            written.append(write_deconvolved(
                destination_dir / "{}-deconv.csv".format(rootname),
                deconv, timebase.date_from_filename(rbd_path)))

    if aux_path and aux_path.exists():
        written.append(write_pointing(
            destination_dir / "{}-rbd_adcu.csv".format(rootname),
            aux_path, aux_schema, options["record_limit"]))

    return written, deconv, offset
