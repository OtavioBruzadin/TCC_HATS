"""
Conversões da base de tempo do HATS.

O instrumento carimba tudo em husec — centésimos de milissegundo desde 0 UT, com
864.000.000 num dia. A convenção vem do SST.
"""

from datetime import datetime, timedelta, timezone

from hats import constants

UTC = timezone.utc


def datetime_from_husec(date_str, husec):
    """husec -> ISO 8601, usando a data do nome do arquivo como referência."""
    if not date_str:
        return None
    base = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    return (base + timedelta(seconds=float(husec) / constants.HUSEC_PER_SECOND)).isoformat()


def datetime_from_unix(seconds, milliseconds):
    """Carimbo unix do microcontrolador -> ISO 8601.

    O campo `ms` é o milissegundo dentro do segundo, apesar de o XML e a wiki do
    CRAAM o descreverem como "milliseconds since 0 UT".
    """
    return (datetime.fromtimestamp(seconds, tz=UTC) + timedelta(milliseconds=milliseconds)).isoformat()


def fast_husec_formatter(date_str):
    """
    Formatador de husec sem construir um datetime por registro.

    Numa hora de dados são 3,6 milhões de conversões, e a construção do objeto
    domina a exportação: medido, 22,3 s por hora contra 3,1 s com aritmética
    inteira. A saída é idêntica, incluindo o fato de isoformat() omitir a parte
    fracionária quando ela é zero.
    """

    def format_husec(husec):
        hours = husec // constants.HUSEC_PER_HOUR
        if hours >= 24:
            # Vira o dia: raro, e o caminho lento já trata corretamente.
            return datetime_from_husec(date_str, husec)
        remainder = husec % constants.HUSEC_PER_HOUR
        minutes = remainder // constants.HUSEC_PER_MINUTE
        remainder %= constants.HUSEC_PER_MINUTE
        seconds = remainder // constants.HUSEC_PER_SECOND
        microseconds = (remainder % constants.HUSEC_PER_SECOND) * 100
        if microseconds:
            return "{}T{:02d}:{:02d}:{:02d}.{:06d}+00:00".format(
                date_str, hours, minutes, seconds, microseconds)
        return "{}T{:02d}:{:02d}:{:02d}+00:00".format(date_str, hours, minutes, seconds)

    return format_husec


def fast_unix_formatter():
    """
    Formatador de carimbo unix com cache por segundo.

    Numa hora de dados há 3600 segundos distintos para 3,6 milhões de registros,
    então o datetime é construído uma vez a cada mil linhas.
    """
    cache = {}

    def format_unix(seconds, milliseconds):
        text = cache.get(seconds)
        if text is None:
            text = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
            cache[seconds] = text
        if milliseconds:
            return "{}.{:06d}+00:00".format(text, milliseconds * 1000)
        return "{}+00:00".format(text)

    return format_unix


def date_from_filename(path):
    """'hats-2026-03-17T1800.rbd' -> '2026-03-17'."""
    name = path.name
    if name.startswith("hats-") and len(name) >= 15:
        return name[5:15]
    return None


def hour_from_filename(path):
    """'hats-2026-03-17T1800.rbd' -> '1800'."""
    name = path.name
    if "T" in name:
        return name.split("T", 1)[1][:4]
    return None
