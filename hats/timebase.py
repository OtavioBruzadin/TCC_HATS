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


def craam_datetime(date_str, husec):
    """
    Porte fiel do husec2dt() do HATS.py, para reproduzir os CSV dele.

    Não é a mesma coisa que datetime_from_husec(): o cálculo dos microssegundos
    passa por ponto flutuante e trunca. Para husec 648000643 o valor exato seria
    64300 µs, mas 643/1e4 = 0.0643 e 0.0643*1e6 = 64299.999999999993, que int()
    leva a 64299. A referência grava .064299, então é isso que precisa sair aqui.
    """
    from datetime import datetime as _datetime

    year, month, day = int(date_str[0:4]), int(date_str[5:7]), int(date_str[8:10])
    hours = int(husec // constants.HUSEC_PER_HOUR)
    minutes = int((husec % constants.HUSEC_PER_HOUR) // constants.HUSEC_PER_MINUTE)
    seconds = ((husec % constants.HUSEC_PER_HOUR) % constants.HUSEC_PER_MINUTE) / 1.0E+04
    whole_seconds = int(seconds)
    microseconds = int((seconds - whole_seconds) * 1e6)
    return _datetime(year, month, day, hours, minutes, whole_seconds, microseconds)


def craam_datetime_column(date_str, husecs):
    """
    Formata uma coluna de tempo como o pandas faz ao gravar o CSV da referência.

    A decisão do formato é da COLUNA, não de cada valor: se algum registro tem
    microssegundos, todos saem com seis casas, inclusive os que caem em segundo
    exato, que viram `.000000`. Se nenhum tem, nenhum sai com fração.

    O `str()` de um datetime decide valor a valor e omite a fração quando ela é
    zero — daí a divergência que só aparece nas horas em que alguma janela cai
    exatamente sobre um segundo inteiro. Na hora das 18:00 de 2026-03-17 isso
    nunca acontece, porque o passo de 320 husec a partir de 648000643 nunca
    alcança um múltiplo de 10000; por isso a diferença passou despercebida.

    Microssegundo é zero exatamente quando husec é múltiplo de 10000.
    """
    fractional = any(husec % constants.HUSEC_PER_SECOND for husec in husecs)
    layout = "%Y-%m-%d %H:%M:%S.%f" if fractional else "%Y-%m-%d %H:%M:%S"
    return [craam_datetime(date_str, husec).strftime(layout) for husec in husecs]
