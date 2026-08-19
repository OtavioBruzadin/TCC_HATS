"""
Decodificação do conversor A/D e conversão para unidades físicas.

Os canais analógicos vêm do AD7770, que entrega 24 bits significativos dentro de
uma palavra de 4 bytes, com o sinal no bit 23. Depois disso a conversão para mV
ou °C é afim, com coeficientes vindos do XML de formato.
"""

ADC_MASK = 0x00FFFFFF
ADC_SIGN_BIT = 0x800000


def decode_ad7770(value):
    """
    Estende o sinal de um valor de 24 bits.

    Sem ramo: quando o bit 23 está ligado, (v & 0x800000) << 1 vale exatamente o
    excesso 0x1000000 a subtrair. Os dois termos cabem em int32, então não há
    overflow. A forma sem `if` é cerca de 2,5 vezes mais rápida numa list
    comprehension do que uma função com desvio, e a diferença pesa: são 18
    milhões de chamadas por hora de dados, somando todos os canais.
    """
    return (value & ADC_MASK) - ((value & ADC_SIGN_BIT) << 1)


def decode_column(column):
    """Decodifica uma coluna inteira. Espelha a expressão usada nos laços quentes."""
    return [(v & ADC_MASK) - ((v & ADC_SIGN_BIT) << 1) for v in column]


def numpy_decode_column(column):
    """Versão vetorizada de decode_column, para arrays numpy."""
    return (column & ADC_MASK) - ((column & ADC_SIGN_BIT) << 1)


def apply(field, raw_value):
    """
    Valor cru do arquivo -> valor na unidade física do campo.

    Decodifica se vier do AD7770 e aplica a reta de calibração se o campo estiver
    marcado para converter.
    """
    value = raw_value
    if field.get("origin") == "ad7770":
        value = decode_ad7770(value)
    if field.get("convert") == "yes":
        value = value * field.get("slope", 1.0) + field.get("offset", 0.0)
    return value


def scale_summary(summary, slope, offset):
    """
    Aplica a calibração a uma estatística já calculada.

    A conversão é afim, então min, max, média e desvio se transformam exatamente
    a partir dos valores em contagens de ADC. Isso evita uma multiplicação por
    amostra por canal — 18 milhões delas por hora de dados.
    """
    if not summary["count"]:
        return dict(summary)
    lowest = summary["min"] * slope + offset
    highest = summary["max"] * slope + offset
    if slope < 0:
        lowest, highest = highest, lowest
    return {
        "count": summary["count"],
        "min": lowest,
        "max": highest,
        "mean": summary["mean"] * slope + offset,
        "sd": summary["sd"] * abs(slope),
    }
