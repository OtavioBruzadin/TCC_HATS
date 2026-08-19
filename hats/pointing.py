"""
Análise do arquivo .aux — o apontamento do telescópio, vindo do getPos/TheSkyX.

Duas coisas exigem cuidado aqui, e nenhuma delas está documentada pelo CRAAM.

Unidades
--------
O `HATSAuxFormat.xml`, o `HATS.py` e a wiki descrevem `right_ascension` em graus
e as taxas em graus por segundo. Os dados estão em horas e em segundos de arco
por segundo. Verificado contra efemérides solares em 2026-03-17, 18 e 19: a
ascensão reta vezes 15 bate com a do Sol em 0,01 grau, e as taxas batem com o
movimento aparente dele em 1%. Os valores originais são preservados; as versões
corrigidas entram como colunas adicionais.

Registros defasados
-------------------
Entre 12% e 23% dos registros trazem `jd == 0` e uma solução de apontamento que é
um retrato coerente de um instante anterior — 1053,5 s nos três dias medidos,
com dispersão de 0,04 s — enquanto o `husec` está correto e contínuo. Não é ruído:
elevação e azimute batem com a posição do Sol em t menos 1053,5 s dentro de
0,03 grau, a mesma precisão dos registros bons.

A defasagem é idêntica nos três dias, o que descarta condição de corrida ou
processo iniciado mais cedo, e sugere um atraso fixo de buffer circular: 1053,5 s
dividido por 1,0506 s por registro dá cerca de 1003 posições. Confirmar isso
exigiria o `getPos.c`, que não está publicado.

Importa porque quase todo registro com `opmode` 7 ou 8, isto é as varreduras,
cai nesse conjunto. Analisar varredura sem filtrar casa marcação de varredura
real com coordenadas de dezessete minutos antes.
"""

from collections import Counter

from hats import constants, records, schema as schema_module, statistics, timebase


def corrected_values(record):
    """Valores convertidos para as unidades que o XML afirma usar."""
    corrected = {}
    for name, (_declared, _actual, factor, corrected_name) in schema_module.AUX_UNIT_FIXES.items():
        value = getattr(record, name, None)
        if value is not None:
            corrected[corrected_name] = float(value) * factor
    return corrected


def is_valid(record):
    """
    Um registro só é utilizável quando a data juliana foi preenchida.

    Com `jd == 0` o apontamento é de um instante anterior, então não pode ser
    pareado com o sinal do detector.
    """
    julian_date = getattr(record, "jd", None)
    return julian_date is not None and float(julian_date) != 0.0


def state(record):
    """Situação de um registro: válido, apontamento zerado, ambos."""
    azimuth = getattr(record, "azimuth", None)
    elevation = getattr(record, "elevation", None)
    right_ascension = getattr(record, "right_ascension", None)
    declination = getattr(record, "declination", None)

    zeroed = True
    if azimuth is not None and elevation is not None and (float(azimuth) or float(elevation)):
        zeroed = False
    if right_ascension is not None and declination is not None and (float(right_ascension) or float(declination)):
        zeroed = False

    valid = is_valid(record)
    return {
        "record_valid": valid,
        "pointing_zeroed": zeroed,
        # Mantido com o nome antigo por compatibilidade, mas agora exige também
        # a data juliana preenchida: antes um registro defasado passava como
        # válido, porque azimute e elevação não são zero, são apenas antigos.
        "pointing_valid": valid and not zeroed,
    }


def estimate_stale_lag(valid_pairs, invalid_pairs):
    """
    Mede a defasagem dos registros inválidos, em segundos.

    Ajusta o tempo sideral contra o husec sobre os registros bons e mede quanto
    cada registro ruim fica atrás dessa reta.
    """
    if not invalid_pairs:
        return None
    fit = statistics.linear_fit(valid_pairs)
    if fit is None:
        return None
    slope, intercept = fit
    lags = [((slope * husec + intercept) - sidereal) * 3600.0 for husec, sidereal in invalid_pairs]
    mean = sum(lags) / len(lags)
    variance = sum((lag - mean) ** 2 for lag in lags) / len(lags)
    return {"mean_seconds": mean, "sd_seconds": variance ** 0.5,
            "min_seconds": min(lags), "max_seconds": max(lags)}


def analyse(path, schema, options):
    """Passada completa sobre o .aux, separando registros bons dos defasados."""
    index_of = schema_module.field_index(schema)
    has = lambda name: name in index_of  # noqa: E731

    total = 0
    valid = 0
    opmode_valid = Counter()
    opmode_stale = Counter()
    object_valid = Counter()
    valid_pairs = []
    invalid_pairs = []
    trackers = {name: statistics.Accumulator()
                for name in ("elevation", "azimuth", "declination") if has(name)}
    right_ascension_degrees = statistics.Accumulator()

    for values in records.iter_records(path, schema, options["record_limit"]):
        total += 1
        record_is_valid = has("jd") and float(values[index_of["jd"]]) != 0.0
        if record_is_valid:
            valid += 1

        if has("husec") and has("sid"):
            pair = (float(values[index_of["husec"]]), float(values[index_of["sid"]]))
            (valid_pairs if record_is_valid else invalid_pairs).append(pair)

        if has("opmode"):
            (opmode_valid if record_is_valid else opmode_stale)[values[index_of["opmode"]]] += 1
        if has("object") and record_is_valid:
            object_valid[values[index_of["object"]]] += 1

        if record_is_valid:
            for name, accumulator in trackers.items():
                accumulator.add_column((values[index_of[name]],))
            if has("right_ascension"):
                factor = schema_module.AUX_UNIT_FIXES["right_ascension"][2]
                right_ascension_degrees.add_column((float(values[index_of["right_ascension"]]) * factor,))

    stale = total - valid
    summaries = {name: accumulator.summary() for name, accumulator in trackers.items()}
    summaries["right_ascension_deg"] = right_ascension_degrees.summary()

    return {
        "total_records": total,
        "valid_records": valid,
        "stale_records": stale,
        "stale_fraction": (float(stale) / total) if total else None,
        "stale_lag": estimate_stale_lag(valid_pairs, invalid_pairs),
        "stale_record_note": (
            "Registros com jd == 0 trazem apontamento de um instante anterior "
            "enquanto o husec está correto. Ficam fora de todas as estatísticas."),
        "opmode_counts_valid": {str(k): v for k, v in sorted(opmode_valid.items())},
        "opmode_counts_stale": {str(k): v for k, v in sorted(opmode_stale.items())},
        "opmode_names": {str(k): v for k, v in constants.OPMODE_NAMES.items()},
        "object_counts_valid": {str(k): v for k, v in sorted(object_valid.items())},
        "statistics_valid": summaries,
    }
