"""
Leitura e demodulação do arquivo .rbd — o sinal do detector, a 1 kHz.

Uma passada sobre o arquivo produz a série demodulada. Nada além disso é
calculado: estatística, verificação de integridade e qualquer outro diagnóstico
ficariam fora da saída da referência, e o objetivo aqui é reproduzi-la.

Duas implementações com o mesmo resultado — `analyse_stdlib`, só com a biblioteca
padrão, e `analyse_numpy`, escolhida quando numpy está disponível. Verificadas
equivalentes bit a bit, o que é exigido por teste.

Fidelidade
----------
Os registros anteriores à hora nominal são descartados, como o HATS.py faz. Isso
importa mais do que parece: o descarte desloca o início da janela deslizante, e
como 559 = 17x32 + 15 a defasagem não é múltipla do passo. Sem reproduzi-lo, as
grades de janelas dos dois pipelines nunca coincidiriam.
"""

import math

from hats import calibration, constants, demodulation, records, schema as schema_module, timebase


def _read_offset(path, schema):
    """
    Quantos registros pular: os anteriores à hora nominal do arquivo.

    Busca binária, válida porque o husec é monotônico nestes arquivos. Custa uns
    poucos seeks em vez de uma passada.
    """
    hour = timebase.hour_from_filename(path)
    if not (hour and hour[:2].isdigit()):
        return 0
    threshold = int(hour[:2]) * constants.HUSEC_PER_HOUR
    return records.first_index_at_or_after(path, schema, "husec", threshold)


def _plan(path, schema, options):
    """Offset de leitura, total de amostras e número de janelas."""
    offset = _read_offset(path, schema)
    total = max(0, records.total_records(path, schema, options["record_limit"]) - offset)
    windows = demodulation.window_count(total, options["window_size"], options["steps"])
    return offset, total, windows


def analyse_stdlib(path, schema, options):
    """Demodulação usando apenas a biblioteca padrão."""
    offset, _total, windows = _plan(path, schema, options)
    index_of = schema_module.field_index(schema)
    golay = next((f for f in schema["fields"] if f["name"] == "golay"), None)

    if not (options["demodulate"] and golay and "husec" in index_of and windows):
        return {"offset": offset, "deconv": None}

    demodulator = demodulation.SlidingDemodulator(
        options["window_size"], options["steps"], options["target_frequency"],
        options["sampling_frequency"], options["bin_mode"], windows)

    slope = golay.get("slope", 1.0)
    intercept = golay.get("offset", 0.0)
    seen = 0
    for columns, count in records.iter_columns(path, schema, options["record_limit"], offset=offset):
        seen += count
        column = columns[index_of["golay"]]
        if golay.get("origin") == "ad7770":
            column = calibration.decode_column(column)
        demodulator.feed([v * slope + intercept for v in column],
                         columns[index_of["husec"]], seen)

    return {"offset": offset, "deconv": (demodulator.husecs, demodulator.amplitudes)}


def analyse_numpy(path, schema, options):
    """Mesma demodulação, vetorizada."""
    import numpy as np
    from numpy.lib.stride_tricks import sliding_window_view

    offset, _total, windows = _plan(path, schema, options)
    dtype_names = schema_module.numpy_dtype(schema).names
    golay = next((f for f in schema["fields"] if f["name"] == "golay"), None)

    if not (options["demodulate"] and golay and "husec" in dtype_names and windows):
        return {"offset": offset, "deconv": None}

    window_size = options["window_size"]
    steps = options["steps"]
    half = window_size // 2
    window = demodulation.flattop_window(window_size)
    coefficient = 2.0 * math.cos(2.0 * math.pi * demodulation.frequency_bin(
        options["target_frequency"], window_size,
        options["sampling_frequency"], options["bin_mode"]) / window_size)

    amplitude_blocks = []
    husec_blocks = []
    carry_signal = np.empty(0, dtype=np.float64)
    carry_husec = np.empty(0, dtype=np.uint64)
    consumed = 0
    next_start = 0
    emitted = 0
    seen = 0

    for block in records.iter_numpy_blocks(path, schema, options["record_limit"], offset=offset):
        seen += block.size
        column = block["golay"]
        if golay.get("origin") == "ad7770":
            column = calibration.numpy_decode_column(column)
        calibrated = column.astype(np.float64) * golay.get("slope", 1.0) + golay.get("offset", 0.0)
        carry_signal = np.concatenate((carry_signal, calibrated))
        carry_husec = np.concatenate((carry_husec, block["husec"].astype(np.uint64)))

        if carry_signal.size < window_size:
            continue

        last_start = seen - window_size
        if next_start <= last_start:
            available = min((last_start - next_start) // steps + 1, windows - emitted)
            if available > 0:
                local = next_start - consumed
                span = (available - 1) * steps + window_size
                views = sliding_window_view(carry_signal[local:local + span], window_size)[::steps]
                amplitude_blocks.append(demodulation.numpy_amplitudes(
                    views, window, coefficient, window_size) / window_size)
                husec_blocks.append(carry_husec[local + half + steps * np.arange(available)])
                next_start += available * steps
                emitted += available
        if next_start > consumed:
            drop = next_start - consumed
            carry_signal = carry_signal[drop:]
            carry_husec = carry_husec[drop:]
            consumed = next_start

    if not amplitude_blocks:
        return {"offset": offset, "deconv": None}

    return {
        "offset": offset,
        "deconv": ([int(v) for v in np.concatenate(husec_blocks)],
                   np.concatenate(amplitude_blocks).tolist()),
    }
