"""
Análise do arquivo .rbd — o sinal do detector, amostrado a 1 kHz.

Uma passada só sobre o arquivo produz três coisas: estatística por canal,
verificação de integridade da sequência, e a amplitude demodulada em 20 Hz.

Há duas implementações com o mesmo resultado: `analyse_stdlib`, que só usa a
biblioteca padrão, e `analyse_numpy`, escolhida quando numpy está disponível.
Verificadas equivalentes sobre uma hora de dados — contagens e husec idênticos,
amplitudes com erro relativo de 9e-16.
"""

import operator

from hats import calibration, constants, demodulation, records, schema as schema_module, statistics, timebase


class IntegrityTracker(object):
    """
    Conta descontinuidades na sequência de registros.

    `sample` deve andar de 1 em 1 e `husec` de 10 em 10, já que a aquisição é a
    1 kHz. Também conta os registros anteriores à hora nominal do arquivo: o
    HATS.py os descarta, aqui eles são mantidos e apenas contados.
    """

    def __init__(self, hour_start_husec):
        self.hour_start_husec = hour_start_husec
        self.sample_leaps = 0
        self.husec_leaps = 0
        self.before_hour = 0
        self.first_husec = None
        self.last_husec = None
        self._previous_sample = None
        self._previous_husec = None

    def feed_columns(self, sample_column, husec_column):
        """Caminho stdlib: as reduções ficam em map/count, fora do interpretador."""
        if husec_column:
            if self.first_husec is None:
                self.first_husec = husec_column[0]
            self.last_husec = husec_column[-1]
            if self.hour_start_husec is not None:
                self.before_hour += sum(map(self.hour_start_husec.__gt__, husec_column))
            deltas = list(map(operator.sub, husec_column[1:], husec_column))
            self.husec_leaps += len(deltas) - deltas.count(constants.HUSEC_PER_SAMPLE)
            if (self._previous_husec is not None
                    and husec_column[0] - self._previous_husec != constants.HUSEC_PER_SAMPLE):
                self.husec_leaps += 1
            self._previous_husec = husec_column[-1]

        if sample_column:
            deltas = list(map(operator.sub, sample_column[1:], sample_column))
            self.sample_leaps += len(deltas) - deltas.count(1)
            if self._previous_sample is not None and sample_column[0] - self._previous_sample != 1:
                self.sample_leaps += 1
            self._previous_sample = sample_column[-1]

    def feed_numpy(self, sample_column, husec_column):
        """Caminho numpy: as mesmas contagens, vetorizadas."""
        import numpy as np

        if husec_column.size:
            if self.first_husec is None:
                self.first_husec = int(husec_column[0])
            self.last_husec = int(husec_column[-1])
            if self.hour_start_husec is not None:
                self.before_hour += int(np.count_nonzero(husec_column < self.hour_start_husec))
            if husec_column.size > 1:
                deltas = np.diff(husec_column.astype(np.int64))
                self.husec_leaps += int(np.count_nonzero(deltas != constants.HUSEC_PER_SAMPLE))
            if (self._previous_husec is not None
                    and int(husec_column[0]) - self._previous_husec != constants.HUSEC_PER_SAMPLE):
                self.husec_leaps += 1
            self._previous_husec = int(husec_column[-1])

        if sample_column.size:
            if sample_column.size > 1:
                self.sample_leaps += int(np.count_nonzero(np.diff(sample_column.astype(np.int64)) != 1))
            if self._previous_sample is not None and int(sample_column[0]) - self._previous_sample != 1:
                self.sample_leaps += 1
            self._previous_sample = int(sample_column[-1])

    def report(self):
        return {
            "sample_number_leaps": self.sample_leaps,
            "husec_leaps": self.husec_leaps,
            "records_before_nominal_hour": self.before_hour,
            "note": ("O HATS.py descarta registros com husec < hora*36000000; "
                     "aqui eles são mantidos e apenas contados."),
        }


def _hour_start_husec(path):
    hour = timebase.hour_from_filename(path)
    if hour and hour[:2].isdigit():
        return int(hour[:2]) * constants.HUSEC_PER_HOUR
    return None


def _demodulation_report(options, husecs, amplitudes, golay_field, date_str, method):
    window_size = options["window_size"]
    used_bin = demodulation.frequency_bin(options["target_frequency"], window_size,
                                          options["sampling_frequency"], options["bin_mode"])
    return {
        "method": method,
        "target_frequency_hz": options["target_frequency"],
        "sampling_frequency_hz": options["sampling_frequency"],
        "window_size": window_size,
        "steps": options["steps"],
        "output_rate_hz": options["sampling_frequency"] / options["steps"],
        "bin_mode": options["bin_mode"],
        "bin_exact": options["target_frequency"] * window_size / options["sampling_frequency"],
        "bin_used": used_bin,
        "bin_frequency_hz": used_bin * options["sampling_frequency"] / window_size,
        "windows": len(amplitudes),
        "unit": golay_field.get("converted_unit") if golay_field else None,
        "amplitude": statistics.summarize(amplitudes),
        "first_time_utc": timebase.datetime_from_husec(date_str, husecs[0]),
        "last_time_utc": timebase.datetime_from_husec(date_str, husecs[-1]),
    }


def _assemble(schema, accumulators, tracker, total, date_str):
    fields = schema_module.converted_fields(schema)
    result = {
        "total_records": total,
        "first_time_utc": (timebase.datetime_from_husec(date_str, tracker.first_husec)
                           if tracker.first_husec is not None else None),
        "last_time_utc": (timebase.datetime_from_husec(date_str, tracker.last_husec)
                          if tracker.last_husec is not None else None),
        "integrity": tracker.report(),
        "statistics_adcu": {},
        "statistics_calibrated": {},
        "units_calibrated": {f["name"]: f.get("converted_unit") for f in fields},
    }
    for field in fields:
        name = field["name"]
        result["statistics_adcu"][name] = accumulators[name].summary()
        if field.get("convert") == "yes":
            result["statistics_calibrated"][name] = calibration.scale_summary(
                result["statistics_adcu"][name], field.get("slope", 1.0), field.get("offset", 0.0))
    return result


def analyse_stdlib(path, schema, options):
    """Análise completa usando apenas a biblioteca padrão."""
    date_str = timebase.date_from_filename(path)
    index_of = schema_module.field_index(schema)
    fields = schema_module.converted_fields(schema)
    accumulators = {f["name"]: statistics.Accumulator() for f in fields}
    tracker = IntegrityTracker(_hour_start_husec(path))

    golay_field = next((f for f in schema["fields"] if f["name"] == "golay"), None)
    count = records.total_records(path, schema, options["record_limit"])
    demodulator = None
    if options["demodulate"] and golay_field is not None and "husec" in index_of:
        windows = demodulation.window_count(count, options["window_size"], options["steps"])
        if windows:
            demodulator = demodulation.SlidingDemodulator(
                options["window_size"], options["steps"], options["target_frequency"],
                options["sampling_frequency"], options["bin_mode"], windows)

    total = 0
    for columns, chunk_count in records.iter_columns(path, schema, options["record_limit"]):
        total += chunk_count
        tracker.feed_columns(columns[index_of["sample"]] if "sample" in index_of else (),
                             columns[index_of["husec"]] if "husec" in index_of else ())

        for field in fields:
            column = columns[index_of[field["name"]]]
            if field.get("origin") == "ad7770":
                column = calibration.decode_column(column)
            accumulators[field["name"]].add_column(column)

            if demodulator is not None and field is golay_field:
                slope = field.get("slope", 1.0)
                offset = field.get("offset", 0.0)
                demodulator.feed([v * slope + offset for v in column],
                                 columns[index_of["husec"]], total)

    result = _assemble(schema, accumulators, tracker, total, date_str)
    if demodulator is not None and demodulator.amplitudes:
        result["demodulation"] = _demodulation_report(
            options, demodulator.husecs, demodulator.amplitudes, golay_field, date_str,
            "flattop_single_bin_dft")
        result["_deconv"] = (demodulator.husecs, demodulator.amplitudes, date_str)
    return result


def analyse_numpy(path, schema, options):
    """Mesma análise, vetorizada. Escolhida automaticamente quando numpy existe."""
    import numpy as np
    from numpy.lib.stride_tricks import sliding_window_view

    date_str = timebase.date_from_filename(path)
    fields = schema_module.converted_fields(schema)
    accumulators = {f["name"]: statistics.Accumulator() for f in fields}
    tracker = IntegrityTracker(_hour_start_husec(path))
    dtype_names = schema_module.numpy_dtype(schema).names

    golay_field = next((f for f in schema["fields"] if f["name"] == "golay"), None)
    count = records.total_records(path, schema, options["record_limit"])
    window_size = options["window_size"]
    steps = options["steps"]

    demodulating = options["demodulate"] and golay_field is not None and "husec" in dtype_names
    max_windows = demodulation.window_count(count, window_size, steps) if demodulating else 0
    demodulating = demodulating and max_windows > 0

    if demodulating:
        projection = demodulation.numpy_projection(
            window_size, options["target_frequency"], options["sampling_frequency"], options["bin_mode"])
        half = window_size // 2
        amplitude_blocks = []
        husec_blocks = []
        carry_signal = np.empty(0, dtype=np.float64)
        carry_husec = np.empty(0, dtype=np.uint64)
        consumed = 0
        next_start = 0
        emitted = 0

    total = 0
    for block in records.iter_numpy_blocks(path, schema, options["record_limit"]):
        total += block.size
        tracker.feed_numpy(block["sample"] if "sample" in dtype_names else np.empty(0),
                           block["husec"] if "husec" in dtype_names else np.empty(0))

        for field in fields:
            column = block[field["name"]]
            if field.get("origin") == "ad7770":
                column = calibration.numpy_decode_column(column)
            as_float = column.astype(np.float64)
            accumulators[field["name"]].add_partial(
                block.size, float(as_float.min()), float(as_float.max()),
                float(as_float.sum()), float(np.dot(as_float, as_float)))

            if demodulating and field is golay_field:
                calibrated = as_float * field.get("slope", 1.0) + field.get("offset", 0.0)
                carry_signal = np.concatenate((carry_signal, calibrated))
                carry_husec = np.concatenate((carry_husec, block["husec"].astype(np.uint64)))

        if demodulating and carry_signal.size >= window_size:
            # Todas as janelas já completas de uma vez: uma multiplicação
            # matriz-vetor em BLAS no lugar de um laço por janela.
            last_start = total - window_size
            if next_start <= last_start:
                available = min((last_start - next_start) // steps + 1, max_windows - emitted)
                if available > 0:
                    local = next_start - consumed
                    span = (available - 1) * steps + window_size
                    views = sliding_window_view(carry_signal[local:local + span], window_size)[::steps]
                    amplitude_blocks.append(np.abs(views @ projection) / window_size)
                    husec_blocks.append(carry_husec[local + half + steps * np.arange(available)])
                    next_start += available * steps
                    emitted += available
            if next_start > consumed:
                drop = next_start - consumed
                carry_signal = carry_signal[drop:]
                carry_husec = carry_husec[drop:]
                consumed = next_start

    result = _assemble(schema, accumulators, tracker, total, date_str)
    if demodulating and amplitude_blocks:
        amplitudes = np.concatenate(amplitude_blocks).tolist()
        husecs = [int(value) for value in np.concatenate(husec_blocks)]
        result["demodulation"] = _demodulation_report(
            options, husecs, amplitudes, golay_field, date_str, "flattop_single_bin_dft_numpy")
        result["_deconv"] = (husecs, amplitudes, date_str)
    return result
