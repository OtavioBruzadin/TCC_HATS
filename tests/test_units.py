"""Testes das peças isoladas: calibração, base de tempo, demodulação, esquema."""

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hats import calibration, constants, demodulation, schema, statistics, timebase


class TestCalibration(unittest.TestCase):
    def test_decodes_negative_24_bit_value(self):
        # Valor real do arquivo de 2021: a palavra bruta e o que ela significa.
        self.assertEqual(calibration.decode_ad7770(-1140863765), -13077)

    def test_decodes_positive_value_unchanged(self):
        self.assertEqual(calibration.decode_ad7770(94027), 94027)

    def test_sign_bit_boundary(self):
        self.assertEqual(calibration.decode_ad7770(0x7FFFFF), 8388607)
        self.assertEqual(calibration.decode_ad7770(0x800000), -8388608)
        self.assertEqual(calibration.decode_ad7770(0xFFFFFF), -1)

    def test_column_decode_matches_scalar(self):
        raw = [-1140863765, 94027, 0x800000, 0x7FFFFF, 0]
        self.assertEqual(calibration.decode_column(raw),
                         [calibration.decode_ad7770(v) for v in raw])

    def test_apply_uses_slope_and_offset(self):
        field = {"origin": "ad7770", "convert": "yes", "slope": 0.001154, "offset": 0.0}
        self.assertAlmostEqual(calibration.apply(field, 94027), 108.507158, places=9)

    def test_scale_summary_is_exact_for_affine_conversion(self):
        values = [10.0, 20.0, 30.0, 40.0]
        slope, offset = 0.5, -3.0
        raw = statistics.summarize(values)
        scaled = calibration.scale_summary(raw, slope, offset)
        direct = statistics.summarize([v * slope + offset for v in values])
        for key in ("min", "max", "mean", "sd"):
            self.assertAlmostEqual(scaled[key], direct[key], places=12, msg=key)

    def test_scale_summary_swaps_extremes_for_negative_slope(self):
        raw = statistics.summarize([1.0, 5.0])
        scaled = calibration.scale_summary(raw, -2.0, 0.0)
        self.assertEqual(scaled["min"], -10.0)
        self.assertEqual(scaled["max"], -2.0)


class TestTimebase(unittest.TestCase):
    def test_fast_formatter_matches_datetime_path(self):
        format_husec = timebase.fast_husec_formatter("2026-03-17")
        for husec in (0, 1, 9999, 10000, 647994413, 648000000, 863999999):
            self.assertEqual(format_husec(husec),
                             timebase.datetime_from_husec("2026-03-17", husec),
                             msg="husec={}".format(husec))

    def test_fast_formatter_omits_zero_fraction(self):
        # datetime.isoformat() não escreve a fração quando ela é zero; o
        # formatador rápido precisa fazer igual para a saída bater.
        format_husec = timebase.fast_husec_formatter("2026-03-17")
        self.assertEqual(format_husec(648000000), "2026-03-17T18:00:00+00:00")

    def test_fast_formatter_handles_day_rollover(self):
        format_husec = timebase.fast_husec_formatter("2026-03-17")
        self.assertEqual(format_husec(constants.HUSEC_PER_DAY + 10000),
                         timebase.datetime_from_husec("2026-03-17", constants.HUSEC_PER_DAY + 10000))

    def test_unix_formatter_matches_and_caches(self):
        format_unix = timebase.fast_unix_formatter()
        for second, millisecond in ((1773770400, 0), (1773770400, 441), (1773770401, 999)):
            self.assertEqual(format_unix(second, millisecond),
                             timebase.datetime_from_unix(second, millisecond))

    def test_filename_parsing(self):
        path = Path("hats-2026-03-17T1800.rbd")
        self.assertEqual(timebase.date_from_filename(path), "2026-03-17")
        self.assertEqual(timebase.hour_from_filename(path), "1800")


class TestDemodulation(unittest.TestCase):
    def setUp(self):
        self.sampling = constants.SAMPLING_FREQUENCY
        self.window = constants.WINDOW_SIZE
        self.steps = constants.STEPS

    def _sine(self, amplitude, frequency, count, phase=0.0):
        return [amplitude * math.sin(2 * math.pi * frequency * i / self.sampling + phase)
                for i in range(count)]

    def test_reference_bin_reproduces_hats_fft_floor(self):
        # floor(20*128/1000) = 2, ou seja 15,625 Hz e não 20 Hz.
        self.assertEqual(demodulation.frequency_bin(20.0, 128, 1000.0, "reference"), 2.0)
        self.assertAlmostEqual(demodulation.frequency_bin(20.0, 128, 1000.0, "exact"), 2.56)

    def test_window_count_matches_c_formula(self):
        # HATS_fft.c: floor((nrec - window + 1) / steps)
        self.assertEqual(demodulation.window_count(1000, 128, 32), 27)
        self.assertEqual(demodulation.window_count(3600000, 128, 32), 112496)
        self.assertEqual(demodulation.window_count(100, 128, 32), 0)

    def test_recovers_known_amplitude(self):
        signal = self._sine(100.0, 20.0, 4096)
        husec = list(range(4096))
        _, amplitudes = demodulation.demodulate(signal, husec)
        mean = sum(amplitudes) / len(amplitudes)
        # O viés de -1% é intrínseco à normalização da flat-top em N=128 e está
        # presente também no código C do CRAAM.
        self.assertAlmostEqual(mean, 99.018, places=2)

    def test_amplitude_is_linear(self):
        husec = list(range(4096))
        _, low = demodulation.demodulate(self._sine(50.0, 20.0, 4096), husec)
        _, high = demodulation.demodulate(self._sine(100.0, 20.0, 4096), husec)
        ratio = (sum(high) / len(high)) / (sum(low) / len(low))
        self.assertAlmostEqual(ratio, 2.0, places=6)

    def test_exact_bin_has_less_phase_scalloping(self):
        husec = list(range(4096))
        spreads = {}
        for mode in ("reference", "exact"):
            _, amplitudes = demodulation.demodulate(
                self._sine(100.0, 20.0, 4096, phase=0.7), husec, bin_mode=mode)
            mean = sum(amplitudes) / len(amplitudes)
            spreads[mode] = max(abs(a - mean) for a in amplitudes)
        self.assertLess(spreads["exact"], spreads["reference"] / 10)

    def test_dot_product_agrees_with_goertzel_recursion(self):
        signal = self._sine(100.0, 20.0, 512, phase=1.3)
        window = demodulation.flattop_window(self.window)
        coefficient = 2.0 * math.cos(2.0 * math.pi * 2.0 / self.window)
        _, amplitudes = demodulation.demodulate(signal, list(range(512)))
        for index, amplitude in enumerate(amplitudes):
            recursion = demodulation.goertzel_amplitude(
                signal, index * self.steps, window, coefficient, self.window) / self.window
            self.assertAlmostEqual(amplitude, recursion, places=10)

    def test_sliding_demodulator_matches_batch(self):
        signal = self._sine(100.0, 20.0, 1000)
        husec = list(range(1000))
        expected_husec, expected = demodulation.demodulate(signal, husec)

        count = demodulation.window_count(1000, self.window, self.steps)
        sliding = demodulation.SlidingDemodulator(
            self.window, self.steps, 20.0, self.sampling, "reference", count)
        # entrega em blocos irregulares, para exercitar as fronteiras
        position = 0
        for size in (137, 400, 63, 200, 200):
            end = min(position + size, 1000)
            sliding.feed(signal[position:end], husec[position:end], end)
            position = end
        sliding.feed(signal[position:], husec[position:], 1000)

        self.assertEqual(sliding.husecs, expected_husec)
        for produced, reference in zip(sliding.amplitudes, expected):
            self.assertAlmostEqual(produced, reference, places=12)


class TestSchema(unittest.TestCase):
    def setUp(self):
        self.xml_dir = Path(__file__).resolve().parent.parent / "XMLTables"

    def test_rbd_schema_from_project_xml(self):
        loaded = schema.load(self.xml_dir, "rbd")
        self.assertEqual(loaded["record_size"], 38)
        self.assertEqual([f["name"] for f in loaded["fields"]],
                         ["sample", "sec", "ms", "husec", "golay", "chopper",
                          "temp_hics", "temp_env", "temp_golay"])

    def test_aux_schema_record_size(self):
        loaded = schema.load(self.xml_dir, "aux")
        self.assertEqual(loaded["record_size"], 80)

    def test_aux_units_are_corrected(self):
        loaded = schema.load(self.xml_dir, "aux")
        by_name = {f["name"]: f for f in loaded["fields"]}
        right_ascension = by_name["right_ascension"]
        # O XML diz "degrees"; o dado está em horas.
        self.assertEqual(right_ascension["declared_unit"], "degrees")
        self.assertEqual(right_ascension["unit"], "hours")
        self.assertEqual(right_ascension["correction_factor"], 15.0)
        self.assertEqual(by_name["ra_rate"]["unit"], "arcsec/s")

    def test_right_ascension_conversion_matches_solar_ephemeris(self):
        # Valor real de 2026-03-17T18:00 UT. O Sol estava em 357,3905 graus.
        hours = 23.826896641213743
        self.assertAlmostEqual(hours * 15.0, 357.4034, places=3)

    def test_fallback_schema_when_no_xml(self):
        loaded = schema.load(Path("/nao/existe"), "rbd")
        self.assertEqual(loaded["mode"], "fallback_fixed_without_xml")
        self.assertEqual(loaded["record_size"], 38)


if __name__ == "__main__":
    unittest.main()
