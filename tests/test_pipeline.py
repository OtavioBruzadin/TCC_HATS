"""
Testes de ponta a ponta: leitura dos binários, análise, e a CLI inteira.

Cada teste monta seus próprios arquivos num diretório temporário, então a suíte
não depende dos dados reais nem do download do Drive.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hats import backends, cli, pointing, rbd, records, schema, weather
from tests import fixtures

PROJECT_ROOT = Path(__file__).resolve().parent.parent
XML_DIR = PROJECT_ROOT / "XMLTables"

DEFAULT_OPTIONS = {
    "demodulate": True, "window_size": 128, "steps": 32, "target_frequency": 20.0,
    "sampling_frequency": 1000.0, "bin_mode": "reference", "record_limit": None,
}


def numpy_installed():
    return backends.numpy_available()


class TemporaryProject(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="hats-test-"))
        shutil.copytree(XML_DIR, self.tmp / "XMLTables")
        self.paths = fixtures.build_day(self.tmp)
        self.rbd_schema = schema.load(self.tmp / "XMLTables", "rbd")
        self.aux_schema = schema.load(self.tmp / "XMLTables", "aux")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestRecords(TemporaryProject):
    def test_total_records_matches_file_size(self):
        self.assertEqual(records.total_records(self.paths["rbd"], self.rbd_schema),
                         self.paths["samples"])

    def test_rejects_file_whose_size_does_not_divide(self):
        broken = self.tmp / "broken.rbd"
        broken.write_bytes(self.paths["rbd"].read_bytes() + b"\x00\x01\x02")
        with self.assertRaises(ValueError) as caught:
            records.total_records(broken, self.rbd_schema)
        self.assertIn("não é múltiplo", str(caught.exception))

    def test_columns_and_records_agree(self):
        by_record = list(records.iter_records(self.paths["rbd"], self.rbd_schema))
        columns = []
        for block, _count in records.iter_columns(self.paths["rbd"], self.rbd_schema):
            if not columns:
                columns = [list(column) for column in block]
            else:
                for index, column in enumerate(block):
                    columns[index].extend(column)
        self.assertEqual(len(by_record), len(columns[0]))
        self.assertEqual(tuple(columns[index][7] for index in range(len(columns))), by_record[7])

    def test_record_limit_is_honoured(self):
        self.assertEqual(len(list(records.iter_records(self.paths["rbd"], self.rbd_schema, limit=50))), 50)


class TestRbdAnalysis(TemporaryProject):
    def test_recovers_the_injected_amplitude(self):
        result = rbd.analyse_stdlib(self.paths["rbd"], self.rbd_schema, dict(DEFAULT_OPTIONS))
        self.assertEqual(result["total_records"], self.paths["samples"])
        self.assertAlmostEqual(result["demodulation"]["amplitude"]["mean"], 99.018, places=2)

    def test_window_count_follows_the_c_formula(self):
        result = rbd.analyse_stdlib(self.paths["rbd"], self.rbd_schema, dict(DEFAULT_OPTIONS))
        expected = (self.paths["samples"] - 128 + 1) // 32
        self.assertEqual(result["demodulation"]["windows"], expected)

    def test_integrity_sees_a_clean_sequence(self):
        result = rbd.analyse_stdlib(self.paths["rbd"], self.rbd_schema, dict(DEFAULT_OPTIONS))
        self.assertEqual(result["integrity"]["sample_number_leaps"], 0)
        self.assertEqual(result["integrity"]["husec_leaps"], 0)

    def test_counts_records_before_the_nominal_hour(self):
        # arquivo T1800 começando meio segundo antes das 18:00
        path = self.paths["day_dir"] / "hats-2026-03-17T1800.rbd"
        fixtures.write_rbd(path, samples=1000, start_husec=18 * 36000000 - 5000)
        result = rbd.analyse_stdlib(path, self.rbd_schema, dict(DEFAULT_OPTIONS))
        self.assertEqual(result["integrity"]["records_before_nominal_hour"], 500)

    def test_no_demod_option(self):
        options = dict(DEFAULT_OPTIONS, demodulate=False)
        result = rbd.analyse_stdlib(self.paths["rbd"], self.rbd_schema, options)
        self.assertNotIn("demodulation", result)

    @unittest.skipUnless(numpy_installed(), "numpy não instalado")
    def test_numpy_backend_agrees_with_stdlib(self):
        plain = rbd.analyse_stdlib(self.paths["rbd"], self.rbd_schema, dict(DEFAULT_OPTIONS))
        fast = rbd.analyse_numpy(self.paths["rbd"], self.rbd_schema, dict(DEFAULT_OPTIONS))

        self.assertEqual(plain["total_records"], fast["total_records"])
        self.assertEqual(plain["integrity"], fast["integrity"])
        self.assertEqual(plain["demodulation"]["windows"], fast["demodulation"]["windows"])

        plain_husec, plain_amplitude, _ = plain["_deconv"]
        fast_husec, fast_amplitude, _ = fast["_deconv"]
        self.assertEqual([int(v) for v in plain_husec], [int(v) for v in fast_husec])
        for one, other in zip(plain_amplitude, fast_amplitude):
            self.assertAlmostEqual(one, other, places=10)

        for channel, summary in plain["statistics_calibrated"].items():
            for key in ("min", "max", "mean"):
                self.assertAlmostEqual(summary[key], fast["statistics_calibrated"][channel][key],
                                       places=6, msg="{}.{}".format(channel, key))


class TestPointing(TemporaryProject):
    def test_separates_stale_records(self):
        result = pointing.analyse(self.paths["aux"], self.aux_schema, dict(DEFAULT_OPTIONS))
        self.assertEqual(result["total_records"], self.paths["aux_records"])
        self.assertEqual(result["stale_records"], self.paths["aux_stale"])
        self.assertEqual(result["valid_records"],
                         self.paths["aux_records"] - self.paths["aux_stale"])

    def test_scan_flags_live_in_the_stale_set(self):
        # É o padrão dos dados reais, e o motivo de o filtro importar.
        result = pointing.analyse(self.paths["aux"], self.aux_schema, dict(DEFAULT_OPTIONS))
        self.assertEqual(result["opmode_counts_valid"].get("7", 0), 0)
        self.assertGreater(result["opmode_counts_stale"].get("7", 0), 0)

    def test_measures_the_stale_lag(self):
        result = pointing.analyse(self.paths["aux"], self.aux_schema, dict(DEFAULT_OPTIONS))
        lag = result["stale_lag"]
        self.assertIsNotNone(lag)
        # A fixture defasa 1000 posições de 1,05 s cada, ou seja ~1053 s — a
        # mesma ordem da defasagem real medida nos dados de 2026-03.
        self.assertAlmostEqual(lag["mean_seconds"], 1052.8, delta=2.0)
        # e a dispersão tem de ser desprezível, como no dado real
        self.assertLess(lag["sd_seconds"], 0.1)

    def test_record_with_zero_julian_date_is_invalid(self):
        self.assertFalse(pointing.is_valid(records.Record({"jd": 0.0})))
        self.assertTrue(pointing.is_valid(records.Record({"jd": 2461117.25})))

    def test_pointing_valid_requires_julian_date(self):
        # Antes, um registro defasado passava como válido porque azimute e
        # elevação não são zero — são apenas antigos.
        stale = records.Record({"jd": 0.0, "azimuth": 331.4, "elevation": 56.0,
                                "right_ascension": 23.8, "declination": -1.1})
        self.assertFalse(pointing.state(stale)["pointing_valid"])
        self.assertFalse(pointing.state(stale)["pointing_zeroed"])

    def test_unit_correction(self):
        record = records.Record({"right_ascension": 23.826896641213743,
                                 "ra_rate": 0.0375385, "dec_rate": 0.0164809})
        corrected = pointing.corrected_values(record)
        self.assertAlmostEqual(corrected["right_ascension_deg"], 357.4034, places=3)
        self.assertAlmostEqual(corrected["ra_rate_deg_s"], 0.0375385 / 3600.0, places=12)


class TestWeather(TemporaryProject):
    def test_repairs_control_character_and_rejects_garbage(self):
        rows, rejected, repaired = weather.read(self.paths["ws"])
        self.assertEqual(rejected, 1)
        self.assertEqual(repaired, 1)
        for row in rows:
            self.assertFalse(row["time"].startswith("\x7f"))

    def test_parses_pressure_with_either_suffix(self):
        short = weather.parse_line("2026-03-17T00:00:00,0R2,Ta=18.4C,Ua=9.0P,Pa=757.5H")
        long = weather.parse_line("2026-03-17T00:00:00,0R2,Ta=18.4C,Ua=9.0P,Pa=757.5HPa")
        self.assertEqual(short["pressure_hpa"], 757.5)
        self.assertEqual(long["pressure_hpa"], 757.5)

    def test_rejects_line_with_wrong_field_count(self):
        self.assertIsNone(weather.parse_line("2026-03-17T00:00:00,0R2,Ta=18.4C"))


class TestCommandLine(TemporaryProject):
    def _run(self, *extra):
        arguments = ["--project-root", str(self.tmp), "--reports-dir", "Reports"] + list(extra)
        # a CLI é conversadora; o silêncio mantém a saída da suíte legível
        with open(os.devnull, "w") as sink:
            stdout, sys.stdout = sys.stdout, sink
            try:
                self.assertEqual(cli.main(arguments), 0)
            finally:
                sys.stdout = stdout
        return self.tmp / "Reports"

    def test_produces_the_expected_files(self):
        reports = self._run("--export-csv")
        self.assertTrue((reports / "summary.json").exists())
        self.assertTrue((reports / "json" / "2026-03-17__day_report.json").exists())
        self.assertTrue((reports / "csv" / "2026-03-17__1800__deconv.csv").exists())
        self.assertTrue((reports / "csv" / "2026-03-17__1800__aux.csv").exists())
        # o CSV do sinal bruto é opt-in
        self.assertFalse((reports / "csv" / "2026-03-17__1800__rbd.csv").exists())

    def test_raw_csv_is_opt_in(self):
        reports = self._run("--export-csv", "--export-rbd-csv")
        self.assertTrue((reports / "csv" / "2026-03-17__1800__rbd.csv").exists())

    def test_summary_points_at_files_instead_of_copying_them(self):
        reports = self._run()
        summary = json.loads((reports / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary["day_reports"]["2026-03-17"], "2026-03-17__day_report.json")
        self.assertNotIn("reports", summary)

        day = json.loads((reports / "json" / "2026-03-17__day_report.json").read_text(encoding="utf-8"))
        digest = day["hours"]["1800"]["rbd"]
        self.assertEqual(digest["report_file"], "2026-03-17__1800__rbd_report.json")
        self.assertNotIn("sampled_records", digest)

    def test_csv_files_are_utf8(self):
        reports = self._run("--export-csv")
        for path in (reports / "csv").glob("*.csv"):
            path.read_text(encoding="utf-8")   # levanta se não for UTF-8

    @unittest.skipUnless(numpy_installed(), "numpy não instalado")
    def test_backends_produce_the_same_csv(self):
        plain = self._run("--export-csv", "--export-rbd-csv", "--backend", "stdlib")
        kept = self.tmp / "keep"
        shutil.copytree(plain, kept)
        shutil.rmtree(plain)
        fast = self._run("--export-csv", "--export-rbd-csv", "--backend", "numpy")

        for path in sorted((kept / "csv").glob("*.csv")):
            other = fast / "csv" / path.name
            if path.name.endswith("deconv.csv"):
                continue   # difere em ~1e-14 mV, ordem de soma
            self.assertEqual(path.read_bytes(), other.read_bytes(), msg=path.name)


if __name__ == "__main__":
    unittest.main()
