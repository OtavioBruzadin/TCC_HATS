"""
Testes de ponta a ponta.

Cada teste monta os binários de que precisa num diretório temporário, então a
suíte não depende dos dados reais nem do download do Drive.

A igualdade byte a byte contra o HATS.py real é verificada por `make diff`, que
precisa do ambiente de referência. Aqui fica travado o que dá para travar sem
ele: o layout das tabelas, o truncamento dos carimbos de tempo e a equivalência
entre os dois backends.
"""

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hats import backends, cli, constants, demodulation, diagnostics, pipeline, rbd, records, schema, timebase
from tests import fixtures

PROJECT_ROOT = Path(__file__).resolve().parent.parent
XML_DIR = PROJECT_ROOT / "XMLTables"

OPTIONS = {
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

    def test_binary_search_finds_the_first_record_of_the_hour(self):
        path = self.paths["day_dir"] / "hats-2026-03-17T1800.rbd"
        fixtures.write_rbd(path, samples=1000, start_husec=18 * 36000000 - 5000)
        index = records.first_index_at_or_after(path, self.rbd_schema, "husec", 18 * 36000000)
        self.assertEqual(index, 500)

    def test_record_limit_is_honoured(self):
        self.assertEqual(len(list(records.iter_records(self.paths["rbd"], self.rbd_schema, limit=50))), 50)


class TestDemodulation(TemporaryProject):
    def test_recovers_the_injected_amplitude(self):
        result = rbd.analyse_stdlib(self.paths["rbd"], self.rbd_schema, dict(OPTIONS))
        _husecs, amplitudes = result["deconv"]
        self.assertAlmostEqual(sum(amplitudes) / len(amplitudes), 99.018, places=2)

    def test_drops_records_before_the_nominal_hour_like_the_reference(self):
        path = self.paths["day_dir"] / "hats-2026-03-17T1800.rbd"
        fixtures.write_rbd(path, samples=1000, start_husec=18 * 36000000 - 5000)
        result = rbd.analyse_stdlib(path, self.rbd_schema, dict(OPTIONS))
        self.assertEqual(result["offset"], 500)
        # 500 amostras restantes -> floor((500-128+1)/32) janelas
        self.assertEqual(len(result["deconv"][1]), (500 - 128 + 1) // 32)

    def test_no_demod_option(self):
        result = rbd.analyse_stdlib(self.paths["rbd"], self.rbd_schema, dict(OPTIONS, demodulate=False))
        self.assertIsNone(result["deconv"])

    @unittest.skipUnless(numpy_installed(), "numpy não instalado")
    def test_backends_agree_bit_for_bit(self):
        """A igualdade bit a bit é o produto; um bit divergente já invalida."""
        plain = rbd.analyse_stdlib(self.paths["rbd"], self.rbd_schema, dict(OPTIONS))
        fast = rbd.analyse_numpy(self.paths["rbd"], self.rbd_schema, dict(OPTIONS))
        self.assertEqual(plain["offset"], fast["offset"])
        self.assertEqual(plain["deconv"][0], fast["deconv"][0])
        for index, (one, other) in enumerate(zip(plain["deconv"][1], fast["deconv"][1])):
            self.assertEqual(one, other, msg="janela {}".format(index))


class TestTables(TemporaryProject):
    def _run(self, *extra):
        arguments = ["--project-root", str(self.tmp), "--output-dir", "Saida",
                     "--diagnostics-dir", "Diagnostico"] + list(extra)
        with open(os.devnull, "w") as sink:
            stdout, sys.stdout = sys.stdout, sink
            try:
                self.assertEqual(cli.main(arguments), 0)
            finally:
                sys.stdout = stdout
        return self.tmp / "Saida"

    def test_writes_the_three_reference_tables(self):
        out = self._run()
        self.assertEqual(sorted(path.name for path in out.glob("*.csv")),
                         ["2026-03-17T1800-deconv.csv",
                          "2026-03-17T1800-rbd_adcu.csv",
                          "2026-03-17T1800-rbd_cal.csv"])

    def test_column_layout_matches_the_reference(self):
        out = self._run()
        self.assertTrue((out / "2026-03-17T1800-deconv.csv").read_text(encoding="utf-8")
                        .startswith("time,husec,amplitude\n"))
        self.assertTrue((out / "2026-03-17T1800-rbd_cal.csv").read_text(encoding="utf-8")
                        .startswith("golay,chopper,temp_hics,temp_env,temp_golay\n"))
        # O toCSV() grava o apontamento por cima do CSV do sinal bruto: os dois
        # usam o nome '-rbd_adcu.csv'. O conteúdo final é o apontamento.
        self.assertTrue((out / "2026-03-17T1800-rbd_adcu.csv").read_text(encoding="utf-8")
                        .startswith("husec,jd,sid,elevation,azimuth,"))

    def test_output_folder_holds_only_the_reference_tables(self):
        """
        Nada além das três tabelas pode aparecer em Saida/: é o que torna a
        comparação com a referência um `diff -r` limpo entre dois diretórios.
        """
        out = self._run()
        self.assertEqual(sorted(path.name for path in out.iterdir()),
                         sorted(path.name for path in out.glob("*.csv")))

    def test_diagnostics_go_to_their_own_folder(self):
        self._run()
        folder = self.tmp / "Diagnostico"
        self.assertEqual(sorted(path.name for path in folder.glob("*.json")),
                         ["2026-03-17-ws.json",
                          "2026-03-17T1800-aux.json",
                          "2026-03-17T1800-rbd.json"])

    def test_diagnostics_report_what_the_reference_hides(self):
        """Os descartados, os defasados e as unidades corrigidas."""
        import json
        self._run()
        folder = self.tmp / "Diagnostico"

        detector = json.loads((folder / "2026-03-17T1800-rbd.json").read_text(encoding="utf-8"))
        self.assertIn("records_dropped_before_hour", detector)
        self.assertIn("statistics_calibrated", detector)

        pointing = json.loads((folder / "2026-03-17T1800-aux.json").read_text(encoding="utf-8"))
        self.assertGreater(pointing["stale_records"], 0)
        self.assertIsNotNone(pointing["stale_lag"])
        self.assertEqual(pointing["unit_corrections"]["right_ascension"]["actual"], "hours")

    def test_diagnostics_can_be_skipped(self):
        self._run("--sem-diagnostico")
        self.assertFalse(list((self.tmp / "Diagnostico").glob("*.json")))

    def test_pointing_also_drops_records_before_the_nominal_hour(self):
        """
        O filtro de husec aparece duas vezes no HATS.py, uma para cada tipo de
        arquivo. Ter implementado só o do .rbd passou despercebido durante toda
        uma sessão, porque a hora usada nos testes não tinha registro nenhum de
        apontamento antes da hora. Só a varredura do dia inteiro expôs.
        """
        from hats import records as records_module
        aux = self.paths["aux"]
        antes = fixtures.write_aux(aux, records=100, stale_every=0,
                                   start_husec=18 * 36000000 - 20 * 10500)
        out = self._run()
        linhas = (out / "2026-03-17T1800-rbd_adcu.csv").read_text(encoding="utf-8").splitlines()
        total = records_module.total_records(aux, self.aux_schema)
        # 20 registros começam antes das 18:00 e têm de sair
        self.assertEqual(len(linhas) - 1, total - 20)

    def test_timestamps_truncate_like_the_reference(self):
        self.assertEqual(str(timebase.craam_datetime("2026-03-17", 648000643)),
                         "2026-03-17 18:00:00.064299")
        self.assertEqual(str(timebase.craam_datetime("2026-03-17", 648000000)),
                         "2026-03-17 18:00:00")

    def test_floats_use_the_shortest_round_trip_repr(self):
        self.assertEqual(pipeline._cell(50.690450199999994), "50.690450199999994")
        self.assertEqual(pipeline._cell(4.6044), "4.6044")
        self.assertEqual(pipeline._cell(648000643), "648000643")

    def test_tables_are_utf8(self):
        for path in self._run().glob("*.csv"):
            path.read_text(encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
