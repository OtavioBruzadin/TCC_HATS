"""
Testes do produto demodulado — o sinal científico do HATS.

Equivale à estrutura `Deconv` que o `getFFT()` do HATS.py cria, com as mesmas três
colunas: time, husec e amplitude.

O teste que carrega o peso é `test_a_amplitude_e_identica_a_do_pipeline_verificado`.
O pacote `hats` reproduz o HATS_fft.c bit a bit, confrontado com o binário que o
CRAAM distribui; se o plugin publicar um número diferente daquele, a divergência
tem de ser nossa e tem de aparecer aqui.
"""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tests import fixtures

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

import hats_export                                                           # noqa: E402
import hats_ingestion                                                        # noqa: E402
import hats_utils as utils                                                   # noqa: E402
import instrument as plugin                                                  # noqa: E402

from hats import constants, demodulation, rbd, schema as schema_module       # noqa: E402

from tests.test_export_raw import blocos_do_csv                              # noqa: E402
from tests.test_ingestion import um_spec                                     # noqa: E402

XML_DIR = REPO_ROOT / "XMLTables"
DADOS_REAIS = os.environ.get("HATS_TEST_DATA")


class DeconvTestCase(unittest.TestCase):

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.addCleanup(self._temp.cleanup)

        self._saida = tempfile.TemporaryDirectory()
        self._anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._saida.name
        self.addCleanup(self._restaura)

        self.dia = fixtures.build_day(self.root, samples=4000)
        self.ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        self.registros = self.ingestion.load_data(um_spec(), str(self.dia["rbd"])).data_records
        self.export = hats_export.HatsExport()

    def _restaura(self):
        self._saida.cleanup()
        if self._anterior is None:
            os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
        else:
            os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._anterior


class TestAmplitude(DeconvTestCase):

    def test_a_amplitude_e_identica_a_do_pipeline_verificado(self):
        """
        Igualdade exata, não aproximada. O plugin não pode introduzir um número
        próprio: o que ele publica tem de ser o que o `hats.rbd` produz.
        """
        esquema = schema_module.load(XML_DIR, "rbd")
        opcoes = {"record_limit": None, "demodulate": True,
                  "window_size": constants.WINDOW_SIZE, "steps": constants.STEPS,
                  "target_frequency": constants.TARGET_FREQUENCY,
                  "sampling_frequency": constants.SAMPLING_FREQUENCY,
                  "bin_mode": "reference"}
        referencia = rbd.analyse_stdlib(self.dia["rbd"], esquema, opcoes)["deconv"]

        demodulado = hats_export.HatsExport._demodulate(self.registros)

        self.assertEqual([r["HUSEC"] for r in demodulado], [int(h) for h in referencia[0]])
        self.assertEqual([r["AMPLITUDE"] for r in demodulado], list(referencia[1]))

    def test_recupera_a_amplitude_da_senoide_sintetica(self):
        """
        A fixture grava 100 mV a 20 Hz. No modo da referência, com o bin truncado
        para 2, o valor recuperado é 99,018 mV — medido no docstring de
        hats/demodulation.py.
        """
        demodulado = hats_export.HatsExport._demodulate(self.registros)
        media = sum(r["AMPLITUDE"] for r in demodulado) / len(demodulado)
        self.assertAlmostEqual(media, 99.018, delta=0.5)

    def test_a_contagem_de_janelas_segue_a_formula_do_C(self):
        demodulado = hats_export.HatsExport._demodulate(self.registros)
        esperado = demodulation.window_count(
            len(self.registros), constants.WINDOW_SIZE, constants.STEPS)
        self.assertEqual(len(demodulado), esperado)

    def test_o_carimbo_e_o_centro_da_janela(self):
        demodulado = hats_export.HatsExport._demodulate(self.registros)
        meio = constants.WINDOW_SIZE // 2
        for indice, registro in enumerate(demodulado[:5]):
            self.assertEqual(registro["HUSEC"],
                             self.registros[indice * constants.STEPS + meio]["HUSEC"])

    def test_o_carimbo_de_tempo_segue_o_husec(self):
        demodulado = hats_export.HatsExport._demodulate(self.registros)
        meia_noite = datetime(2026, 3, 17, tzinfo=timezone.utc)
        for registro in demodulado[:5]:
            esperado = (registro["UTC_TIME"] - meia_noite).total_seconds() * 1e4
            self.assertAlmostEqual(esperado, registro["HUSEC"], places=3)


class TestProtecoes(DeconvTestCase):

    def test_entrada_curta_demais(self):
        curto = self.registros[:constants.WINDOW_SIZE - 1]
        with self.assertRaises(hats_export.InsufficientRecords) as capturado:
            hats_export.HatsExport._demodulate(curto)
        self.assertIn(str(constants.WINDOW_SIZE), str(capturado.exception))

    def test_registros_fora_de_ordem_falham(self):
        """
        A janela deslizante pressupõe amostras contíguas e crescentes. A ingestão
        garante isso, mas na exportação os registros vêm do banco — e uma consulta
        que os devolvesse fora de ordem daria uma amplitude sem significado, em
        silêncio, porque a conta não falha.
        """
        bagunçado = list(self.registros)
        bagunçado[10], bagunçado[20] = bagunçado[20], bagunçado[10]
        with self.assertRaises(ValueError) as capturado:
            hats_export.HatsExport._demodulate(bagunçado)
        self.assertIn("fora de ordem", str(capturado.exception))


class TestArquivos(DeconvTestCase):

    def test_o_retorno_declara_o_produto(self):
        gerado = self.export.GenerateCSVFileDeconvFormat32ms(self.registros)
        self.assertEqual(gerado.level, "derived")
        self.assertEqual(gerado.scientific_product_key, utils.PRODUTO_DECONV)
        self.assertEqual(gerado.product_resolution, "32ms")

    def test_o_csv_tem_as_tres_colunas_do_Deconv(self):
        gerado = self.export.GenerateCSVFileDeconvFormat32ms(self.registros)
        itens = [l for l in blocos_do_csv(gerado.generated_file_full_path)["#ITEMS"] if l]
        self.assertEqual(itens[0], ["ISO_DATETIME", "HUSEC", "AMPLITUDE"])

    def test_o_csv_nao_traz_as_colunas_do_detector(self):
        gerado = self.export.GenerateCSVFileDeconvFormat32ms(self.registros)
        itens = [l for l in blocos_do_csv(gerado.generated_file_full_path)["#ITEMS"] if l]
        for coluna in ("GOLAY", "CHOPPER", "TEMP_HICS"):
            self.assertNotIn(coluna, itens[0])

    def test_o_cabecalho_declara_32ms(self):
        gerado = self.export.GenerateCSVFileDeconvFormat32ms(self.registros)
        blocos = blocos_do_csv(gerado.generated_file_full_path)
        cabecalho = {l[0]: l[1] for l in blocos["#HEADER"] if len(l) == 2}
        self.assertEqual(cabecalho["FILERES"], "32ms")
        self.assertEqual(cabecalho["LEVEL"], "derived")

    def test_o_adapter_delega(self):
        adapter = plugin.HatsInstrument(xml_dir=str(XML_DIR))
        gerado = adapter.GenerateCSVFileDeconvFormat32ms(self.registros)
        self.assertTrue(Path(gerado.generated_file_full_path).exists())


@unittest.skipUnless(DADOS_REAIS, "defina HATS_TEST_DATA, ou rode make test-real")
class TestDadosReais(unittest.TestCase):
    """O mesmo confronto, sobre o arquivo de verdade."""

    def test_a_amplitude_bate_sobre_uma_hora_real(self):
        caminho = Path(DADOS_REAIS) / "hats-2026-03-17T1800.rbd"
        if not caminho.exists():
            self.skipTest("nao achei {}".format(caminho))

        limite = 200000
        ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR), record_limit=limite)
        registros = ingestion.load_data(um_spec(), str(caminho)).data_records

        esquema = schema_module.load(XML_DIR, "rbd")
        opcoes = {"record_limit": limite, "demodulate": True,
                  "window_size": constants.WINDOW_SIZE, "steps": constants.STEPS,
                  "target_frequency": constants.TARGET_FREQUENCY,
                  "sampling_frequency": constants.SAMPLING_FREQUENCY,
                  "bin_mode": "reference"}
        referencia = rbd.analyse_stdlib(caminho, esquema, opcoes)["deconv"]

        demodulado = hats_export.HatsExport._demodulate(registros)
        self.assertEqual([r["HUSEC"] for r in demodulado], [int(h) for h in referencia[0]])
        self.assertEqual([r["AMPLITUDE"] for r in demodulado], list(referencia[1]))


if __name__ == "__main__":
    unittest.main()
