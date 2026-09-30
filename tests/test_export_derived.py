"""
Testes dos produtos derivados e do adapter.

Guia §7.1 (DERIVED é agregação), §7.2 e §8 (janela fixa, nova resolução), §12
(instrument.py e build_instrument).

Duas coisas aqui divergem do texto do guia por definição do suporte, e os testes
existem para fixar isso: é MÉDIA e não mediana, e as janelas são de 100 e 1000
amostras e não de 10 e 100.
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
from external_contracts.time_series_export_adapter import TimeSeriesExportAdapter      # noqa: E402
from external_contracts.time_series_ingestion_adapter import TimeSeriesIngestionAdapter  # noqa: E402

from tests.test_export_raw import blocos_do_csv                              # noqa: E402
from tests.test_ingestion import um_spec                                     # noqa: E402

XML_DIR = REPO_ROOT / "XMLTables"

try:
    from astropy.io import fits as astropy_fits
except ImportError:
    astropy_fits = None


class DerivedTestCase(unittest.TestCase):

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.addCleanup(self._temp.cleanup)

        self._saida = tempfile.TemporaryDirectory()
        self._anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._saida.name
        self.addCleanup(self._restaura)

        self.export = hats_export.HatsExport()

    def _restaura(self):
        self._saida.cleanup()
        if self._anterior is None:
            os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
        else:
            os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._anterior

    def registros_reais(self, amostras=4000):
        dia = fixtures.build_day(self.root, samples=amostras)
        ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        return ingestion.load_data(um_spec(), str(dia["rbd"])).data_records

    @staticmethod
    def registros_sinteticos(valores):
        """Registros mínimos, com GOLAY controlado, para conferir a conta à mão."""
        base = datetime(2026, 3, 17, 18, 0, tzinfo=timezone.utc)
        return [{"UTC_TIME": base, "HUSEC": 648000000 + 10 * i, "PROC_SEQ": i,
                 "GOLAY": float(v), "CHOPPER": 0.0, "TEMP_HICS": 0.0,
                 "TEMP_ENV": 0.0, "TEMP_GOLAY": 0.0}
                for i, v in enumerate(valores)]


class TestJanelas(DerivedTestCase):
    """
    O §7.2 diz 10 e 100 registros. Esses números são do POEMAS, que amostra a
    10 ms. O suporte confirmou que o alvo é a resolução, então a 1 kHz são 100 e
    1000 — e o código calcula isso da frequência de amostragem, não escreve à mão.
    """

    def test_as_janelas_saem_da_frequencia_de_amostragem(self):
        self.assertEqual(hats_export.JANELA_100MS, 100)
        self.assertEqual(hats_export.JANELA_1S, 1000)

    def test_100ms_agrupa_100_amostras(self):
        registros = self.registros_reais(amostras=1000)
        derivados = hats_export.HatsExport._derive_raw_to_fixed_windows(
            registros, hats_export.JANELA_100MS)
        self.assertEqual(len(derivados), len(registros) // 100)

    def test_1s_agrupa_1000_amostras(self):
        registros = self.registros_reais(amostras=4000)
        derivados = hats_export.HatsExport._derive_raw_to_fixed_windows(
            registros, hats_export.JANELA_1S)
        self.assertEqual(len(derivados), len(registros) // 1000)

    def test_a_janela_incompleta_e_descartada(self):
        registros = self.registros_sinteticos([1.0] * 250)
        derivados = hats_export.HatsExport._derive_raw_to_fixed_windows(registros, 100)
        self.assertEqual(len(derivados), 2)

    def test_entrada_curta_demais_tem_excecao_propria(self):
        registros = self.registros_sinteticos([1.0] * 50)
        with self.assertRaises(hats_export.InsufficientRecords) as capturado:
            hats_export.HatsExport._derive_raw_to_fixed_windows(registros, 100)
        self.assertIn("100", str(capturado.exception))
        self.assertIn("50", str(capturado.exception))

    def test_continua_sendo_valueerror_para_o_pipeline(self):
        self.assertTrue(issubclass(hats_export.InsufficientRecords, ValueError))


class TestMedia(DerivedTestCase):
    """
    É MÉDIA, não mediana. O §7.2 diz mediana e o POEMAS usa np.median; o suporte
    confirmou por escrito que é média aritmética. Este teste fixa a decisão, e
    falharia se alguém "corrigisse" para mediana ao ler o guia.
    """

    def test_calcula_a_media_aritmetica(self):
        # média de 1,2,3,100 = 26,5   |   mediana seria 2,5
        registros = self.registros_sinteticos([1, 2, 3, 100])
        derivados = hats_export.HatsExport._derive_raw_to_fixed_windows(registros, 4)
        self.assertAlmostEqual(derivados[0]["GOLAY"], 26.5)

    def test_nao_e_mediana(self):
        registros = self.registros_sinteticos([1, 2, 3, 100])
        derivados = hats_export.HatsExport._derive_raw_to_fixed_windows(registros, 4)
        self.assertNotAlmostEqual(derivados[0]["GOLAY"], 2.5)

    def test_todos_os_canais_sao_agregados(self):
        registros = self.registros_reais(amostras=1000)
        derivados = hats_export.HatsExport._derive_raw_to_fixed_windows(registros, 100)
        for canal in hats_export.CANAIS_AGREGADOS:
            with self.subTest(canal=canal):
                esperado = sum(r[canal] for r in registros[:100]) / 100
                self.assertAlmostEqual(derivados[0][canal], esperado, places=9)

    def test_o_carimbo_e_o_centro_da_janela(self):
        registros = self.registros_reais(amostras=1000)
        derivados = hats_export.HatsExport._derive_raw_to_fixed_windows(registros, 100)
        self.assertEqual(derivados[0]["HUSEC"], registros[50]["HUSEC"])
        self.assertEqual(derivados[1]["HUSEC"], registros[150]["HUSEC"])


class TestArquivosDerivados(DerivedTestCase):

    def setUp(self):
        super().setUp()
        self.registros = self.registros_reais(amostras=4000)

    def test_o_retorno_declara_derived_e_a_resolucao(self):
        for metodo, chave, resolucao in (
                (self.export.GenerateCSVFileDerivedFormat100ms, utils.PRODUTO_100MS, "100ms"),
                (self.export.GenerateCSVFileDerivedFormat1s, utils.PRODUTO_1S, "1s")):
            with self.subTest(produto=chave):
                gerado = metodo(self.registros)
                self.assertEqual(gerado.level, "derived")
                self.assertEqual(gerado.scientific_product_key, chave)
                self.assertEqual(gerado.product_resolution, resolucao)

    def test_o_csv_tem_menos_linhas_que_o_bruto(self):
        gerado = self.export.GenerateCSVFileDerivedFormat100ms(self.registros)
        itens = [l for l in blocos_do_csv(gerado.generated_file_full_path)["#ITEMS"] if l]
        self.assertEqual(len(itens) - 1, len(self.registros) // 100)

    def test_o_cabecalho_declara_a_nova_resolucao(self):
        gerado = self.export.GenerateCSVFileDerivedFormat1s(self.registros)
        blocos = blocos_do_csv(gerado.generated_file_full_path)
        cabecalho = {l[0]: l[1] for l in blocos["#HEADER"] if len(l) == 2}
        self.assertEqual(cabecalho["FILERES"], "1s")
        self.assertEqual(cabecalho["LEVEL"], "derived")

    def test_o_arquivo_avisa_que_media_de_portadora_nao_e_fluxo(self):
        gerado = self.export.GenerateCSVFileDerivedFormat100ms(self.registros)
        texto = Path(gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("arithmetic mean", texto)
        self.assertIn("NOT carry the solar signal", texto)
        self.assertIn("centre of its aggregation window", texto)

    def test_a_media_de_um_numero_inteiro_de_ciclos_cancela(self):
        """
        O aviso não é retórico. A fixture grava uma senoide de 20 Hz a 1 kHz, então
        cada janela de 100 amostras cobre exatamente dois ciclos — e a média sai
        zero. É o mesmo efeito que no dado real deixa as médias oscilando em torno
        de zero com desvio de 1,5 mV, contra uma amplitude de 124 mV.
        """
        derivados = hats_export.HatsExport._derive_raw_to_fixed_windows(
            self.registros, hats_export.JANELA_100MS)
        for derivado in derivados[:10]:
            self.assertAlmostEqual(derivado["GOLAY"], 0.0, places=9)

    def test_o_arquivo_diz_quantas_amostras_a_janela_tem(self):
        gerado = self.export.GenerateCSVFileDerivedFormat1s(self.registros)
        texto = Path(gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("1000 consecutive samples", texto)

    @unittest.skipIf(astropy_fits is None, "astropy nao instalado")
    def test_o_fits_derivado_pode_ser_lido_de_volta(self):
        gerado = self.export.GenerateFitsFileDerivedFormat100ms(self.registros)
        with astropy_fits.open(gerado.generated_file_full_path) as hdus:
            self.assertEqual(hdus[0].header["LEVEL"], "derived")
            self.assertEqual(hdus[0].header["FILERES"], "100ms")
            self.assertEqual(len(hdus[1].data), len(self.registros) // 100)


class TestAdapter(DerivedTestCase):
    """Guia §12."""

    def test_build_instrument_devolve_o_adapter(self):
        adapter = plugin.build_instrument()
        self.assertIsInstance(adapter, plugin.HatsInstrument)

    def test_o_adapter_cumpre_os_dois_contratos(self):
        """
        O isinstance de um Protocol runtime_checkable confere a presença de todos os
        métodos. Com os seis do §7.2 presentes, o adapter é um TimeSeriesExportAdapter.
        """
        adapter = plugin.build_instrument()
        self.assertIsInstance(adapter, TimeSeriesIngestionAdapter)
        self.assertIsInstance(adapter, TimeSeriesExportAdapter)

    def test_os_seis_metodos_obrigatorios_existem_e_delegam(self):
        adapter = plugin.HatsInstrument(xml_dir=str(XML_DIR))
        registros = self.registros_reais(amostras=4000)
        for nome in ("GenerateCSVFileRawFormat",
                     "GenerateCSVFileDerivedFormat100ms",
                     "GenerateCSVFileDerivedFormat1s"):
            with self.subTest(metodo=nome):
                gerado = getattr(adapter, nome)(registros)
                self.assertTrue(Path(gerado.generated_file_full_path).exists())

    def test_o_adapter_tambem_ingere(self):
        dia = fixtures.build_day(self.root)
        adapter = plugin.HatsInstrument(xml_dir=str(XML_DIR))
        resultado = adapter.load_data(um_spec(), str(dia["rbd"]))
        self.assertEqual(len(resultado.data_records), dia["samples"])

    def test_o_entrypoint_e_callable_sem_parametros(self):
        """§13.2: o pipeline executa build_instrument() sem argumento."""
        import inspect
        assinatura = inspect.signature(plugin.build_instrument)
        self.assertEqual(len(assinatura.parameters), 0)


if __name__ == "__main__":
    unittest.main()
