"""
Testes da exportação RAW.

Guia §7.1 (RAW é "sem transformação"), §7.2 ("gera arquivos sem alterar os dados",
"mantém resolução original"), §9 (diretório temporário, nome interno), §10 (os sete
campos do retorno) e §11 (o plugin não move, não renomeia, não publica).
"""

import csv
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
from external_contracts.time_series_specialist_generated_file import (        # noqa: E402
    TimeSeriesSpecialistGeneratedFile)

from tests.test_ingestion import um_spec                                     # noqa: E402

XML_DIR = REPO_ROOT / "XMLTables"

try:
    from astropy.io import fits as astropy_fits
except ImportError:
    astropy_fits = None


def blocos_do_csv(caminho):
    """Separa o CSV nos blocos #DOCUMENTATION, #HEADER e #ITEMS."""
    blocos = {}
    atual = None
    with open(caminho, newline="", encoding="utf-8") as arquivo:
        for linha in csv.reader(arquivo, delimiter="|"):
            if linha and linha[0].startswith("#"):
                atual = linha[0]
                blocos[atual] = []
                continue
            if atual:
                blocos[atual].append(linha)
    return blocos


class ExportTestCase(unittest.TestCase):

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.addCleanup(self._temp.cleanup)

        # o diretório temporário da exportação também vai para o descarte
        self._saida = tempfile.TemporaryDirectory()
        self._tmp_anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._saida.name
        self.addCleanup(self._restaura_tmp)

        self.dia = fixtures.build_day(self.root)
        ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        self.registros = ingestion.load_data(um_spec(), str(self.dia["rbd"])).data_records
        self.export = hats_export.HatsExport()

    def _restaura_tmp(self):
        self._saida.cleanup()
        if self._tmp_anterior is None:
            os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
        else:
            os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._tmp_anterior


class TestRetornoDaExportacao(ExportTestCase):
    """Guia §10."""

    def test_os_sete_campos(self):
        gerado = self.export.GenerateCSVFileRawFormat(self.registros)

        self.assertIsInstance(gerado, TimeSeriesSpecialistGeneratedFile)
        self.assertEqual(gerado.instrument_name, "HATS")
        self.assertEqual(gerado.target_date, datetime(2026, 3, 17, tzinfo=timezone.utc))
        self.assertEqual(gerado.level, "raw")
        self.assertEqual(gerado.scientific_product_key, utils.PRODUTO_RAW)
        self.assertEqual(gerado.product_resolution, utils.RESOLUCAO_RAW)
        self.assertEqual(gerado.generated_file_format, "CSV")
        self.assertTrue(Path(gerado.generated_file_full_path).is_absolute())
        self.assertTrue(Path(gerado.generated_file_full_path).exists())

    def test_o_arquivo_fica_em_diretorio_temporario(self):
        """§9 e §11: o plugin escreve em temporário; publicar é do pipeline."""
        gerado = self.export.GenerateCSVFileRawFormat(self.registros)
        self.assertTrue(gerado.generated_file_full_path.startswith(self._saida.name),
                        gerado.generated_file_full_path)

    def test_o_nome_e_unico_a_cada_chamada(self):
        """
        O nome não tem valor semântico: o pipeline renomeia. Só precisa não colidir
        quando dois produtos do mesmo dia caem no mesmo diretório.
        """
        um = self.export.GenerateCSVFileRawFormat(self.registros)
        outro = self.export.GenerateCSVFileRawFormat(self.registros)
        self.assertNotEqual(um.generated_file_full_path, outro.generated_file_full_path)

    def test_records_vazio_falha(self):
        with self.assertRaises(ValueError):
            self.export.GenerateCSVFileRawFormat([])


class TestCSV(ExportTestCase):

    def setUp(self):
        super().setUp()
        self.gerado = self.export.GenerateCSVFileRawFormat(self.registros)
        self.blocos = blocos_do_csv(self.gerado.generated_file_full_path)

    def test_tem_os_tres_blocos(self):
        for bloco in ("#DOCUMENTATION", "#HEADER", "#ITEMS"):
            self.assertIn(bloco, self.blocos)

    def test_as_colunas_sao_as_declaradas(self):
        itens = [linha for linha in self.blocos["#ITEMS"] if linha]
        self.assertEqual(itens[0], [c.nome for c in hats_export.COLUNAS_RAW])

    def test_uma_linha_por_registro(self):
        itens = [linha for linha in self.blocos["#ITEMS"] if linha]
        self.assertEqual(len(itens) - 1, len(self.registros))

    def test_o_cabecalho_declara_UTC_e_a_resolucao(self):
        cabecalho = {l[0]: l[1] for l in self.blocos["#HEADER"] if len(l) == 2}
        self.assertEqual(cabecalho["TZ"], "UTC")
        self.assertEqual(cabecalho["N_RECORD"], str(len(self.registros)))
        self.assertEqual(cabecalho["FILERES"], utils.RESOLUCAO_RAW)
        self.assertEqual(cabecalho["LEVEL"], "raw")
        self.assertEqual(cabecalho["OBSERVAT"], "OAFA")

    def test_o_aviso_do_sinal_picado_viaja_no_arquivo(self):
        """
        Quem baixar o produto bruto precisa saber que GOLAY é portadora. O aviso no
        código não chega a essa pessoa.
        """
        texto = Path(self.gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("CAUTION", texto)
        self.assertIn("124 mV", texto)

    def test_o_carimbo_guarda_os_microssegundos(self):
        itens = [linha for linha in self.blocos["#ITEMS"] if linha]
        primeira = itens[1]
        self.assertEqual(len(primeira[0]), 26, primeira[0])
        self.assertTrue(primeira[0].startswith("2026-03-17T18:00:00."))

    def test_os_valores_nao_sao_alterados(self):
        """§7.2: RAW "gera arquivos sem alterar os dados"."""
        itens = [linha for linha in self.blocos["#ITEMS"] if linha]
        indice = {nome: posicao for posicao, nome in enumerate(itens[0])}
        for posicao, registro in enumerate(self.registros[:200], start=1):
            linha = itens[posicao]
            self.assertEqual(int(linha[indice["HUSEC"]]), registro["HUSEC"])
            self.assertAlmostEqual(float(linha[indice["GOLAY"]]), registro["GOLAY"],
                                   places=6)

    def test_a_escrituracao_da_aquisicao_nao_e_publicada(self):
        itens = [linha for linha in self.blocos["#ITEMS"] if linha]
        for campo in ("PROC_SEQ", "SAMPLE", "SEC", "MS", "FULL_FILE_PATH"):
            self.assertNotIn(campo, itens[0])


class TestResolucaoNaEscrita(unittest.TestCase):
    """
    O suporte autorizou valores numéricos, com uma ressalva: cuidado com
    arredondamento que comprometa a resolução do instrumento, tanto na ingestão
    quanto na exportação.

    Na ingestão não há arredondamento — os valores são float. Na exportação há, e é
    aqui que a ressalva morde: se as casas decimais fossem poucas, duas contagens
    vizinhas do ADC sairiam com o mesmo texto e o passo de quantização viraria
    degrau de arredondamento.
    """

    # Passos de quantização, das retas de calibração do HATSDataFormat.xml.
    PASSOS = {"temperatura": 2.99e-5, "golay": 1.154e-3, "chopper": 2.33e-3}

    def test_contagens_vizinhas_do_ADC_nao_colidem(self):
        for canal, passo in self.PASSOS.items():
            with self.subTest(canal=canal):
                base = 30.0
                self.assertNotEqual(utils.format_float(base),
                                    utils.format_float(base + passo),
                                    "{}: o passo de {:.2e} nao sobrevive a {} casas".format(
                                        canal, passo, utils.DECIMAL_PLACES))

    def test_quatro_casas_destruiriam_o_canal_de_temperatura(self):
        """
        Por que não seguimos as quatro casas do POEMAS: o passo do canal de
        temperatura é 2,99e-5 °C, menor que a granularidade de 1e-4.
        """
        base = 30.0
        passo = self.PASSOS["temperatura"]
        self.assertEqual(utils.format_float(base, places=4),
                         utils.format_float(base + passo, places=4))

    def test_o_husec_sai_inteiro_e_sem_arredondamento(self):
        """
        Orientação do suporte: manter o HUSEC como inteiro, porque a persistência
        guarda o UTC_TIME com precisão de milissegundo e é o husec que preserva os
        0,1 ms do contador.
        """
        coluna = next(c for c in hats_export.COLUNAS_RAW if c.nome == "HUSEC")
        self.assertEqual(coluna.tipo, "int")
        self.assertEqual(coluna.fits, "K")
        registro = {utils.TIME_FIELD: datetime(2026, 3, 17, tzinfo=timezone.utc),
                    "HUSEC": 648000003}
        self.assertEqual(hats_export.HatsExport._celula(coluna, registro), "648000003")


@unittest.skipIf(astropy_fits is None, "astropy nao instalado neste ambiente")
class TestFITS(ExportTestCase):

    def setUp(self):
        super().setUp()
        self.gerado = self.export.GenerateFitsFileRawFormat(self.registros)

    def test_pode_ser_lido_de_volta(self):
        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            cabecalho, tabela = hdus[0].header, hdus[1]

            self.assertEqual(cabecalho["N_RECORD"], len(self.registros))
            self.assertEqual(cabecalho["TZ"], "UTC")
            self.assertEqual(cabecalho["LEVEL"], "raw")
            self.assertEqual(list(tabela.columns.names),
                             [c.nome for c in hats_export.COLUNAS_RAW])
            self.assertEqual(len(tabela.data), len(self.registros))

    def test_os_valores_batem_com_os_do_csv(self):
        """
        Os dois escritores saem da mesma tabela de colunas; se divergirem, é bug de
        duplicação — que é exatamente o que a tabela única existe para impedir.
        """
        csv_gerado = self.export.GenerateCSVFileRawFormat(self.registros)
        blocos = blocos_do_csv(csv_gerado.generated_file_full_path)
        itens = [linha for linha in blocos["#ITEMS"] if linha]
        indice = {nome: posicao for posicao, nome in enumerate(itens[0])}

        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            dados = hdus[1].data
            for posicao in range(200):
                linha = itens[posicao + 1]
                self.assertEqual(dados["ISO_DATETIME"][posicao], linha[indice["ISO_DATETIME"]])
                self.assertEqual(int(dados["HUSEC"][posicao]), int(linha[indice["HUSEC"]]))
                self.assertAlmostEqual(float(dados["GOLAY"][posicao]),
                                       float(linha[indice["GOLAY"]]), places=6)

    def test_a_documentacao_acompanha_o_cabecalho(self):
        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            comentarios = "\n".join(str(c) for c in hdus[0].header["COMMENT"])
        self.assertIn("CAUTION", comentarios)
        self.assertIn("COPYRIGHT", comentarios)


class TestValoresPendentes(unittest.TestCase):
    """
    scientific_product_key e product_resolution sao declaracao do catalogo. Ficam
    num lugar so, e este teste existe para que mover ou renomear quebre aqui.
    """

    def test_estao_isolados_em_hats_utils(self):
        self.assertEqual(utils.PRODUTO_RAW, "raw_1ms")
        self.assertEqual(utils.RESOLUCAO_RAW, "1ms")

    def test_o_codigo_nao_repete_os_valores(self):
        fonte = (PLUGIN_DIR / "hats_export.py").read_text(encoding="utf-8")
        self.assertNotIn('"raw_1ms"', fonte)
        self.assertNotIn('"1ms"', fonte)


if __name__ == "__main__":
    unittest.main()
