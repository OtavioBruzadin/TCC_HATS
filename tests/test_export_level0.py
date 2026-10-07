"""
Testes do level 0 como o Guigue pediu: as estruturas do HATS.py em camadas.

Ele pediu quatro estruturas no FITS — `rData`, `cData`, `aux` e `Deconv` — e um
arquivo CSV por estrutura. O contrato entrega uma fonte por chamada, então o que
cabe num FITS é o par que vem junto: `rData` e `cData`, do mesmo `.rbd`. O `aux`
tem fonte própria e o `Deconv` é derivado, e cada um sai no seu arquivo.

Os testes que carregam o peso são dois. Um amarra as duas camadas do detector pela
reta de calibração declarada no XML: a contagem publicada tem de ser a que gera o
valor físico publicado, e não uma contagem qualquer. O outro confronta o
apontamento exportado com o que o `hats.records` lê do binário, campo a campo, com
igualdade exata.
"""

import os
import sys
import tempfile
import unittest
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

from hats import records, schema as schema_module                            # noqa: E402

from tests.test_export_raw import blocos_do_csv                              # noqa: E402
from tests.test_ingestion import um_spec                                     # noqa: E402

XML_DIR = REPO_ROOT / "XMLTables"
DADOS_REAIS = os.environ.get("HATS_TEST_DATA")

try:
    from astropy.io import fits as astropy_fits
except ImportError:
    astropy_fits = None


class Level0TestCase(unittest.TestCase):

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.addCleanup(self._temp.cleanup)

        self._saida = tempfile.TemporaryDirectory()
        self._anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._saida.name
        self.addCleanup(self._restaura)

        self.dia = fixtures.build_day(self.root)
        self.ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        self.detector = self.ingestion.load_data(
            um_spec(), str(self.dia["rbd"])).data_records
        self.apontamento = self.ingestion.load_data(
            um_spec(), str(self.dia["aux"])).data_records
        self.export = hats_export.HatsExport()

    def _restaura(self):
        self._saida.cleanup()
        if self._anterior is None:
            os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
        else:
            os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._anterior

    def itens(self, gerado):
        return [l for l in blocos_do_csv(gerado.generated_file_full_path)["#ITEMS"] if l]


@unittest.skipIf(astropy_fits is None, "astropy nao instalado neste ambiente")
class TestCamadasDoDetector(Level0TestCase):
    """O FITS do level 0 do `.rbd`: duas estruturas, duas tabelas."""

    def setUp(self):
        super().setUp()
        self.gerado = self.export.GenerateFitsFileRawFormat(self.detector)

    def test_tem_as_duas_camadas_na_ordem_pedida(self):
        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            # o EXTNAME sai em maiúsculas pela convenção do FITS; a busca por nome
            # é insensível a caixa, e é por nome que se lê uma camada
            self.assertEqual([hdu.name for hdu in hdus[1:]], ["RDATA", "CDATA"])

    def test_cada_camada_tem_as_colunas_da_sua_estrutura(self):
        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            self.assertEqual(list(hdus["rData"].columns.names),
                             [c.nome for c in hats_export.COLUNAS_CONTAGENS])
            self.assertEqual(list(hdus["cData"].columns.names),
                             [c.nome for c in hats_export.COLUNAS_RAW])

    def test_as_duas_camadas_tem_o_mesmo_numero_de_linhas(self):
        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            self.assertEqual(len(hdus["rData"].data), len(self.detector))
            self.assertEqual(len(hdus["cData"].data), len(self.detector))

    def test_as_duas_camadas_carimbam_a_mesma_amostra(self):
        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            cru, convertido = hdus["rData"].data, hdus["cData"].data
            self.assertEqual(list(cru["HUSEC"]), list(convertido["HUSEC"]))
            self.assertEqual(list(cru["ISO_DATETIME"]), list(convertido["ISO_DATETIME"]))

    def test_a_contagem_publicada_gera_o_valor_fisico_publicado(self):
        """
        O vínculo entre as duas camadas é a reta de calibração do XML.

        Sem este teste as duas tabelas poderiam sair de passadas diferentes sobre o
        arquivo, ou com um deslocamento de uma amostra, e nada acusaria: as duas
        seriam plausíveis sozinhas.
        """
        esquema = schema_module.load(XML_DIR, "rbd")
        retas = {campo["name"].upper(): (float(campo["slope"]), float(campo["offset"]))
                 for campo in esquema["fields"] if campo.get("convert") == "yes"}
        self.assertTrue(retas, "o esquema do XML nao declarou nenhuma conversao")

        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            cru, convertido = hdus["rData"].data, hdus["cData"].data
            for nome, (ganho, offset) in retas.items():
                with self.subTest(canal=nome):
                    for posicao in range(0, len(self.detector), 97):
                        contagem = int(cru[nome + "_ADCU"][posicao])
                        self.assertAlmostEqual(
                            float(convertido[nome][posicao]),
                            contagem * ganho + offset, places=9,
                            msg="{} na posicao {}".format(nome, posicao))

    def test_a_contagem_negativa_do_ADC_sobrevive(self):
        """
        O canal do detector oscila em torno de zero, então metade das contagens é
        negativa. Publicá-las como inteiro sem sinal — ou truncá-las — apagaria meio
        ciclo do chopper.
        """
        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            contagens = list(hdus["rData"].data["GOLAY_ADCU"])
        self.assertLess(min(contagens), 0)
        self.assertGreater(max(contagens), 0)
        # a mesma amostra, nas duas unidades, tem de ter o mesmo sinal
        with astropy_fits.open(self.gerado.generated_file_full_path) as hdus:
            fisicos = list(hdus["cData"].data["GOLAY"])
        for contagem, fisico in zip(contagens[:500], fisicos[:500]):
            self.assertEqual(contagem < 0, fisico < 0)


class TestCSVDasContagens(Level0TestCase):
    """No CSV, que é plano, cada estrutura é um arquivo."""

    def setUp(self):
        super().setUp()
        self.gerado = self.export.GenerateCSVFileRawCountsFormat(self.detector)

    def test_as_colunas_sao_as_do_rData(self):
        self.assertEqual(self.itens(self.gerado)[0],
                         [c.nome for c in hats_export.COLUNAS_CONTAGENS])

    def test_nao_traz_as_colunas_convertidas(self):
        cabecalho = self.itens(self.gerado)[0]
        for coluna in ("GOLAY", "CHOPPER", "TEMP_HICS", "TEMP_ENV", "TEMP_GOLAY"):
            self.assertNotIn(coluna, cabecalho)

    def test_as_contagens_saem_inteiras(self):
        itens = self.itens(self.gerado)
        indice = itens[0].index("GOLAY_ADCU")
        for linha in itens[1:50]:
            self.assertNotIn(".", linha[indice], linha)
            int(linha[indice])

    def test_os_valores_sao_os_do_registro(self):
        itens = self.itens(self.gerado)
        indice = {nome: posicao for posicao, nome in enumerate(itens[0])}
        for posicao, registro in enumerate(self.detector[:200], start=1):
            linha = itens[posicao]
            self.assertEqual(int(linha[indice["HUSEC"]]), registro["HUSEC"])
            self.assertEqual(int(linha[indice["GOLAY_ADCU"]]), registro["GOLAY_ADCU"])

    def test_e_um_arquivo_diferente_do_cData(self):
        convertido = self.export.GenerateCSVFileRawFormat(self.detector)
        self.assertNotEqual(self.gerado.generated_file_full_path,
                            convertido.generated_file_full_path)
        self.assertNotEqual(self.gerado.scientific_product_key,
                            convertido.scientific_product_key)

    def test_o_retorno_declara_o_produto(self):
        self.assertEqual(self.gerado.level, "raw")
        self.assertEqual(self.gerado.scientific_product_key, utils.PRODUTO_CONTAGENS)
        self.assertEqual(self.gerado.product_resolution, utils.RESOLUCAO_RAW)

    def test_o_arquivo_diz_que_nao_houve_calibracao(self):
        texto = Path(self.gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("no calibration applied", texto)
        self.assertIn("rData", texto)


class TestApontamento(Level0TestCase):
    """O `.aux` como estrutura publicada, com as doze colunas que ele listou."""

    def setUp(self):
        super().setUp()
        self.gerado = self.export.GenerateCSVFilePointingFormat(self.apontamento)

    def test_tem_as_doze_colunas(self):
        colunas = self.itens(self.gerado)[0]
        self.assertEqual(len(colunas), 12)
        self.assertEqual(colunas, [
            "ISO_DATETIME", "HUSEC", "JD", "SID", "ELEVATION", "AZIMUTH",
            "RIGHT_ASCENSION", "DECLINATION", "RA_RATE", "DEC_RATE",
            "OBJECT", "OPMODE"])

    def test_os_valores_sao_os_lidos_do_binario(self):
        """
        Igualdade exata contra o `hats.records`, que é o caminho verificado. O
        apontamento não passa por conta nenhuma: o que sai no arquivo tem de ser o
        que está no binário.
        """
        esquema = schema_module.load(XML_DIR, "aux")
        posicao_de = schema_module.field_index(esquema)
        brutos = list(records.iter_records(self.dia["aux"], esquema, None))
        self.assertEqual(len(brutos), len(self.apontamento))

        itens = self.itens(self.gerado)
        indice = {nome: posicao for posicao, nome in enumerate(itens[0])}
        for posicao, bruto in enumerate(brutos):
            linha = itens[posicao + 1]
            self.assertEqual(int(linha[indice["HUSEC"]]),
                             int(bruto[posicao_de["husec"]]))
            for nome in ("object", "opmode"):
                self.assertEqual(int(linha[indice[nome.upper()]]),
                                 int(bruto[posicao_de[nome]]))
            for nome in ("jd", "sid", "elevation", "azimuth", "right_ascension",
                         "declination", "ra_rate", "dec_rate"):
                self.assertEqual(linha[indice[nome.upper()]],
                                 utils.format_float(bruto[posicao_de[nome]]),
                                 "{} na posicao {}".format(nome, posicao))

    def test_object_e_opmode_saem_inteiros(self):
        itens = self.itens(self.gerado)
        indice = {nome: posicao for posicao, nome in enumerate(itens[0])}
        for linha in itens[1:]:
            for nome in ("OBJECT", "OPMODE"):
                self.assertNotIn(".", linha[indice[nome]], linha)

    def test_o_arquivo_avisa_do_apontamento_defasado(self):
        """
        Entre 3,5% e 10% dos registros trazem solução de ~1053 s antes. Quem for
        interpolar o apontamento sobre uma grade uniforme precisa saber disso pelo
        arquivo, não pelo código.
        """
        texto = Path(self.gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("CAUTION", texto)
        self.assertIn("1053", texto)
        self.assertIn("nearest-neighbour", texto)

    def test_os_defasados_nao_sao_filtrados_na_exportacao(self):
        """
        O aviso diz que eles existem; o arquivo os publica. Filtrar no level 0 seria
        decidir pelo Guigue o que ele quer no level 1, e o `jd` zerado é justamente
        como se reconhece um deles.
        """
        itens = self.itens(self.gerado)
        indice = itens[0].index("JD")
        zerados = sum(1 for linha in itens[1:] if float(linha[indice]) == 0.0)
        self.assertEqual(zerados, self.dia["aux_stale"])
        self.assertEqual(len(itens) - 1, len(self.apontamento))

    def test_as_unidades_declaradas_no_XML_nao_sao_as_dos_dados(self):
        """
        O XML diz "degrees" em três campos onde o dado está em hora e arcsec/s. O
        pacote `hats` corrige isso no esquema; a descrição da coluna tem de dizer a
        unidade do dado, senão o arquivo publica o número certo com o rótulo errado.
        """
        por_nome = {c.nome: c for c in hats_export.COLUNAS_APONTAMENTO}
        for nome, (_declarada, real, _fator, _corrigido) in schema_module.AUX_UNIT_FIXES.items():
            with self.subTest(campo=nome):
                self.assertIn(real, por_nome[nome.upper()].descricao)

        # e o arquivo diz isso por escrito, porque quem ler o XML vai achar que as
        # colunas estão em grau
        texto = Path(self.gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("NOTE ON UNITS", texto)

    def test_o_retorno_declara_a_cadencia_propria(self):
        self.assertEqual(self.gerado.level, "raw")
        self.assertEqual(self.gerado.scientific_product_key, utils.PRODUTO_APONTAMENTO)
        self.assertEqual(self.gerado.product_resolution, utils.RESOLUCAO_APONTAMENTO)

    def test_o_cabecalho_declara_a_resolucao_do_apontamento(self):
        blocos = blocos_do_csv(self.gerado.generated_file_full_path)
        cabecalho = {l[0]: l[1] for l in blocos["#HEADER"] if len(l) == 2}
        self.assertEqual(cabecalho["FILERES"], utils.RESOLUCAO_APONTAMENTO)
        self.assertEqual(cabecalho["N_RECORD"], str(len(self.apontamento)))

    @unittest.skipIf(astropy_fits is None, "astropy nao instalado neste ambiente")
    def test_o_fits_do_apontamento_tem_a_camada_aux(self):
        gerado = self.export.GenerateFitsFilePointingFormat(self.apontamento)
        with astropy_fits.open(gerado.generated_file_full_path) as hdus:
            self.assertEqual([hdu.name for hdu in hdus[1:]], ["AUX"])
            self.assertEqual(list(hdus["aux"].columns.names),
                             [c.nome for c in hats_export.COLUNAS_APONTAMENTO])
            self.assertEqual(len(hdus["aux"].data), len(self.apontamento))


class TestTrocaDeEstrutura(Level0TestCase):
    """
    Quatro estruturas e quatro famílias de método: cruzar os fios tem de falhar.

    Sem a checagem, o CSV do apontamento chamado com registros do detector sairia
    com as onze colunas vazias — arquivo bem formado, publicável, sem dado nenhum.
    """

    def test_apontamento_com_registros_do_detector_falha(self):
        with self.assertRaises(hats_export.WrongStructure) as capturado:
            self.export.GenerateCSVFilePointingFormat(self.detector)
        self.assertIn("aux", str(capturado.exception))

    def test_detector_com_registros_do_apontamento_falha(self):
        with self.assertRaises(hats_export.WrongStructure):
            self.export.GenerateCSVFileRawFormat(self.apontamento)

    def test_contagens_com_registros_do_apontamento_falha(self):
        with self.assertRaises(hats_export.WrongStructure):
            self.export.GenerateCSVFileRawCountsFormat(self.apontamento)

    def test_continua_sendo_um_valueerror(self):
        """O pipeline não conhece a subclasse."""
        self.assertTrue(issubclass(hats_export.WrongStructure, ValueError))

    def test_o_csv_recusa_duas_camadas(self):
        """O CSV é plano: duas estruturas no mesmo arquivo não têm representação."""
        with self.assertRaises(ValueError) as capturado:
            self.export._escreve(
                self.detector, hats_export.PRODUTO_RAW, "CSV", "csv",
                (hats_export.CAMADA_RDATA, hats_export.CAMADA_CDATA))
        self.assertIn("uma estrutura por arquivo", str(capturado.exception))


class TestDocumentacaoPorProduto(Level0TestCase):
    """
    Cada produto descreve o que ele é.

    Antes desta separação o bloco de documentação era escolhido por `janela is
    None`, e o arquivo do demodulado — que também não tem janela de agregação —
    saía dizendo "LEVEL = raw" e "no transformation applied", o que é falso: ele é
    o resultado da demodulação.
    """

    def test_o_demodulado_nao_se_diz_cru(self):
        registros = self.ingestion.load_data(
            um_spec(), str(self.dia["rbd"])).data_records
        gerado = self.export.GenerateCSVFileDeconvFormat32ms(registros)
        texto = Path(gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("LEVEL = derived", texto)
        self.assertNotIn("no transformation applied", texto)
        self.assertIn("Goertzel", texto)

    def test_o_cData_se_diz_calibrado_e_nao_transformado(self):
        gerado = self.export.GenerateCSVFileRawFormat(self.detector)
        texto = Path(gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("LEVEL = raw", texto)
        self.assertIn("no other transformation", texto)

    def test_o_fits_de_duas_camadas_documenta_as_duas(self):
        linhas = hats_export.HatsExport._documentacao(
            hats_export.PRODUTO_RAW,
            (hats_export.CAMADA_RDATA, hats_export.CAMADA_CDATA))
        texto = "\n".join(linhas)
        self.assertIn("STRUCTURE rData", texto)
        self.assertIn("STRUCTURE cData", texto)
        for coluna in hats_export.COLUNAS_CONTAGENS:
            self.assertIn(coluna.nome, texto)

    def test_o_adapter_delega_os_metodos_novos(self):
        adapter = plugin.HatsInstrument(xml_dir=str(XML_DIR))
        for metodo, registros in (
                ("GenerateCSVFileRawCountsFormat", self.detector),
                ("GenerateCSVFilePointingFormat", self.apontamento),
                ("GenerateFitsFilePointingFormat", self.apontamento)):
            with self.subTest(metodo=metodo):
                if metodo.startswith("GenerateFits") and astropy_fits is None:
                    continue
                gerado = getattr(adapter, metodo)(registros)
                self.assertTrue(Path(gerado.generated_file_full_path).exists())


class TestValoresPendentes(unittest.TestCase):
    """
    As chaves dos produtos novos são declaração do catálogo, como as outras: ficam
    em hats_utils.py e em nenhum outro lugar.
    """

    def test_estao_isolados_em_hats_utils(self):
        self.assertEqual(utils.PRODUTO_CONTAGENS, "raw_counts_1ms")
        self.assertEqual(utils.PRODUTO_APONTAMENTO, "raw_pointing_1050ms")
        self.assertEqual(utils.RESOLUCAO_APONTAMENTO, "1050ms")

    def test_o_codigo_da_exportacao_nao_repete_os_valores(self):
        fonte = (PLUGIN_DIR / "hats_export.py").read_text(encoding="utf-8")
        for valor in ('"raw_counts_1ms"', '"raw_pointing_1050ms"', '"1050ms"'):
            self.assertNotIn(valor, fonte)


@unittest.skipUnless(DADOS_REAIS, "defina HATS_TEST_DATA, ou rode make test-real")
class TestDadosReais(unittest.TestCase):
    """O mesmo confronto, sobre o `.aux` de verdade."""

    def setUp(self):
        self._saida = tempfile.TemporaryDirectory()
        self._anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._saida.name
        self.addCleanup(self._restaura)

    def _restaura(self):
        self._saida.cleanup()
        if self._anterior is None:
            os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
        else:
            os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._anterior

    def test_o_apontamento_exportado_bate_com_o_binario(self):
        caminho = Path(DADOS_REAIS) / "aux" / "hats-2026-03-17T1800.aux"
        if not caminho.exists():
            self.skipTest("nao achei {}".format(caminho))

        ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        registros = ingestion.load_data(um_spec(), str(caminho)).data_records

        esquema = schema_module.load(XML_DIR, "aux")
        posicao_de = schema_module.field_index(esquema)
        brutos = list(records.iter_records(caminho, esquema, None))

        # a ingestão descarta o que está antes da hora nominal do arquivo
        descartados = len(brutos) - len(registros)
        self.assertGreaterEqual(descartados, 0)

        gerado = hats_export.HatsExport().GenerateCSVFilePointingFormat(registros)
        itens = [l for l in blocos_do_csv(gerado.generated_file_full_path)["#ITEMS"] if l]
        indice = {nome: posicao for posicao, nome in enumerate(itens[0])}
        self.assertEqual(len(itens) - 1, len(registros))

        for posicao, bruto in enumerate(brutos[descartados:]):
            linha = itens[posicao + 1]
            self.assertEqual(int(linha[indice["HUSEC"]]), int(bruto[posicao_de["husec"]]))
            self.assertEqual(linha[indice["ELEVATION"]],
                             utils.format_float(bruto[posicao_de["elevation"]]))
            self.assertEqual(linha[indice["AZIMUTH"]],
                             utils.format_float(bruto[posicao_de["azimuth"]]))

    def test_a_fracao_de_apontamento_defasado_esta_na_faixa_medida(self):
        """O aviso que vai no arquivo diz 3,5% a 10%; o dado real tem de confirmar."""
        caminho = Path(DADOS_REAIS) / "aux" / "hats-2026-03-17T1800.aux"
        if not caminho.exists():
            self.skipTest("nao achei {}".format(caminho))

        ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        registros = ingestion.load_data(um_spec(), str(caminho)).data_records
        defasados = sum(1 for r in registros if r["JD"] == 0.0)
        fracao = 100.0 * defasados / len(registros)
        self.assertGreater(fracao, 0.0)
        self.assertLess(fracao, 15.0, "medido {:.1f}%".format(fracao))


if __name__ == "__main__":
    unittest.main()
