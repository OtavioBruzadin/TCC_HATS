"""
Testes do level 1 — o dado integrado como o Guigue descreveu.

Ele pediu, para 0,1 s e 1 s, "um arquivo único" com doze itens: tempo ISO, o sinal
demodulado, as três temperaturas e nove variáveis do apontamento. São catorze
colunas, porque o item 3 da lista dele são as três temperaturas.

E pediu o método: "criar uma sequência temporal uniforme de 0,1 ou 1 segundo e
fazer uma interpolação dos dados sobre essa sequência".

Dois testes carregam o peso. Um confere a amplitude da grade contra a série
demodulada do `hats.rbd` — o caminho confrontado com o binário do CRAAM —
reinterpolada aqui com `bisect`, numa implementação independente da do plugin. O
outro exercita os DOIS caminhos da decisão pendente sobre o apontamento defasado, e
exige que o arquivo declare qual deles rodou.
"""

import bisect
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests import fixtures

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

import hats_export                                                           # noqa: E402
import hats_ingestion                                                        # noqa: E402
import hats_level1 as level1                                                 # noqa: E402
import hats_utils as utils                                                   # noqa: E402
import instrument as plugin                                                  # noqa: E402

from hats import constants, rbd, schema as schema_module                     # noqa: E402

from tests.test_export_raw import blocos_do_csv                              # noqa: E402
from tests.test_ingestion import um_spec                                     # noqa: E402

XML_DIR = REPO_ROOT / "XMLTables"
DADOS_REAIS = os.environ.get("HATS_TEST_DATA")

# As catorze colunas, na ordem em que ele enumerou os doze itens.
COLUNAS_ESPERADAS = [
    "ISO_DATETIME", "AMPLITUDE", "TEMP_HICS", "TEMP_ENV", "TEMP_GOLAY",
    "SID", "ELEVATION", "AZIMUTH", "RIGHT_ASCENSION", "DECLINATION",
    "RA_RATE", "DEC_RATE", "OBJECT", "OPMODE"]


def interpola_com_bisect(nos_x, nos_y, alvo):
    """
    Interpolação linear escrita de outra forma, para confrontar a do plugin.

    Busca binária em vez de varredura com cursor: se as duas concordarem, o
    resultado não depende de como a passada foi organizada.
    """
    posicao = bisect.bisect_right(nos_x, alvo) - 1
    if posicao >= len(nos_x) - 1:
        posicao = len(nos_x) - 2
    esquerda, direita = nos_x[posicao], nos_x[posicao + 1]
    valor_esquerda, valor_direita = nos_y[posicao], nos_y[posicao + 1]
    peso = (alvo - esquerda) / (direita - esquerda)
    return valor_esquerda + (valor_direita - valor_esquerda) * peso


def husec_do_carimbo(texto):
    """Do ISO de volta para husec desde 0 UT, para conferir a grade."""
    hora = int(texto[11:13])
    minuto = int(texto[14:16])
    segundo = int(texto[17:19])
    micro = int(texto[20:26])
    return ((hora * 3600 + minuto * 60 + segundo) * constants.HUSEC_PER_SECOND
            + micro // 100)


class Level1TestCase(unittest.TestCase):

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.addCleanup(self._temp.cleanup)

        self._saida = tempfile.TemporaryDirectory()
        self._anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._saida.name
        self.addCleanup(self._restaura)

        # vinte segundos de detector, para a grade de 1 s ter pontos de sobra
        self.dia = fixtures.build_day(self.root, samples=20000)
        self.ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        self.detector = self.ingestion.load_data(
            um_spec(), str(self.dia["rbd"])).data_records
        self.apontamento = self.ingestion.load_data(
            um_spec(), str(self.dia["aux"])).data_records
        self.entrada = self.detector + self.apontamento
        self.export = hats_export.HatsExport()

    def _restaura(self):
        self._saida.cleanup()
        if self._anterior is None:
            os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
        else:
            os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._anterior

    def itens(self, gerado):
        return [l for l in blocos_do_csv(gerado.generated_file_full_path)["#ITEMS"] if l]

    def tabela(self, gerado):
        """O CSV como (índice das colunas, linhas de dado)."""
        itens = self.itens(gerado)
        return {nome: posicao for posicao, nome in enumerate(itens[0])}, itens[1:]


class TestColunas(Level1TestCase):

    def test_sao_as_catorze_colunas_na_ordem_que_ele_enumerou(self):
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        self.assertEqual(self.itens(gerado)[0], COLUNAS_ESPERADAS)

    def test_a_tabela_de_colunas_do_codigo_e_a_mesma(self):
        self.assertEqual([c.nome for c in hats_export.COLUNAS_NIVEL1],
                         COLUNAS_ESPERADAS)

    def test_nao_publica_o_canal_cru_nem_o_husec(self):
        """
        Ele não pediu nem GOLAY nem husec no level 1: o sinal é a amplitude
        demodulada, e a grade é uniforme e ancorada em 0 UT, então o instante de
        cada ponto se recupera exatamente do carimbo ISO.
        """
        colunas = self.itens(self.export.GenerateCSVFileLevel1Format100ms(self.entrada))[0]
        for ausente in ("GOLAY", "GOLAY_ADCU", "HUSEC", "CHOPPER", "JD"):
            self.assertNotIn(ausente, colunas)

    def test_um_arquivo_unico_e_nao_um_por_estrutura(self):
        """
        "No caso dos dados integrados (0,1 e 1 s) acho que basta um arquivo único
        com essas colunas" — level 0 é um arquivo por estrutura, level 1 não.
        """
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        indice, linhas = self.tabela(gerado)
        self.assertEqual(len(indice), len(COLUNAS_ESPERADAS))
        self.assertTrue(linhas)


class TestGradeDoProduto(Level1TestCase):

    def setUp(self):
        super().setUp()
        self.gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        self.indice, self.linhas = self.tabela(self.gerado)
        self.husecs = [husec_do_carimbo(linha[0]) for linha in self.linhas]

    def test_o_passo_e_constante(self):
        passos = {b - a for a, b in zip(self.husecs, self.husecs[1:])}
        self.assertEqual(passos, {hats_export.PASSO_NIVEL1_100MS})

    def test_todo_ponto_e_multiplo_do_passo_desde_0_UT(self):
        for husec in self.husecs:
            self.assertEqual(husec % hats_export.PASSO_NIVEL1_100MS, 0)

    def test_nao_extrapola_nenhuma_das_series(self):
        """
        A grade só pode cobrir onde TODAS as séries têm nó: fora disso o valor seria
        invenção. A série demodulada começa no centro da primeira janela de 128
        amostras, e o apontamento útil começa depois.
        """
        demodulado = hats_export.HatsExport._demodulate(self.detector)
        validos = [r for r in self.apontamento if r["JD"] != 0.0]
        primeiro = max(demodulado[0]["HUSEC"], self.detector[0]["HUSEC"],
                       validos[0]["HUSEC"])
        ultimo = min(demodulado[-1]["HUSEC"], self.detector[-1]["HUSEC"],
                     validos[-1]["HUSEC"])
        self.assertGreaterEqual(self.husecs[0], primeiro)
        self.assertLessEqual(self.husecs[-1], ultimo)

    def test_o_produto_de_1s_tem_cerca_de_um_decimo_das_linhas(self):
        de_um_segundo = self.export.GenerateCSVFileLevel1Format1s(self.entrada)
        _, linhas = self.tabela(de_um_segundo)
        self.assertAlmostEqual(len(self.linhas) / len(linhas), 10.0, delta=1.0)

    def test_os_dois_produtos_declaram_resolucoes_diferentes(self):
        de_um_segundo = self.export.GenerateCSVFileLevel1Format1s(self.entrada)
        self.assertEqual(self.gerado.scientific_product_key, utils.PRODUTO_NIVEL1_100MS)
        self.assertEqual(self.gerado.product_resolution, utils.RESOLUCAO_100MS)
        self.assertEqual(de_um_segundo.scientific_product_key, utils.PRODUTO_NIVEL1_1S)
        self.assertEqual(de_um_segundo.product_resolution, utils.RESOLUCAO_1S)
        self.assertEqual(de_um_segundo.level, "derived")

    def test_a_grade_de_1s_cai_em_segundo_inteiro(self):
        de_um_segundo = self.export.GenerateCSVFileLevel1Format1s(self.entrada)
        _, linhas = self.tabela(de_um_segundo)
        for linha in linhas:
            self.assertTrue(linha[0].endswith(".000000"), linha[0])


class TestValoresInterpolados(Level1TestCase):

    def test_a_amplitude_vem_da_serie_demodulada_verificada(self):
        """
        A amplitude de cada ponto da grade tem de ser a interpolação linear da série
        que o `hats.rbd` produz — o caminho confrontado bit a bit com o binário do
        CRAAM. A interpolação de conferência é escrita com busca binária, de outra
        forma que a do plugin.
        """
        esquema = schema_module.load(XML_DIR, "rbd")
        opcoes = {"record_limit": None, "demodulate": True,
                  "window_size": constants.WINDOW_SIZE, "steps": constants.STEPS,
                  "target_frequency": constants.TARGET_FREQUENCY,
                  "sampling_frequency": constants.SAMPLING_FREQUENCY,
                  "bin_mode": "reference"}
        referencia = rbd.analyse_stdlib(self.dia["rbd"], esquema, opcoes)["deconv"]
        nos_x = [int(h) for h in referencia[0]]
        nos_y = [float(a) for a in referencia[1]]

        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        indice, linhas = self.tabela(gerado)
        for linha in linhas:
            husec = husec_do_carimbo(linha[0])
            esperado = interpola_com_bisect(nos_x, nos_y, husec)
            self.assertEqual(linha[indice["AMPLITUDE"]], utils.format_float(esperado),
                             "husec {}".format(husec))

    def test_o_apontamento_vem_da_interpolacao_dos_registros_validos(self):
        validos = [r for r in self.apontamento if r["JD"] != 0.0]
        nos_x = [r["HUSEC"] for r in validos]
        for campo in ("ELEVATION", "AZIMUTH", "SID"):
            with self.subTest(campo=campo):
                nos_y = [float(r[campo]) for r in validos]
                gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
                indice, linhas = self.tabela(gerado)
                for linha in linhas:
                    husec = husec_do_carimbo(linha[0])
                    esperado = interpola_com_bisect(nos_x, nos_y, husec)
                    self.assertEqual(linha[indice[campo]],
                                     utils.format_float(esperado))

    def test_os_categoricos_so_trazem_valor_que_o_instrumento_escreveu(self):
        validos = [r for r in self.apontamento if r["JD"] != 0.0]
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        indice, linhas = self.tabela(gerado)
        for campo in ("OBJECT", "OPMODE"):
            with self.subTest(campo=campo):
                escritos = {int(r[campo]) for r in validos}
                publicados = {int(linha[indice[campo]]) for linha in linhas}
                self.assertTrue(publicados.issubset(escritos),
                                "{} publicou {} e o instrumento escreveu {}".format(
                                    campo, publicados, escritos))

    def test_os_categoricos_saem_inteiros(self):
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        indice, linhas = self.tabela(gerado)
        for linha in linhas:
            for campo in ("OBJECT", "OPMODE"):
                self.assertNotIn(".", linha[indice[campo]])

    def test_a_temperatura_vem_do_detector_e_nao_do_apontamento(self):
        """
        As três temperaturas são do `.rbd`, a 1 ms. Se viessem interpoladas dos nós
        errados o número continuaria plausível — daí o teste.
        """
        nos_x = [r["HUSEC"] for r in self.detector]
        nos_y = [float(r["TEMP_GOLAY"]) for r in self.detector]
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        indice, linhas = self.tabela(gerado)
        for linha in linhas[:40]:
            husec = husec_do_carimbo(linha[0])
            esperado = interpola_com_bisect(nos_x, nos_y, husec)
            self.assertEqual(linha[indice["TEMP_GOLAY"]],
                             utils.format_float(esperado))


class TestDecisaoPendenteDoApontamento(Level1TestCase):
    """
    A primeira das duas decisões pendentes: usar ou não como nó os registros de
    apontamento com `jd == 0`, que trazem solução de ~1053 s antes.

    Os dois caminhos estão implementados e testados, e o arquivo declara qual
    rodou — é uma constante em hats_utils.py para inverter.
    """

    def test_o_padrao_descarta_os_defasados(self):
        self.assertTrue(utils.NIVEL1_DESCARTA_APONTAMENTO_DEFASADO)

    def test_descartando_a_grade_comeca_depois_do_primeiro_valido(self):
        """
        Na fixture o registro de índice 0 é defasado, então o primeiro nó útil é o de
        índice 1 — e a grade não pode começar antes dele.
        """
        validos = [r for r in self.apontamento if r["JD"] != 0.0]
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        _, linhas = self.tabela(gerado)
        self.assertGreaterEqual(husec_do_carimbo(linhas[0][0]), validos[0]["HUSEC"])

    def test_mantendo_os_defasados_o_apontamento_muda(self):
        """
        Inverter a decisão tem de mudar o número publicado, senão o teste do padrão
        não estaria provando nada. Na fixture o registro defasado carrega o estado de
        mil posições antes, o que desloca a elevação em cerca de 0,2 grau.
        """
        com_descarte = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        indice, linhas_com = self.tabela(com_descarte)

        with mock.patch.object(utils, "NIVEL1_DESCARTA_APONTAMENTO_DEFASADO", False):
            sem_descarte = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
            _, linhas_sem = self.tabela(sem_descarte)

        por_husec_com = {husec_do_carimbo(l[0]): l[indice["ELEVATION"]]
                         for l in linhas_com}
        por_husec_sem = {husec_do_carimbo(l[0]): l[indice["ELEVATION"]]
                         for l in linhas_sem}
        comuns = set(por_husec_com).intersection(por_husec_sem)
        self.assertTrue(comuns)

        diferentes = [h for h in comuns if por_husec_com[h] != por_husec_sem[h]]
        self.assertTrue(diferentes,
                        "as duas decisoes deram exatamente o mesmo resultado")

        maior = max(abs(float(por_husec_com[h]) - float(por_husec_sem[h]))
                    for h in diferentes)
        self.assertGreater(maior, 0.1, "a diferenca medida foi {:.6f} grau".format(maior))

    def test_o_arquivo_declara_a_decisao_que_rodou(self):
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        texto = Path(gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("STALE POINTING", texto)
        self.assertIn("DISCARDED as interpolation nodes", texto)
        self.assertIn("pending confirmation", texto)

        with mock.patch.object(utils, "NIVEL1_DESCARTA_APONTAMENTO_DEFASADO", False):
            outro = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
            texto = Path(outro.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("KEPT as interpolation nodes", texto)
        self.assertNotIn("DISCARDED as interpolation nodes", texto)

    def test_sem_registro_valido_sobrando_falha_explicando(self):
        defasados = [dict(r, JD=0.0) for r in self.apontamento]
        with self.assertRaises(hats_export.InsufficientRecords) as capturado:
            self.export.GenerateCSVFileLevel1Format100ms(self.detector + defasados)
        self.assertIn("jd zerado", str(capturado.exception))


class TestDecisaoPendenteDosCategoricos(Level1TestCase):
    """
    A segunda decisão pendente: `object` e `opmode` são identificadores, e aqui vão
    por vizinho mais próximo.
    """

    def test_o_padrao_e_vizinho_mais_proximo(self):
        self.assertEqual(utils.NIVEL1_MODO_DOS_CATEGORICOS, "nearest")

    def test_o_arquivo_diz_como_os_categoricos_foram_reamostrados(self):
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        texto = Path(gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("nearest neighbour", texto)
        self.assertIn("did not write", texto)

    def test_outro_modo_falha_em_vez_de_inventar(self):
        """
        Não há segundo caminho implementado de propósito: inventar um modo que ele
        não pediu seria pior do que marcar a decisão.
        """
        with mock.patch.object(utils, "NIVEL1_MODO_DOS_CATEGORICOS", "linear"):
            with self.assertRaises(ValueError) as capturado:
                self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        self.assertIn("nunca escreveu", str(capturado.exception))


class TestAsDuasFontesNaMesmaChamada(Level1TestCase):
    """
    O level 1 é a única coisa que o Guigue pediu que precisa das duas fontes juntas,
    e o contrato entrega uma fonte por chamada. Quando falta uma, a mensagem tem de
    dizer isso — não dar coluna vazia nem erro de chave.
    """

    def test_so_detector_falha_dizendo_que_falta_apontamento(self):
        with self.assertRaises(hats_export.WrongStructure) as capturado:
            self.export.GenerateCSVFileLevel1Format100ms(self.detector)
        mensagem = str(capturado.exception)
        self.assertIn("apontamento", mensagem)
        self.assertIn("catalogo", mensagem)

    def test_so_apontamento_falha_dizendo_que_falta_detector(self):
        with self.assertRaises(hats_export.WrongStructure) as capturado:
            self.export.GenerateCSVFileLevel1Format100ms(self.apontamento)
        self.assertIn("detector", str(capturado.exception))

    def test_a_ordem_em_que_as_fontes_chegam_nao_importa(self):
        uma = self.export.GenerateCSVFileLevel1Format100ms(
            self.detector + self.apontamento)
        outra = self.export.GenerateCSVFileLevel1Format100ms(
            self.apontamento + self.detector)
        self.assertEqual(self.itens(uma)[1:], self.itens(outra)[1:])

    def test_sem_canal_de_temperatura_falha_explicando(self):
        """O formato de 26 bytes do `.rbd` não tem temperatura, e dele não sai este
        produto."""
        sem_temperatura = [{k: v for k, v in r.items() if k != "TEMP_ENV"}
                           for r in self.detector]
        with self.assertRaises(hats_export.WrongStructure) as capturado:
            self.export.GenerateCSVFileLevel1Format100ms(
                sem_temperatura + self.apontamento)
        self.assertIn("26 bytes", str(capturado.exception))


class TestArquivos(Level1TestCase):

    def test_o_cabecalho_declara_a_resolucao_e_o_nivel(self):
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        blocos = blocos_do_csv(gerado.generated_file_full_path)
        cabecalho = {l[0]: l[1] for l in blocos["#HEADER"] if len(l) == 2}
        self.assertEqual(cabecalho["FILERES"], utils.RESOLUCAO_100MS)
        self.assertEqual(cabecalho["LEVEL"], "derived")
        self.assertEqual(cabecalho["TZ"], "UTC")

    def test_o_arquivo_explica_a_grade_e_a_interpolacao(self):
        gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        texto = Path(gerado.generated_file_full_path).read_text(encoding="utf-8")
        self.assertIn("uniform time grid", texto)
        self.assertIn("0 UT", texto)
        self.assertIn("not an average over the interval", texto)
        self.assertIn("nothing is extrapolated", texto)

    def test_o_adapter_delega_os_quatro_metodos(self):
        adapter = plugin.HatsInstrument(xml_dir=str(XML_DIR))
        for metodo in ("GenerateCSVFileLevel1Format100ms",
                       "GenerateCSVFileLevel1Format1s"):
            with self.subTest(metodo=metodo):
                gerado = getattr(adapter, metodo)(self.entrada)
                self.assertTrue(Path(gerado.generated_file_full_path).exists())
        for metodo in ("GenerateFitsFileLevel1Format100ms",
                       "GenerateFitsFileLevel1Format1s"):
            with self.subTest(metodo=metodo):
                try:
                    from astropy.io import fits                               # noqa: F401
                except ImportError:
                    continue
                gerado = getattr(adapter, metodo)(self.entrada)
                self.assertTrue(Path(gerado.generated_file_full_path).exists())

    def test_o_fits_tem_a_camada_do_level1(self):
        try:
            from astropy.io import fits
        except ImportError:
            self.skipTest("astropy nao instalado neste ambiente")
        gerado = self.export.GenerateFitsFileLevel1Format100ms(self.entrada)
        with fits.open(gerado.generated_file_full_path) as hdus:
            self.assertEqual([hdu.name for hdu in hdus[1:]], ["LEVEL1"])
            self.assertEqual(list(hdus["Level1"].columns.names), COLUNAS_ESPERADAS)

    def test_o_fits_e_o_csv_trazem_os_mesmos_numeros(self):
        try:
            from astropy.io import fits
        except ImportError:
            self.skipTest("astropy nao instalado neste ambiente")
        csv_gerado = self.export.GenerateCSVFileLevel1Format100ms(self.entrada)
        indice, linhas = self.tabela(csv_gerado)
        fits_gerado = self.export.GenerateFitsFileLevel1Format100ms(self.entrada)
        with fits.open(fits_gerado.generated_file_full_path) as hdus:
            dados = hdus["Level1"].data
            self.assertEqual(len(dados), len(linhas))
            for posicao, linha in enumerate(linhas[:50]):
                self.assertEqual(dados["ISO_DATETIME"][posicao], linha[0])
                self.assertAlmostEqual(float(dados["AMPLITUDE"][posicao]),
                                       float(linha[indice["AMPLITUDE"]]), places=6)
                self.assertEqual(int(dados["OPMODE"][posicao]),
                                 int(linha[indice["OPMODE"]]))


class TestValoresPendentes(unittest.TestCase):

    def test_as_chaves_estao_isoladas_em_hats_utils(self):
        self.assertEqual(utils.PRODUTO_NIVEL1_100MS, "level1_100ms")
        self.assertEqual(utils.PRODUTO_NIVEL1_1S, "level1_1s")

    def test_o_codigo_da_exportacao_nao_repete_as_chaves(self):
        fonte = (PLUGIN_DIR / "hats_export.py").read_text(encoding="utf-8")
        for valor in ('"level1_100ms"', '"level1_1s"'):
            self.assertNotIn(valor, fonte)


@unittest.skipUnless(DADOS_REAIS, "defina HATS_TEST_DATA, ou rode make test-real")
class TestDadosReais(unittest.TestCase):
    """O level 1 sobre o par de arquivos de verdade."""

    LIMITE = 200000

    # O primeiro registro de apontamento defasado desta hora está em t+270 s, então
    # o teste que compara as duas decisões precisa de uma janela maior.
    LIMITE_COM_DEFASADOS = 400000

    def setUp(self):
        self._saida = tempfile.TemporaryDirectory()
        self._anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._saida.name
        self.addCleanup(self._restaura)

        self.rbd_path = Path(DADOS_REAIS) / "hats-2026-03-17T1800.rbd"
        self.aux_path = Path(DADOS_REAIS) / "aux" / "hats-2026-03-17T1800.aux"
        if not self.rbd_path.exists() or not self.aux_path.exists():
            self.skipTest("nao achei o par de arquivos de 2026-03-17T1800")

        ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR),
                                                 record_limit=self.LIMITE)
        self.detector = ingestion.load_data(um_spec(), str(self.rbd_path)).data_records
        self.apontamento = ingestion.load_data(um_spec(), str(self.aux_path)).data_records
        self.export = hats_export.HatsExport()

    def _restaura(self):
        self._saida.cleanup()
        if self._anterior is None:
            os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
        else:
            os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = self._anterior

    def test_a_grade_e_uniforme_sobre_o_dado_real(self):
        gerado = self.export.GenerateCSVFileLevel1Format100ms(
            self.detector + self.apontamento)
        itens = [l for l in blocos_do_csv(gerado.generated_file_full_path)["#ITEMS"] if l]
        husecs = [husec_do_carimbo(linha[0]) for linha in itens[1:]]

        self.assertGreater(len(husecs), 1000)
        passos = {b - a for a, b in zip(husecs, husecs[1:])}
        self.assertEqual(passos, {hats_export.PASSO_NIVEL1_100MS})
        for husec in husecs:
            self.assertEqual(husec % hats_export.PASSO_NIVEL1_100MS, 0)

    def test_a_amplitude_da_grade_vem_da_serie_verificada(self):
        esquema = schema_module.load(XML_DIR, "rbd")
        opcoes = {"record_limit": self.LIMITE, "demodulate": True,
                  "window_size": constants.WINDOW_SIZE, "steps": constants.STEPS,
                  "target_frequency": constants.TARGET_FREQUENCY,
                  "sampling_frequency": constants.SAMPLING_FREQUENCY,
                  "bin_mode": "reference"}
        referencia = rbd.analyse_stdlib(self.rbd_path, esquema, opcoes)["deconv"]
        nos_x = [int(h) for h in referencia[0]]
        nos_y = [float(a) for a in referencia[1]]

        gerado = self.export.GenerateCSVFileLevel1Format1s(
            self.detector + self.apontamento)
        itens = [l for l in blocos_do_csv(gerado.generated_file_full_path)["#ITEMS"] if l]
        indice = {nome: posicao for posicao, nome in enumerate(itens[0])}
        for linha in itens[1:]:
            husec = husec_do_carimbo(linha[0])
            esperado = interpola_com_bisect(nos_x, nos_y, husec)
            self.assertEqual(linha[indice["AMPLITUDE"]], utils.format_float(esperado),
                             "husec {}".format(husec))

    def test_os_defasados_reais_mudam_o_resultado(self):
        """
        A decisão pendente não é teórica: no dado real ela muda o apontamento
        publicado. Mede quanto.

        Precisa de uma janela maior que a dos outros testes: nesta hora o primeiro
        registro com `jd == 0` está em t+270 s, e com 200 mil amostras de detector a
        grade cobre só 199 s — não alcançaria nenhum defasado, e o teste passaria
        sem provar nada. Por isso a premissa é verificada antes da medição.
        """
        ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR),
                                                 record_limit=self.LIMITE_COM_DEFASADOS)
        detector = ingestion.load_data(um_spec(), str(self.rbd_path)).data_records
        entrada = detector + self.apontamento

        dentro = [r for r in self.apontamento
                  if r["JD"] == 0.0
                  and detector[0]["HUSEC"] <= r["HUSEC"] <= detector[-1]["HUSEC"]]
        self.assertTrue(dentro,
                        "nenhum registro defasado cai na janela de {:.0f} s: aumente "
                        "LIMITE_COM_DEFASADOS".format(
                            (detector[-1]["HUSEC"] - detector[0]["HUSEC"]) / 10000.0))

        com = self.export.GenerateCSVFileLevel1Format1s(entrada)
        itens_com = [l for l in blocos_do_csv(com.generated_file_full_path)["#ITEMS"] if l]
        indice = {nome: posicao for posicao, nome in enumerate(itens_com[0])}

        with mock.patch.object(utils, "NIVEL1_DESCARTA_APONTAMENTO_DEFASADO", False):
            sem = self.export.GenerateCSVFileLevel1Format1s(entrada)
            itens_sem = [l for l in blocos_do_csv(sem.generated_file_full_path)["#ITEMS"] if l]

        por_husec_com = {husec_do_carimbo(l[0]): float(l[indice["ELEVATION"]])
                         for l in itens_com[1:]}
        por_husec_sem = {husec_do_carimbo(l[0]): float(l[indice["ELEVATION"]])
                         for l in itens_sem[1:]}
        comuns = set(por_husec_com).intersection(por_husec_sem)
        maior = max(abs(por_husec_com[h] - por_husec_sem[h]) for h in comuns)
        self.assertGreater(maior, 0.0,
                           "no dado real as duas decisoes deram o mesmo resultado")
        # fica registrado quanto é, porque este número é argumento para a conversa
        print("\n    elevacao: as duas decisoes diferem em ate {:.4f} grau sobre "
              "{} pontos da grade, com {} registros defasados na janela".format(
                  maior, len(comuns), len(dentro)))


@unittest.skipUnless(DADOS_REAIS, "defina HATS_TEST_DATA, ou rode make test-real")
class TestOQueOArquivoAfirma(unittest.TestCase):
    """
    O arquivo do level 1 afirma dois números medidos sobre o descarte: a maior
    lacuna que ele deixa entre dois registros de apontamento úteis, e quanto a
    elevação muda ao longo dela. Estes testes varrem todas as horas disponíveis e
    impedem as duas afirmações de envelhecerem.
    """

    MAIOR_LACUNA_AFIRMADA = 67.0          # segundos
    MAIOR_VARIACAO_AFIRMADA = 0.23        # graus de elevação ao longo da lacuna

    def setUp(self):
        self.auxiliares = sorted((Path(DADOS_REAIS) / "aux").glob("*.aux"))
        if not self.auxiliares:
            self.skipTest("nenhum .aux em {}".format(Path(DADOS_REAIS) / "aux"))
        self.ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))

    def test_descartar_os_defasados_nao_abre_lacuna_maior_que_a_afirmada(self):
        maior_lacuna = 0.0
        maior_variacao = 0.0
        for caminho in self.auxiliares:
            registros = self.ingestion.load_data(um_spec(), str(caminho)).data_records
            validos = [r for r in registros if r["JD"] != 0.0]
            if len(validos) < 2:
                continue
            for anterior, seguinte in zip(validos, validos[1:]):
                lacuna = (seguinte["HUSEC"] - anterior["HUSEC"]) / float(
                    constants.HUSEC_PER_SECOND)
                if lacuna > maior_lacuna:
                    maior_lacuna = lacuna
                    maior_variacao = abs(seguinte["ELEVATION"] - anterior["ELEVATION"])

        # tolerância de 1 s e de 0,01 grau: os números do arquivo são arredondados
        self.assertLessEqual(maior_lacuna, self.MAIOR_LACUNA_AFIRMADA + 1.0,
                             "a maior lacuna medida foi {:.1f} s".format(maior_lacuna))
        self.assertLessEqual(maior_variacao, self.MAIOR_VARIACAO_AFIRMADA + 0.01,
                             "a elevacao mudou {:.3f} grau na maior lacuna".format(
                                 maior_variacao))
        print("\n    maior lacuna sem defasado: {:.1f} s, e a elevacao muda "
              "{:.3f} grau nela".format(maior_lacuna, maior_variacao))

    def test_a_hora_sem_nenhum_apontamento_valido_falha_explicando(self):
        """
        Existe de verdade: em 2026-03-18T1100 nenhum registro do `.aux` tem jd
        preenchido. Com o descarte ligado não sobra nó, e o produto não pode sair —
        tem de falhar dizendo o que aconteceu, não publicar coluna inventada.
        """
        rbd_path = Path(DADOS_REAIS) / "hats-2026-03-18T1100.rbd"
        aux_path = Path(DADOS_REAIS) / "aux" / "hats-2026-03-18T1100.aux"
        if not rbd_path.exists() or not aux_path.exists():
            self.skipTest("nao achei o par de 2026-03-18T1100")

        ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR),
                                                 record_limit=200000)
        detector = ingestion.load_data(um_spec(), str(rbd_path)).data_records
        apontamento = ingestion.load_data(um_spec(), str(aux_path)).data_records
        self.assertTrue(all(r["JD"] == 0.0 for r in apontamento),
                        "esta hora deixou de ser a hora sem apontamento valido")

        saida = tempfile.TemporaryDirectory()
        anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = saida.name
        try:
            with self.assertRaises(hats_export.InsufficientRecords) as capturado:
                hats_export.HatsExport().GenerateCSVFileLevel1Format1s(
                    detector + apontamento)
        finally:
            saida.cleanup()
            if anterior is None:
                os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
            else:
                os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = anterior
        self.assertIn("jd zerado", str(capturado.exception))


if __name__ == "__main__":
    unittest.main()
