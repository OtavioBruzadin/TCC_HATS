"""
Testes do reader do binário.

O reader é a costura entre o plugin e o pacote `hats`. Ele não calcula nada, então
o que se verifica aqui é: se o esquema veio do XML e não de adivinhação, se o
arquivo é validado, se os canais saem em unidade física iguais ao que o pacote
produz, e se os dois formatos do HATS são reconhecidos.
"""

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

from tests import fixtures

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

import hats_binary_reader as reader                                     # noqa: E402

from hats import calibration, records, schema as schema_module          # noqa: E402

XML_DIR = REPO_ROOT / "XMLTables"


class ReaderTestCase(unittest.TestCase):

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.addCleanup(self._temp.cleanup)


class TestResolucaoDosXML(ReaderTestCase):

    def test_o_argumento_tem_precedencia(self):
        self.assertEqual(reader.resolve_xml_directory(str(XML_DIR)), XML_DIR)

    def test_sem_argumento_usa_HATSXMLPATH(self):
        anterior = os.environ.get("HATSXMLPATH")
        os.environ["HATSXMLPATH"] = str(XML_DIR)
        try:
            self.assertEqual(reader.resolve_xml_directory(), XML_DIR)
        finally:
            if anterior is None:
                del os.environ["HATSXMLPATH"]
            else:
                os.environ["HATSXMLPATH"] = anterior

    def test_sem_nada_falha_citando_a_variavel(self):
        anterior = os.environ.pop("HATSXMLPATH", None)
        try:
            with self.assertRaises(ValueError) as capturado:
                reader.resolve_xml_directory()
            self.assertIn("HATSXMLPATH", str(capturado.exception))
        finally:
            if anterior is not None:
                os.environ["HATSXMLPATH"] = anterior

    def test_diretorio_inexistente_falha(self):
        with self.assertRaises(ValueError):
            reader.resolve_xml_directory(str(self.root / "nao-existe"))

    def test_diretorio_sem_os_xml_falha_em_vez_de_adivinhar(self):
        """
        O hats.schema.load() cai num esquema fixo interno quando não acha os XML.
        Isso é razoável na linha de comando e inaceitável aqui: em produção,
        decodificar com layout adivinhado é pior do que não rodar.
        """
        vazio = self.root / "sem_xml"
        vazio.mkdir()
        with self.assertRaises(ValueError) as capturado:
            reader.load_schema("rbd", str(vazio))
        mensagem = str(capturado.exception)
        self.assertIn("HATSDataFormat", mensagem)
        self.assertIn("adivinhado", mensagem)

    def test_esquema_desconhecido_falha(self):
        with self.assertRaises(ValueError):
            reader.load_schema("ws", str(XML_DIR))


class TestEsquemas(ReaderTestCase):

    def test_formato_vigente_tem_38_bytes_e_nove_campos(self):
        esquema = reader.load_schema("rbd", str(XML_DIR))
        self.assertEqual(esquema["record_size"], 38)
        self.assertEqual(esquema["mode"], "xml")
        self.assertEqual([c["name"] for c in esquema["fields"]],
                         ["sample", "sec", "ms", "husec", "golay", "chopper",
                          "temp_hics", "temp_env", "temp_golay"])

    def test_apontamento_tem_80_bytes(self):
        esquema = reader.load_schema("aux", str(XML_DIR))
        self.assertEqual(esquema["record_size"], 80)
        self.assertEqual(esquema["mode"], "xml")

    def test_kind_vem_da_extensao(self):
        self.assertEqual(reader.kind_for(Path("hats-2026-03-17T1800.rbd")), "rbd")
        self.assertEqual(reader.kind_for(Path("hats-2026-03-17T1800.aux")), "aux")
        with self.assertRaises(ValueError):
            reader.kind_for(Path("hats-2026-03-17.ws"))


class TestCanalDoDetector(ReaderTestCase):
    """
    O canal mudou de nome entre as versões do formato. Procurar só por 'golay' faz
    um arquivo de 2021 ser lido inteiro e demodulado como se não tivesse detector,
    sem erro e sem saída — foi assim que o problema apareceu.
    """

    def test_no_formato_de_38_bytes_chama_golay(self):
        esquema = reader.load_schema("rbd", str(XML_DIR))
        self.assertEqual(reader.detector_channel(esquema)["name"], "golay")

    def test_no_formato_de_26_bytes_chama_adcu(self):
        so_o_110 = self.root / "xml110"
        so_o_110.mkdir()
        shutil.copy(XML_DIR / "HATSDataFormat-110.xml", so_o_110 / "HATSDataFormat-110.xml")
        esquema = reader.load_schema("rbd", str(so_o_110))

        self.assertEqual(esquema["record_size"], 26)
        self.assertEqual(reader.detector_channel(esquema)["name"], "adcu")

    def test_sem_nenhum_dos_dois_falha_listando_os_campos(self):
        esquema = {"name": "inventado",
                   "fields": [{"name": "husec"}, {"name": "chopper"}]}
        with self.assertRaises(ValueError) as capturado:
            reader.detector_channel(esquema)
        mensagem = str(capturado.exception)
        self.assertIn("golay", mensagem)
        self.assertIn("adcu", mensagem)
        self.assertIn("chopper", mensagem)


class TestCabecalho(ReaderTestCase):

    def test_cabecalho_do_rbd(self):
        dia = fixtures.build_day(self.root)
        esquema = reader.load_schema("rbd", str(XML_DIR))
        cabecalho = reader.read_header(dia["rbd"], esquema)

        self.assertEqual(cabecalho["FILENAME"], "hats-2026-03-17T1800.rbd")
        self.assertEqual(cabecalho["KIND"], "rbd")
        self.assertEqual(cabecalho["DATE"], "2026-03-17")
        self.assertEqual(cabecalho["HOUR"], "1800")
        self.assertEqual(cabecalho["NREC"], dia["samples"])
        self.assertEqual(cabecalho["RECORD_SIZE"], 38)
        self.assertEqual(cabecalho["DETECTOR_CHANNEL"], "golay")
        self.assertEqual(cabecalho["SCHEMA"]["mode"], "xml")

    def test_binario_truncado_falha_citando_o_tamanho(self):
        """É este erro que roteia o arquivo para invalid_format_dir_name."""
        dia = fixtures.build_day(self.root)
        truncado = self.root / "hats-2026-03-17T1900.rbd"
        truncado.write_bytes(dia["rbd"].read_bytes()[:38 * 10 + 7])

        esquema = reader.load_schema("rbd", str(XML_DIR))
        with self.assertRaises(ValueError) as capturado:
            reader.read_header(truncado, esquema)
        self.assertIn("38", str(capturado.exception))


class TestBlocosCalibrados(ReaderTestCase):

    def setUp(self):
        super().setUp()
        self.dia = fixtures.build_day(self.root)
        self.esquema = reader.load_schema("rbd", str(XML_DIR))

    def test_os_blocos_cobrem_o_arquivo_inteiro(self):
        total = sum(n for _bloco, n in
                    reader.iter_calibrated_blocks(self.dia["rbd"], self.esquema))
        self.assertEqual(total, self.dia["samples"])

    def test_cada_bloco_traz_todos_os_campos(self):
        """
        Cada canal do AD7770 aparece duas vezes: o valor físico e a contagem de
        sinal estendido que o originou. São as estruturas cData e rData do HATS.py.
        """
        bloco, quantidade = next(reader.iter_calibrated_blocks(self.dia["rbd"], self.esquema))

        declarados = [c["name"] for c in self.esquema["fields"]]
        do_adc = [c["name"] for c in self.esquema["fields"] if c.get("origin") == "ad7770"]
        esperados = declarados + [nome + reader.SUFIXO_CRU for nome in do_adc]
        self.assertEqual(sorted(bloco), sorted(esperados))

        for nome, coluna in bloco.items():
            with self.subTest(campo=nome):
                self.assertEqual(len(coluna), quantidade)

    def test_a_contagem_crua_e_o_valor_fisico_sao_coerentes(self):
        """
        O valor físico tem de ser exatamente a contagem passada pela reta declarada
        no XML — senão rData e cData contariam histórias diferentes do mesmo dado.
        """
        bloco, _ = next(reader.iter_calibrated_blocks(self.dia["rbd"], self.esquema,
                                                     record_limit=64))
        for campo in self.esquema["fields"]:
            if campo.get("origin") != "ad7770":
                continue
            nome = campo["name"]
            with self.subTest(campo=nome):
                ganho = float(campo.get("slope", 1.0))
                offset = float(campo.get("offset", 0.0))
                for cru, fisico in zip(bloco[nome + reader.SUFIXO_CRU], bloco[nome]):
                    self.assertAlmostEqual(cru * ganho + offset, fisico, places=12)

    def test_a_contagem_crua_e_inteira(self):
        bloco, _ = next(reader.iter_calibrated_blocks(self.dia["rbd"], self.esquema,
                                                     record_limit=16))
        for valor in bloco["golay" + reader.SUFIXO_CRU]:
            self.assertIsInstance(valor, int)

    def test_o_husec_sai_cru_e_o_detector_em_mV(self):
        bloco, _ = next(reader.iter_calibrated_blocks(self.dia["rbd"], self.esquema))
        self.assertEqual(bloco["husec"][0], 18 * 36000000)
        self.assertIsInstance(bloco["husec"][0], int)
        # a fixture grava uma senoide de 100 mV: a primeira amostra é sin(0) = 0
        self.assertAlmostEqual(bloco["golay"][0], 0.0, places=6)
        self.assertLess(max(bloco["golay"]), 101.0)
        self.assertGreater(min(bloco["golay"]), -101.0)

    def test_a_calibracao_bate_com_a_do_pacote(self):
        """
        O reader não pode ter conta própria: cada valor tem de ser o que o
        `hats.calibration` produz a partir do mesmo registro cru.
        """
        posicao_de = schema_module.field_index(self.esquema)
        crus = list(records.iter_records(self.dia["rbd"], self.esquema, limit=64))
        bloco, _ = next(reader.iter_calibrated_blocks(self.dia["rbd"], self.esquema,
                                                     record_limit=64))

        for campo in self.esquema["fields"]:
            nome = campo["name"]
            with self.subTest(campo=nome):
                esperado = [calibration.apply(campo, registro[posicao_de[nome]])
                            for registro in crus]
                self.assertEqual(bloco[nome], esperado)

    def test_o_limite_de_registros_e_respeitado(self):
        total = sum(n for _b, n in reader.iter_calibrated_blocks(
            self.dia["rbd"], self.esquema, record_limit=137))
        self.assertEqual(total, 137)

    def test_le_tambem_o_formato_de_26_bytes(self):
        so_o_110 = self.root / "xml110"
        so_o_110.mkdir()
        shutil.copy(XML_DIR / "HATSDataFormat-110.xml", so_o_110 / "HATSDataFormat-110.xml")
        esquema = reader.load_schema("rbd", str(so_o_110))

        antigo = self.root / "hats-2021-08-26T1800.rbd"
        amostras = fixtures.write_rbd_110(antigo, samples=300)

        cabecalho = reader.read_header(antigo, esquema)
        self.assertEqual(cabecalho["NREC"], amostras)
        self.assertEqual(cabecalho["RECORD_SIZE"], 26)
        self.assertEqual(cabecalho["DETECTOR_CHANNEL"], "adcu")

        bloco, _ = next(reader.iter_calibrated_blocks(antigo, esquema))
        self.assertIn("adcu", bloco)
        self.assertNotIn("golay", bloco)
        self.assertAlmostEqual(bloco["adcu"][0], 0.0, places=6)


class TestPontoDeEntrada(ReaderTestCase):

    def test_read_and_decode_devolve_cabecalho_esquema_e_blocos(self):
        dia = fixtures.build_day(self.root)
        cabecalho, esquema, blocos = reader.read_and_decode_hats_file(
            str(dia["rbd"]), xml_dir=str(XML_DIR))

        self.assertEqual(cabecalho["NREC"], dia["samples"])
        self.assertEqual(esquema["record_size"], 38)
        self.assertEqual(sum(n for _b, n in blocos), dia["samples"])

    def test_nada_e_lido_do_disco_antes_de_consumir(self):
        """Os blocos são um gerador: a ingestão controla quando o disco é tocado."""
        dia = fixtures.build_day(self.root)
        _cab, _esq, blocos = reader.read_and_decode_hats_file(
            str(dia["rbd"]), xml_dir=str(XML_DIR))
        import types
        self.assertIsInstance(blocos, types.GeneratorType)

    def test_arquivo_inexistente_falha(self):
        with self.assertRaises(ValueError):
            reader.read_and_decode_hats_file(str(self.root / "nao-existe.rbd"),
                                             xml_dir=str(XML_DIR))

    def test_aux_tambem_passa_pelo_reader(self):
        dia = fixtures.build_day(self.root)
        cabecalho, esquema, blocos = reader.read_and_decode_hats_file(
            str(dia["aux"]), xml_dir=str(XML_DIR))

        self.assertEqual(cabecalho["KIND"], "aux")
        self.assertNotIn("DETECTOR_CHANNEL", cabecalho)
        bloco, _ = next(blocos)
        self.assertIn("elevation", bloco)
        self.assertIn("opmode", bloco)


if __name__ == "__main__":
    unittest.main()
