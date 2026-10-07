"""
Testes da ingestão do detector.

O que se verifica é o §5 e o §6 do guia: a assinatura, os quatro campos
obrigatórios do retorno, a presença do UTC_TIME, e sobretudo o passo 4 do §5.3 —
"garantir consistência temporal", que o §6 chama de Ponto crítico.
"""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tests import fixtures

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

import hats_ingestion                                                        # noqa: E402
from external_contracts.instrument_ingestion_runtime_spec import (            # noqa: E402
    InstrumentIngestionRuntimeSpec)

import struct
import tempfile

XML_DIR = REPO_ROOT / "XMLTables"
HORA = 18 * 36000000


def um_spec(resolucao="1ms"):
    """O que o pipeline montaria a partir do bloco `ingestion` do catálogo."""
    return InstrumentIngestionRuntimeSpec(
        instrument_name="HATS",
        time_field="UTC_TIME",
        native_resolutions_by_mode={"DEFAULT": resolucao},
        scan_ignore_dirs=[], include_prefixes=["hats-"], include_suffixes=[],
        include_extensions=[".rbd"], exclude_extensions=[], exclude_suffixes=[],
        processed_dir_name="00_processed", ignored_dir_name="00_ignored",
        invalid_format_dir_name="99_formato_invalid")


class IngestionTestCase(unittest.TestCase):

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.addCleanup(self._temp.cleanup)
        self.ingestion = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        self.spec = um_spec()

    def escreve_rbd(self, husecs, nome="hats-2026-03-17T1800.rbd"):
        """Um .rbd com os husec exatamente nesta ordem, para testar o passo 4."""
        caminho = self.root / nome
        with caminho.open("wb") as handle:
            for indice, husec in enumerate(husecs):
                handle.write(struct.pack(
                    fixtures.RBD_FORMAT, 1000 + indice, 1773770400, indice % 1000, husec,
                    fixtures.encode_ad7770(1000), fixtures.encode_ad7770(2000),
                    fixtures.encode_ad7770(1045255), fixtures.encode_ad7770(1001714),
                    fixtures.encode_ad7770(1694898)))
        return caminho


class TestRetornoObrigatorio(IngestionTestCase):
    """Guia §5.4."""

    def setUp(self):
        super().setUp()
        self.dia = fixtures.build_day(self.root)
        self.resultado = self.ingestion.load_data(self.spec, str(self.dia["rbd"]))

    def test_data_records_nao_vazio(self):
        self.assertEqual(len(self.resultado.data_records), self.dia["samples"])

    def test_data_time_resolution_vem_do_spec(self):
        self.assertEqual(self.resultado.data_time_resolution, "1ms")
        outro = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR))
        self.assertEqual(
            outro.load_data(um_spec("2ms"), str(self.dia["rbd"])).data_time_resolution,
            "2ms")

    def test_data_level_e_raw(self):
        """§1: ingestão sempre gera RAW."""
        self.assertEqual(self.resultado.data_level, "raw")

    def test_observation_day_e_a_meia_noite_do_dia(self):
        self.assertEqual(self.resultado.observation_day,
                         datetime(2026, 3, 17, tzinfo=timezone.utc))

    def test_o_caminho_de_origem_acompanha(self):
        self.assertEqual(self.resultado.source_file_path, str(self.dia["rbd"]))
        self.assertEqual(self.resultado.instrument_name, "HATS")


class TestEstruturaDoRegistro(IngestionTestCase):
    """Guia §6."""

    def setUp(self):
        super().setUp()
        self.dia = fixtures.build_day(self.root)
        self.registros = self.ingestion.load_data(self.spec, str(self.dia["rbd"])).data_records

    def test_tem_o_campo_UTC_TIME_com_data_completa(self):
        primeiro = self.registros[0]
        self.assertIn("UTC_TIME", primeiro)
        carimbo = primeiro["UTC_TIME"]
        self.assertIsInstance(carimbo, datetime)
        self.assertIsNotNone(carimbo.tzinfo)
        self.assertEqual((carimbo.year, carimbo.month, carimbo.day), (2026, 3, 17))
        self.assertEqual((carimbo.hour, carimbo.minute, carimbo.second), (18, 0, 0))

    def test_o_carimbo_vem_do_husec_exatamente(self):
        """1 husec = 100 µs, conta inteira, sem passar por float."""
        caminho = self.escreve_rbd([HORA + 3, HORA + 13, HORA + 23])
        registros = self.ingestion.load_data(self.spec, str(caminho)).data_records
        self.assertEqual(registros[0]["UTC_TIME"].microsecond, 300)
        self.assertEqual(registros[1]["UTC_TIME"].microsecond, 1300)
        self.assertEqual(registros[2]["UTC_TIME"].microsecond, 2300)

    def test_traz_o_husec_e_o_PROC_SEQ(self):
        self.assertEqual(self.registros[0]["HUSEC"], HORA)
        self.assertEqual(self.registros[0]["PROC_SEQ"], 0)
        self.assertEqual(self.registros[-1]["PROC_SEQ"], len(self.registros) - 1)

    def test_traz_os_canais_cientificos_em_unidade_fisica(self):
        primeiro = self.registros[0]
        for canal in ("GOLAY", "CHOPPER", "TEMP_HICS", "TEMP_ENV", "TEMP_GOLAY"):
            with self.subTest(canal=canal):
                self.assertIn(canal, primeiro)
                self.assertIsInstance(primeiro[canal], float)
        # a fixture grava uma senoide de 100 mV; sin(0) = 0
        self.assertAlmostEqual(primeiro["GOLAY"], 0.0, places=6)
        # as temperaturas ficam na faixa ambiente
        self.assertTrue(20.0 < primeiro["TEMP_HICS"] < 60.0, primeiro["TEMP_HICS"])

    def test_o_carimbo_nao_aparece_duas_vezes(self):
        """O husec vira UTC_TIME e HUSEC, mas não um campo chamado 'HUSEC' duplicado."""
        self.assertNotIn("husec", self.registros[0])
        self.assertEqual(sorted(self.registros[0]),
                         sorted(["UTC_TIME", "HUSEC", "PROC_SEQ", "FULL_FILE_PATH",
                                 "SAMPLE", "SEC", "MS",
                                 "GOLAY", "CHOPPER", "TEMP_HICS", "TEMP_ENV", "TEMP_GOLAY",
                                 "GOLAY_ADCU", "CHOPPER_ADCU", "TEMP_HICS_ADCU",
                                 "TEMP_ENV_ADCU", "TEMP_GOLAY_ADCU"]))

    def test_o_registro_guarda_a_contagem_crua_e_o_valor_fisico(self):
        """
        O Guigue pediu rData e cData lado a lado no level 0: a contagem do ADC com
        sinal estendido e o valor na unidade física que ela gera.
        """
        primeiro = self.registros[0]
        for canal in ("GOLAY", "CHOPPER", "TEMP_HICS", "TEMP_ENV", "TEMP_GOLAY"):
            with self.subTest(canal=canal):
                self.assertIsInstance(primeiro[canal + "_ADCU"], int)
                self.assertIsInstance(primeiro[canal], float)

    def test_a_procedencia_e_so_o_FULL_FILE_PATH(self):
        """
        Orientação do suporte: manter FULL_FILE_PATH, que identifica o arquivo de
        origem. FILEPATH atende compatibilidade legada do POEMAS e TELESCOPE é
        redundante — o pipeline já põe a identificação do instrumento em
        META.INSTRUMENT.
        """
        primeiro = self.registros[0]
        self.assertEqual(primeiro["FULL_FILE_PATH"], str(self.dia["rbd"]))
        self.assertNotIn("FILEPATH", primeiro)
        self.assertNotIn("TELESCOPE", primeiro)

    def test_o_caminho_e_o_mesmo_objeto_em_todos_os_registros(self):
        """Um caminho absoluto copiado 3,6 milhões de vezes seria caro sem motivo."""
        primeiro = self.registros[0]["FULL_FILE_PATH"]
        for registro in self.registros[1:50]:
            self.assertIs(registro["FULL_FILE_PATH"], primeiro)


class TestConsistenciaTemporal(IngestionTestCase):
    """
    Passo 4 do §5.3, o "Ponto crítico" do §6.

    No POEMAS foi preciso inventar o sub-segundo. Aqui o husec já existe, então o
    trabalho é verificar — e a verificação separa o que é irrecuperável do que é
    apenas perda de amostra.
    """

    def test_husec_duplicado_falha(self):
        caminho = self.escreve_rbd([HORA, HORA + 10, HORA + 10, HORA + 20])
        with self.assertRaises(ValueError) as capturado:
            self.ingestion.load_data(self.spec, str(caminho))
        mensagem = str(capturado.exception)
        self.assertIn("duplicado", mensagem)
        self.assertIn(str(HORA + 10), mensagem)

    def test_husec_fora_de_ordem_falha(self):
        caminho = self.escreve_rbd([HORA, HORA + 20, HORA + 10, HORA + 30])
        with self.assertRaises(ValueError) as capturado:
            self.ingestion.load_data(self.spec, str(caminho))
        self.assertIn("decrescente", str(capturado.exception))

    def test_lacuna_avisa_e_nao_falha(self):
        """
        Lacuna é perda de amostra do sistema de aquisição, e acontece. Recusar o dia
        por isso seria pior do que registrar.
        """
        # salto alinhado à grade: de +10 a +1020 faltam as 100 amostras do meio
        caminho = self.escreve_rbd([HORA, HORA + 10, HORA + 1020, HORA + 1030])
        resultado = self.ingestion.load_data(self.spec, str(caminho))

        self.assertEqual(len(resultado.data_records), 4)
        self.assertEqual(resultado.instrument_metadata["time_gaps"], 1)
        self.assertEqual(resultado.instrument_metadata["samples_missing_in_gaps"], 100)
        self.assertTrue(any("lacuna" in aviso for aviso in resultado.processing_warnings))

    def test_a_contagem_de_amostras_perdidas_e_aproximada(self):
        """
        A lacuna medida na hora real das 18:00 é de 1007 husec — não é múltiplo de
        10, ou seja o instrumento não retomou na grade. Então "quantas amostras
        faltaram" não tem resposta exata, e o aviso diz "cerca de".
        """
        caminho = self.escreve_rbd([HORA, HORA + 10, HORA + 1017, HORA + 1027])
        resultado = self.ingestion.load_data(self.spec, str(caminho))

        self.assertEqual(resultado.instrument_metadata["time_gaps"], 1)
        self.assertEqual(resultado.instrument_metadata["samples_missing_in_gaps"], 99)
        self.assertTrue(any("cerca de" in a for a in resultado.processing_warnings))

    def test_serie_perfeita_nao_gera_aviso_de_lacuna(self):
        caminho = self.escreve_rbd([HORA + 10 * i for i in range(50)])
        resultado = self.ingestion.load_data(self.spec, str(caminho))
        self.assertEqual(resultado.instrument_metadata["time_gaps"], 0)
        self.assertEqual(resultado.processing_warnings, [])


class TestDescarteAntesDaHora(IngestionTestCase):

    def test_os_registros_anteriores_a_hora_saem(self):
        caminho = self.escreve_rbd([HORA - 30, HORA - 20, HORA - 10, HORA, HORA + 10])
        resultado = self.ingestion.load_data(self.spec, str(caminho))

        self.assertEqual(len(resultado.data_records), 2)
        self.assertEqual(resultado.instrument_metadata["records_dropped_before_hour"], 3)
        self.assertEqual(resultado.data_records[0]["HUSEC"], HORA)
        self.assertTrue(any("hora nominal" in a for a in resultado.processing_warnings))

    def test_a_amostra_real_descarta_559(self):
        """A Amostra/ reproduz o início de uma hora real, com 559 registros defasados."""
        amostra = REPO_ROOT / "Amostra" / "hats-2026-03-17T1800.rbd"
        if not amostra.exists():
            self.skipTest("Amostra/ não encontrada")
        resultado = self.ingestion.load_data(self.spec, str(amostra))
        self.assertEqual(resultado.instrument_metadata["records_dropped_before_hour"], 559)

    def test_o_PROC_SEQ_conta_so_o_que_ficou(self):
        caminho = self.escreve_rbd([HORA - 20, HORA - 10, HORA, HORA + 10])
        registros = self.ingestion.load_data(self.spec, str(caminho)).data_records
        self.assertEqual([r["PROC_SEQ"] for r in registros], [0, 1])


class TestRecusas(IngestionTestCase):

    def test_aux_e_ws_sao_recusados_com_explicacao(self):
        dia = fixtures.build_day(self.root)
        for caminho in (dia["aux"], dia["ws"]):
            with self.subTest(arquivo=caminho.name):
                with self.assertRaises(ValueError) as capturado:
                    self.ingestion.load_data(self.spec, str(caminho))
                self.assertIn("fonte RAW", str(capturado.exception))

    def test_nome_fora_da_convencao_falha(self):
        fora = self.root / "detector.rbd"
        fora.write_bytes((self.escreve_rbd([HORA]).read_bytes()))
        with self.assertRaises(ValueError):
            self.ingestion.load_data(self.spec, str(fora))

    def test_arquivo_truncado_falha(self):
        dia = fixtures.build_day(self.root)
        truncado = self.root / "hats-2026-03-17T1900.rbd"
        truncado.write_bytes(dia["rbd"].read_bytes()[:38 * 5 + 11])
        with self.assertRaises(ValueError):
            self.ingestion.load_data(self.spec, str(truncado))

    def test_limite_de_registros_e_respeitado(self):
        dia = fixtures.build_day(self.root)
        limitada = hats_ingestion.HatsIngestion(xml_dir=str(XML_DIR), record_limit=200)
        resultado = limitada.load_data(self.spec, str(dia["rbd"]))
        self.assertEqual(len(resultado.data_records), 200)


class TestMetadados(IngestionTestCase):

    def test_o_que_o_metadado_registra(self):
        dia = fixtures.build_day(self.root)
        metadados = self.ingestion.load_data(self.spec, str(dia["rbd"])).instrument_metadata

        self.assertEqual(metadados["observation_date"], "2026-03-17")
        self.assertEqual(metadados["observation_hour"], "1800")
        self.assertEqual(metadados["time_field"], "UTC_TIME")
        self.assertEqual(metadados["detector_channel"], "golay")
        self.assertEqual(metadados["records_in_file"], dia["samples"])
        self.assertEqual(metadados["records_kept"], dia["samples"])
        self.assertEqual(metadados["schema"]["record_size"], 38)
        self.assertEqual(metadados["schema"]["mode"], "xml")
        self.assertEqual(metadados["acquisition"]["sampling_frequency_hz"], 1000.0)
        self.assertEqual(metadados["acquisition"]["chopper_frequency_hz"], 20.0)


if __name__ == "__main__":
    unittest.main()
