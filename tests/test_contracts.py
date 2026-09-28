"""
Testes dos contratos do MackSun.

Os arquivos em `plugins/hats/external_contracts/` são cópias do que o MackSun
distribui, e não código nosso — então o que se testa aqui não é a nossa lógica, é
se as cópias **estão íntegras e se comportam como o guia descreve**. Se uma
validação deixar de disparar, ou um campo obrigatório deixar de ser obrigatório,
tudo o que for construído em cima está apoiado numa suposição errada.

O guia §5.4 lista os campos obrigatórios do TimeSeriesLoadDataResult e o §10 os do
TimeSeriesSpecialistGeneratedFile. Cada item das duas listas tem um teste.
"""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

from external_contracts.errors import ValidationError                          # noqa: E402
from external_contracts.instrument_ingestion_runtime_spec import (             # noqa: E402
    InstrumentIngestionRuntimeSpec)
from external_contracts.time_series_export_adapter import TimeSeriesExportAdapter        # noqa: E402
from external_contracts.time_series_ingestion_adapter import TimeSeriesIngestionAdapter  # noqa: E402
from external_contracts.time_series_load_data_result import TimeSeriesLoadDataResult     # noqa: E402
from external_contracts.time_series_specialist_generated_file import (         # noqa: E402
    TimeSeriesSpecialistGeneratedFile)

UM_DIA = datetime(2026, 3, 17, tzinfo=timezone.utc)
UM_REGISTRO = [{"UTC_TIME": UM_DIA}]


def load_data_result(**alteracoes):
    """Um resultado válido, com os campos que o teste quiser trocar."""
    argumentos = {
        "data_records": UM_REGISTRO,
        "data_time_resolution": "1ms",
        "data_level": "raw",
        "observation_day": UM_DIA,
    }
    argumentos.update(alteracoes)
    return TimeSeriesLoadDataResult(**argumentos)


def generated_file(**alteracoes):
    argumentos = {
        "instrument_name": "HATS",
        "target_date": UM_DIA,
        "level": "raw",
        "scientific_product_key": "algum_produto",
        "product_resolution": "1ms",
        "generated_file_format": "CSV",
        "generated_file_full_path": "/tmp/algum_arquivo.csv",
    }
    argumentos.update(alteracoes)
    return TimeSeriesSpecialistGeneratedFile(**argumentos)


class TestLoadDataResult(unittest.TestCase):
    """Guia §5.4: data_records não vazio, data_time_resolution, data_level, observation_day."""

    def test_o_caso_valido_passa(self):
        resultado = load_data_result()
        self.assertEqual(resultado.data_level, "raw")
        self.assertEqual(len(resultado.data_records), 1)

    def test_data_records_tem_de_ser_lista(self):
        with self.assertRaises(ValidationError):
            load_data_result(data_records={"UTC_TIME": UM_DIA})

    def test_data_records_nao_pode_ser_vazio(self):
        with self.assertRaises(ValidationError):
            load_data_result(data_records=[])

    def test_data_time_resolution_e_obrigatorio(self):
        for ausente in ("", "   ", None):
            with self.subTest(valor=repr(ausente)):
                with self.assertRaises(ValidationError):
                    load_data_result(data_time_resolution=ausente)

    def test_data_level_so_aceita_raw_ou_derived(self):
        for aceito in ("raw", "derived", "RAW", " Derived "):
            with self.subTest(valor=aceito):
                self.assertTrue(load_data_result(data_level=aceito))
        for recusado in ("level2", "", None, "calibrated"):
            with self.subTest(valor=repr(recusado)):
                with self.assertRaises(ValidationError):
                    load_data_result(data_level=recusado)

    def test_observation_day_e_obrigatorio_e_datetime(self):
        with self.assertRaises(ValidationError):
            load_data_result(observation_day=None)
        with self.assertRaises(ValidationError):
            load_data_result(observation_day="2026-03-17")
        with self.assertRaises(ValidationError):
            load_data_result(observation_day=UM_DIA.date())

    def test_os_campos_opcionais_tem_padrao(self):
        resultado = load_data_result()
        self.assertEqual(resultado.instrument_name, "")
        self.assertEqual(resultado.acquisition_mode, "")
        self.assertEqual(resultado.processing_warnings, [])
        self.assertEqual(resultado.instrument_metadata, {})

    def test_o_resultado_e_imutavel(self):
        """frozen=True: o pipeline não pode ter o resultado alterado depois de validado."""
        resultado = load_data_result()
        with self.assertRaises(Exception):
            resultado.data_level = "derived"


class TestSpecialistGeneratedFile(unittest.TestCase):
    """Guia §10: os sete campos do retorno da exportação."""

    def test_o_caso_valido_passa(self):
        self.assertEqual(generated_file().generated_file_format, "CSV")

    def test_instrument_name_e_obrigatorio(self):
        for ausente in ("", "   ", None):
            with self.subTest(valor=repr(ausente)):
                with self.assertRaises(ValidationError):
                    generated_file(instrument_name=ausente)

    def test_target_date_tem_de_ser_datetime(self):
        with self.assertRaises(ValidationError):
            generated_file(target_date=None)
        with self.assertRaises(ValidationError):
            generated_file(target_date="2026-03-17")

    def test_level_so_aceita_raw_ou_derived(self):
        self.assertTrue(generated_file(level="derived"))
        for recusado in ("level2", "", None):
            with self.subTest(valor=repr(recusado)):
                with self.assertRaises(ValidationError):
                    generated_file(level=recusado)

    def test_scientific_product_key_e_obrigatorio(self):
        with self.assertRaises(ValidationError):
            generated_file(scientific_product_key="")

    def test_product_resolution_e_obrigatorio(self):
        with self.assertRaises(ValidationError):
            generated_file(product_resolution="")

    def test_generated_file_format_e_obrigatorio(self):
        with self.assertRaises(ValidationError):
            generated_file(generated_file_format="")

    def test_generated_file_full_path_e_obrigatorio(self):
        with self.assertRaises(ValidationError):
            generated_file(generated_file_full_path="   ")


class TestIngestionRuntimeSpec(unittest.TestCase):
    """
    O spec é construído pelo pipeline a partir do bloco `ingestion` do catálogo, e
    o plugin apenas o consome (guia §13.2). O teste fixa a forma que consumimos.
    """

    CAMPOS = ("instrument_name", "time_field", "native_resolutions_by_mode",
              "scan_ignore_dirs", "include_prefixes", "include_suffixes",
              "include_extensions", "exclude_extensions", "exclude_suffixes",
              "processed_dir_name", "ignored_dir_name", "invalid_format_dir_name")

    def um_spec(self):
        return InstrumentIngestionRuntimeSpec(
            instrument_name="HATS",
            time_field="UTC_TIME",
            native_resolutions_by_mode={"DETECTOR": "1ms"},
            scan_ignore_dirs=[], include_prefixes=[], include_suffixes=[],
            include_extensions=[], exclude_extensions=[], exclude_suffixes=[],
            processed_dir_name="00_processed", ignored_dir_name="00_ignored",
            invalid_format_dir_name="99_formato_invalid")

    def test_tem_os_doze_campos(self):
        spec = self.um_spec()
        for campo in self.CAMPOS:
            with self.subTest(campo=campo):
                self.assertTrue(hasattr(spec, campo))

    def test_todos_os_campos_sao_obrigatorios(self):
        """Nenhum tem padrão: o pipeline preenche os doze."""
        with self.assertRaises(TypeError):
            InstrumentIngestionRuntimeSpec(instrument_name="HATS", time_field="UTC_TIME")

    def test_o_spec_e_imutavel(self):
        with self.assertRaises(Exception):
            self.um_spec().time_field = "OUTRO"


class TestProtocolos(unittest.TestCase):
    """
    Os dois Protocols são runtime_checkable, então o pipeline pode conferir o
    adapter com isinstance. Confirmamos que a checagem é pela presença do método.
    """

    def test_ingestion_adapter_reconhece_quem_tem_load_data(self):
        class Qualquer(object):
            def load_data(self, spec, file_full_path):
                return None

        self.assertIsInstance(Qualquer(), TimeSeriesIngestionAdapter)

    def test_ingestion_adapter_recusa_quem_nao_tem(self):
        class SemNada(object):
            pass

        self.assertNotIsInstance(SemNada(), TimeSeriesIngestionAdapter)

    def test_export_adapter_exige_os_seis_metodos_obrigatorios(self):
        """Guia §7.2. O isinstance de um Protocol confere a presença de todos."""
        nomes = ("GenerateFitsFileRawFormat", "GenerateCSVFileRawFormat",
                 "GenerateFitsFileDerivedFormat100ms", "GenerateCSVFileDerivedFormat100ms",
                 "GenerateFitsFileDerivedFormat1s", "GenerateCSVFileDerivedFormat1s")

        completo = type("Completo", (), {nome: (lambda self, raw_records: None)
                                        for nome in nomes})
        self.assertIsInstance(completo(), TimeSeriesExportAdapter)

        for faltando in nomes:
            restantes = {nome: (lambda self, raw_records: None)
                         for nome in nomes if nome != faltando}
            incompleto = type("Incompleto", (), restantes)
            with self.subTest(sem=faltando):
                self.assertNotIsInstance(incompleto(), TimeSeriesExportAdapter)


class TestValidationError(unittest.TestCase):

    def test_continua_sendo_um_valueerror(self):
        """
        No container vem de core.contracts.errors. O substituto local tem de ter a
        mesma semântica, senão um `except ValueError` do pipeline deixaria de pegar.
        """
        self.assertTrue(issubclass(ValidationError, ValueError))


if __name__ == "__main__":
    unittest.main()
