# path: /app/plugins/hats/instrument.py
"""
Adapter do HATS para o pipeline do MackSun.

Este é o arquivo que o `plugin.source_file` do catálogo aponta, e `build_instrument`
é o `plugin.entrypoint`. O §13.2 é explícito: o pipeline **não usa import
tradicional** — carrega este arquivo pelo caminho e executa o símbolo.

A classe só delega. A leitura do formato está em hats_binary_reader.py, a conversão
em registros em hats_ingestion.py e a geração de arquivos em hats_export.py, que é
a estrutura que o §12 recomenda.
"""

from __future__ import annotations

import sys
from pathlib import Path

# O core carrega este arquivo pelo caminho, então o diretório do plugin não entra
# em sys.path sozinho e os imports planos abaixo não resolveriam.
PLUGIN_ROOT = Path(__file__).resolve().parent
if str(PLUGIN_ROOT) not in sys.path:
    sys.path.insert(0, str(PLUGIN_ROOT))

from external_contracts.time_series_export_adapter import TimeSeriesExportAdapter
from external_contracts.time_series_ingestion_adapter import TimeSeriesIngestionAdapter
from hats_export import HatsExport
from hats_ingestion import HatsIngestion


class HatsInstrument(TimeSeriesIngestionAdapter, TimeSeriesExportAdapter):
    """Delegação pura. A lógica está nos módulos que esta classe compõe."""

    def __init__(self, xml_dir=None, record_limit=None) -> None:
        self._ingestion = HatsIngestion(xml_dir=xml_dir, record_limit=record_limit)
        self._export = HatsExport()

    # ---------------------------------------------------------------- ingestão

    def load_data(self, spec, file_full_path):
        return self._ingestion.load_data(spec, file_full_path)

    # -------------------------------------------------------------- exportação

    def GenerateFitsFileRawFormat(self, raw_records):
        return self._export.GenerateFitsFileRawFormat(raw_records)

    def GenerateCSVFileRawFormat(self, raw_records):
        return self._export.GenerateCSVFileRawFormat(raw_records)

    def GenerateFitsFileDerivedFormat100ms(self, raw_records):
        return self._export.GenerateFitsFileDerivedFormat100ms(raw_records)

    def GenerateCSVFileDerivedFormat100ms(self, raw_records):
        return self._export.GenerateCSVFileDerivedFormat100ms(raw_records)

    def GenerateFitsFileDerivedFormat1s(self, raw_records):
        return self._export.GenerateFitsFileDerivedFormat1s(raw_records)

    def GenerateCSVFileDerivedFormat1s(self, raw_records):
        return self._export.GenerateCSVFileDerivedFormat1s(raw_records)

    # ------------------------------- um arquivo CSV por estrutura do level 0

    def GenerateCSVFileRawCountsFormat(self, raw_records):
        return self._export.GenerateCSVFileRawCountsFormat(raw_records)

    def GenerateFitsFilePointingFormat(self, raw_records):
        return self._export.GenerateFitsFilePointingFormat(raw_records)

    def GenerateCSVFilePointingFormat(self, raw_records):
        return self._export.GenerateCSVFilePointingFormat(raw_records)

    # ------------------------------------------- produto científico do HATS

    def GenerateFitsFileDeconvFormat32ms(self, raw_records):
        return self._export.GenerateFitsFileDeconvFormat32ms(raw_records)

    def GenerateCSVFileDeconvFormat32ms(self, raw_records):
        return self._export.GenerateCSVFileDeconvFormat32ms(raw_records)


def build_instrument():
    """
    Entrypoint do §12. Callable sem parâmetros, devolve o adapter.

    Em produção o pacote `hats` está instalado no ambiente do job e os XML de
    formato são localizados por HATSXMLPATH — sem ela o reader falha, em vez de
    adivinhar o layout.
    """
    return HatsInstrument()
