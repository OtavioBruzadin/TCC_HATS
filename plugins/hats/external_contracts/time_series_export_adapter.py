# Cópia do contrato distribuído pelo MackSun junto com o guia de implementação.
# Duas diferenças em relação ao original, e nenhuma outra:
#
#   - o ValidationError vem de external_contracts.errors, que tenta o
#     core.contracts.errors primeiro e cai num substituto local quando esse
#     pacote não existe — senão nada aqui seria importável fora do container;
#   - as quebras de linha foram normalizadas de CRLF para LF.
#
# Neste arquivo há uma terceira: os três imports que apontavam para
# core.external_contracts.time_series_ingestion.* foram trazidos para o caminho
# plano do plugin, porque no original não resolvem a partir do diretório dele.
# Dois deles o Protocol não usa, e foram mantidos como estão no original.
#
# Fora disso é o arquivo deles, linha por linha. Ver plugins/README.md.
from __future__ import annotations

from typing import Protocol, List, Dict, Any, runtime_checkable

from external_contracts.instrument_ingestion_runtime_spec import InstrumentIngestionRuntimeSpec
from external_contracts.time_series_load_data_result import TimeSeriesLoadDataResult
from external_contracts.time_series_specialist_generated_file import \
    TimeSeriesSpecialistGeneratedFile

@runtime_checkable
class TimeSeriesExportAdapter(Protocol):
    """
    CONTRATO DURO PARA EXPORTAÇÃO DE SÉRIES TEMPORAIS

    Regras inegociáveis:

    1. O pipeline SEMPRE passa apenas dados RAW.
    2. O plugin NÃO recebe nome de arquivo nem diretório de saída.
    3. O plugin decide como e onde escrever o arquivo temporário.
    4. O nome retornado pelo plugin NÃO tem valor semântico.
       O pipeline SEMPRE renomeia o arquivo antes da publicação.
    5. Métodos *Derived* são responsáveis por derivar internamente
       a partir dos dados RAW.
    6. O plugin NÃO publica, NÃO zipa, NÃO acessa banco
       e NÃO conhece regras do pipeline.
    """


    def GenerateFitsFileRawFormat(
        self,
        raw_records: List[Dict[str, Any]],
    ) -> TimeSeriesSpecialistGeneratedFile: ...

    def GenerateFitsFileDerivedFormat100ms(
        self,
        raw_records: List[Dict[str, Any]],
    ) -> TimeSeriesSpecialistGeneratedFile: ...

    def GenerateFitsFileDerivedFormat1s(
        self,
        raw_records: List[Dict[str, Any]],
    ) -> TimeSeriesSpecialistGeneratedFile: ...

    def GenerateCSVFileRawFormat(
        self,
        raw_records: List[Dict[str, Any]],
    ) -> TimeSeriesSpecialistGeneratedFile: ...

    def GenerateCSVFileDerivedFormat100ms(
        self,
        raw_records: List[Dict[str, Any]],
    ) -> TimeSeriesSpecialistGeneratedFile: ...

    def GenerateCSVFileDerivedFormat1s(
        self,
        raw_records: List[Dict[str, Any]],
    ) -> TimeSeriesSpecialistGeneratedFile: ...
