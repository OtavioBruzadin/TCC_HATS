# Cópia do contrato distribuído pelo MackSun junto com o guia de implementação.
# Duas diferenças em relação ao original, e nenhuma outra:
#
#   - o ValidationError vem de external_contracts.errors, que tenta o
#     core.contracts.errors primeiro e cai num substituto local quando esse
#     pacote não existe — senão nada aqui seria importável fora do container;
#   - as quebras de linha foram normalizadas de CRLF para LF.
#
# Fora disso é o arquivo deles, linha por linha. Ver plugins/README.md.
from __future__ import annotations

from typing import Protocol, runtime_checkable

from external_contracts.instrument_ingestion_runtime_spec import InstrumentIngestionRuntimeSpec
from external_contracts.time_series_load_data_result import TimeSeriesLoadDataResult


@runtime_checkable
class TimeSeriesIngestionAdapter(Protocol):


    def load_data(
        self,
        spec: InstrumentIngestionRuntimeSpec,
        file_full_path: str,
    ) -> TimeSeriesLoadDataResult: ...


