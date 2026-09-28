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

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List

from external_contracts.errors import ValidationError


@dataclass(frozen=True, slots=True)
class TimeSeriesLoadDataResult:
    data_records: List[Dict[str, Any]]
    data_time_resolution: str
    data_level: str
    observation_day: datetime

    instrument_name: str = ""
    acquisition_mode: str = ""
    source_file_path: str = ""
    processing_warnings: List[str] = field(default_factory=list)
    instrument_metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.data_records, list):
            raise ValidationError("TimeSeriesLoadDataResult invalido: data_records nao e lista")

        if not self.data_records:
            raise ValidationError("TimeSeriesLoadDataResult invalido: data_records vazio")

        if not str(self.data_time_resolution or "").strip():
            raise ValidationError("TimeSeriesLoadDataResult invalido: data_time_resolution ausente")

        lv = str(self.data_level or "").strip().lower()
        if lv not in {"raw", "derived"}:
            raise ValidationError("TimeSeriesLoadDataResult invalido: data_level deve ser raw ou derived")

        if self.observation_day is None:
            raise ValidationError("TimeSeriesLoadDataResult invalido: observation_day ausente")

        if not isinstance(self.observation_day, datetime):
            raise ValidationError("TimeSeriesLoadDataResult invalido: observation_day deve ser datetime")

