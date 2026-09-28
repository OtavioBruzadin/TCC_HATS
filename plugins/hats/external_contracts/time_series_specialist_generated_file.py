# Cópia do contrato distribuído pelo MackSun junto com o guia de implementação.
# Duas diferenças em relação ao original, e nenhuma outra:
#
#   - o ValidationError vem de external_contracts.errors, que tenta o
#     core.contracts.errors primeiro e cai num substituto local quando esse
#     pacote não existe — senão nada aqui seria importável fora do container;
#   - as quebras de linha foram normalizadas de CRLF para LF.
#
# Fora disso é o arquivo deles, linha por linha. Ver plugins/README.md.
# path: core/external_contracts/time_series_ingestion/time_series_specialist_generated_file.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from external_contracts.errors import ValidationError


@dataclass(frozen=True)
class TimeSeriesSpecialistGeneratedFile:
    instrument_name: str
    target_date: datetime
    level: str
    scientific_product_key: str
    product_resolution: str
    generated_file_format: str
    generated_file_full_path: str

    def __post_init__(self) -> None:
        inst = str(self.instrument_name or "").strip().upper()
        if not inst:
            raise ValidationError("instrument_name obrigatorio")

        if not isinstance(self.target_date, datetime):
            raise ValidationError("target_date obrigatorio")

        lvl = str(self.level or "").strip().lower()
        if lvl not in {"raw", "derived"}:
            raise ValidationError("level deve ser raw ou derived")

        spk = str(self.scientific_product_key or "").strip()
        if not spk:
            raise ValidationError("scientific_product_key obrigatorio")

        res = str(self.product_resolution or "").strip()
        if not res:
            raise ValidationError("product_resolution obrigatorio")

        fmt = str(self.generated_file_format or "").strip().upper()
        if not fmt:
            raise ValidationError("generated_file_format obrigatorio")

        p = str(self.generated_file_full_path or "").strip()
        if not p:
            raise ValidationError("generated_file_full_path obrigatorio")
