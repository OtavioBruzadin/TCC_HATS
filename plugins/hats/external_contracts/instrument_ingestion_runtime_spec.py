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

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True, slots=True)
class InstrumentIngestionRuntimeSpec:
    instrument_name: str
    time_field: str
    native_resolutions_by_mode: Dict[str, str]

    scan_ignore_dirs: List[str]
    include_prefixes: List[str]
    include_suffixes: List[str]
    include_extensions: List[str]
    exclude_extensions: List[str]
    exclude_suffixes: List[str]

    processed_dir_name: str
    ignored_dir_name: str
    invalid_format_dir_name: str
