"""
Metadados do instrumento, formatação e diretório temporário.

Equivalente do `poemas_utils.py` na estrutura que o §12 recomenda.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

INSTRUMENT_NAME = "HATS"

# Metadados fixos. As coordenadas vêm do OAFA.py do CRAAM, o mesmo módulo que o
# HATS.py usa para calcular efemérides.
ORIGIN = "CRAAM/Universidade Presbiteriana Mackenzie"
TELESCOPE = "High Altitude THz Solar photometer (HATS)"
OBSERVATORY = "OAFA"
STATION = "Lat = -31.80230000, Lon = -69.32650000, Height = 2.420 km"
FREQUENCY = "15 THz (20 um)"

# ---------------------------------------------------------------------------
# PENDENTE DE DEFINIÇÃO DO SUPORTE
#
# `scientific_product_key` e `product_resolution` são campos obrigatórios do
# retorno da exportação (§10), mas os valores são DECLARAÇÃO DO CATÁLOGO — o
# slide da arquitetura diz isso com todas as letras: a entrada do catálogo declara
# "particionamento, produtos e formatos".
#
# Os valores abaixo são provisórios, escolhidos por analogia com o POEMAS, que usa
# `raw_10ms` para o produto bruto a 10 ms. Quando o documento do HATS existir,
# trocar aqui — e só aqui.
# ---------------------------------------------------------------------------
PRODUTO_RAW = "raw_1ms"
RESOLUCAO_RAW = "1ms"
PRODUTO_100MS = "derived_100ms"
RESOLUCAO_100MS = "100ms"
PRODUTO_1S = "derived_1s"
RESOLUCAO_1S = "1s"
PRODUTO_DECONV = "deconv_32ms"
RESOLUCAO_DECONV = "32ms"

# As duas estruturas que o level 0 do Guigue acrescenta e que precisam de chave
# própria: o `rData` porque o CSV é plano e cada estrutura é um arquivo, e o `aux`
# porque é outra fonte RAW, com outra cadência. Mesma pendência das de cima — os
# nomes são declaração do catálogo.
#
# A resolução do apontamento é a mesma string que a ingestão usa como resolução
# nativa do modo POINTING, mas as duas dizem coisas diferentes: lá é o que o
# instrumento faz, aqui é o que o catálogo declara. Se um dia divergirem, é porque
# o catálogo decidiu outra coisa.
PRODUTO_CONTAGENS = "raw_counts_1ms"
PRODUTO_APONTAMENTO = "raw_pointing_1050ms"
RESOLUCAO_APONTAMENTO = "1050ms"

# O guia §6 exige este nome exato no registro.
TIME_FIELD = "UTC_TIME"

# Seis casas decimais, não quatro como no POEMAS. O canal de temperatura tem passo
# de 2,99e-5 °C por contagem do ADC: com quatro casas o passo de quantização cai
# abaixo da última casa gravada e vira degrau de arredondamento. Medido no dado
# real: TEMP_GOLAY sai 50.690450199999994.
DECIMAL_PLACES = 6

# O carimbo nativo do HATS é o husec, com resolução de 0,1 ms. Três casas decimais,
# como o POEMAS usa, perderiam o último dígito do relógio do instrumento; seis
# representam o husec exatamente, já que 1 husec = 100 µs.
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def format_float(value: Any, places: int = DECIMAL_PLACES) -> str:
    return "{:.{}f}".format(float(value), places)


def as_utc(value: Any) -> datetime:
    """Aceita datetime ingênuo (tratado como UTC) ou com fuso."""
    if not isinstance(value, datetime):
        raise ValueError("{} deve ser datetime, veio {}".format(
            TIME_FIELD, type(value).__name__))
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def iso_micro(value: datetime) -> str:
    """ISO 8601 com microssegundos — 26 caracteres."""
    return value.strftime(ISO_FORMAT)


def iso_second(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def get_specialist_tmp_dir(instrument: str = INSTRUMENT_NAME) -> str:
    """
    Diretório temporário onde a exportação escreve (§9).

    Mesma convenção do POEMAS, inclusive a variável de ambiente. O arquivo que sai
    daqui é temporário de propósito: o §11 diz que renomear, mover e publicar é
    responsabilidade exclusiva do pipeline.
    """
    root = os.getenv("CRAAM_SPECIALIST_TMP_ROOT") or tempfile.gettempdir()
    path = Path(root) / "craam_specialist" / str(instrument).lower()
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def safe_name_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return "".join(
        caractere if (caractere.isascii() and (caractere.isalnum() or caractere in "_."))
        else "_"
        for caractere in text)


def build_output_filename(first_time: datetime, product_key: str, fmt: str,
                          extension: str) -> str:
    """
    Nome do arquivo temporário.

    Não tem valor semântico — o contrato diz explicitamente que o pipeline renomeia
    antes de publicar. Só precisa ser único, e o uuid garante isso mesmo com dois
    produtos do mesmo dia gerados no mesmo diretório.
    """
    return "{}_{}_{}_{}_{}.{}".format(
        INSTRUMENT_NAME,
        safe_name_token(product_key),
        as_utc(first_time).strftime("%Y%m%d"),
        safe_name_token(fmt.upper()),
        uuid.uuid4().hex[:12],
        extension)


def extract_target_date(records: List[Dict[str, Any]]) -> datetime:
    """Meia-noite UTC do dia do primeiro registro, para o `target_date` do §10."""
    if not records:
        raise ValueError("records vazio")
    primeiro = as_utc(records[0].get(TIME_FIELD))
    return datetime(primeiro.year, primeiro.month, primeiro.day, tzinfo=timezone.utc)
