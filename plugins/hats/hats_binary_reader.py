"""
Leitura e decodificação dos binários do HATS.

Equivalente do `poemas_binary_reader.py`, e o mesmo papel no fluxo que o guia
recomenda (§12): `arquivo → reader → ingestion → registros → export`. Aqui termina
a responsabilidade sobre o *formato*; quem monta registro é a ingestão.

Nada é recalculado neste módulo. O layout dos registros, a decodificação do
AD7770 e as retas de calibração vêm do pacote `hats`, que reproduz o `HATS.py` do
CRAAM bit a bit. O reader resolve onde estão os XML de formato, escolhe o esquema,
confere que o arquivo fecha e entrega os canais já em unidade física.

Duas decisões que valem explicação
----------------------------------
**Sem fallback de esquema.** O `hats.schema.load()` cai num esquema fixo interno
quando não acha os XML, o que é razoável para uma ferramenta de linha de comando e
é errado aqui: em produção, decodificar 46 milhões de documentos por dia com um
layout adivinhado é pior do que não rodar. O reader confere `schema["mode"]` e
falha se o fallback tiver sido usado.

**O canal do detector tem dois nomes.** No formato de 38 bytes (a partir de
2021-10-25) ele se chama `golay`; no de 26 bytes, `adcu`. Procurar só por `golay`
faz um arquivo antigo ser lido inteiro e demodulado como se não tivesse detector —
sem erro e sem saída. O reader resolve o nome, e falha se não achar nenhum dos
dois.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from hats import calibration, records, schema as schema_module, timebase

KIND_BY_SUFFIX = {".rbd": "rbd", ".aux": "aux"}

# Ordem de preferência: o formato vigente primeiro.
DETECTOR_CHANNEL_NAMES = ("golay", "adcu")

# Sufixo das colunas que trazem a contagem do ADC com o sinal estendido, antes da
# reta de calibração. O HATS.py mantém as duas versões em estruturas separadas —
# rData com a contagem e cData com o valor físico — e o Guigue pediu as duas no
# FITS de level 0, em camadas próprias.
SUFIXO_CRU = "_adcu"


def resolve_xml_directory(xml_dir: Optional[str] = None) -> Path:
    """
    Onde estão os XML de formato: argumento, senão HATSXMLPATH.

    HATSXMLPATH é a variável que o manual do CRAAM manda exportar na instalação.
    Não há terceiro caminho: sem os XML o reader não decodifica.
    """
    escolhido = xml_dir or os.environ.get("HATSXMLPATH")
    if not escolhido:
        raise ValueError(
            "nao sei onde estao os XML de formato do HATS. Passe xml_dir ou exporte "
            "HATSXMLPATH, como o manual do CRAAM indica")

    caminho = Path(escolhido).expanduser()
    if not caminho.is_dir():
        raise ValueError("o diretorio de XML {} nao existe".format(caminho))
    return caminho


def load_schema(kind: str, xml_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Esquema de 'rbd' ou 'aux', lido do XML — nunca do fallback interno.

    O `hats.schema.load()` marca em `mode` de onde veio o esquema. Qualquer valor
    diferente de 'xml' significa que ele não achou os arquivos e adivinhou, e aqui
    isso é erro, não recurso.
    """
    if kind not in ("rbd", "aux"):
        raise ValueError("tipo de esquema desconhecido: {}".format(kind))

    diretorio = resolve_xml_directory(xml_dir)
    esquema = schema_module.load(diretorio, kind)

    if esquema.get("mode") != "xml":
        esperados = ("HATSDataFormat.xml, HATSDataFormat-120.xml ou HATSDataFormat-110.xml"
                     if kind == "rbd" else "HATSAuxFormat.xml")
        raise ValueError(
            "nenhum XML de formato '{}' foi encontrado em {}; esperado {}. O esquema "
            "fixo interno do pacote hats nao e usado aqui: decodificar com layout "
            "adivinhado e pior do que nao rodar".format(kind, diretorio, esperados))

    return esquema


def kind_for(path: Path) -> str:
    """Tipo de esquema a partir da extensão do arquivo."""
    kind = KIND_BY_SUFFIX.get(path.suffix.lower())
    if kind is None:
        raise ValueError(
            "extensao nao reconhecida em {}: o reader le .rbd e .aux".format(path.name))
    return kind


def detector_channel(schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    O campo do canal do detector, sob qualquer um dos nomes que o formato já usou.
    """
    por_nome = {campo["name"]: campo for campo in schema["fields"]}
    for nome in DETECTOR_CHANNEL_NAMES:
        if nome in por_nome:
            return por_nome[nome]
    raise ValueError(
        "nenhum canal de detector em {}: esperado um de {}. Os campos declarados "
        "sao {}".format(schema["name"], ", ".join(DETECTOR_CHANNEL_NAMES),
                        ", ".join(sorted(por_nome))))


def read_header(path: Path, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Metadados do arquivo, sem percorrer o conteúdo.

    A contagem de registros já valida o arquivo: `records.total_records` falha se o
    tamanho não for múltiplo do registro, e é esse erro que roteia um arquivo
    truncado para a pasta de formato inválido.
    """
    kind = kind_for(path)
    total = records.total_records(path, schema)

    cabecalho: Dict[str, Any] = {
        "FILENAME": path.name,
        "FULL_FILE_PATH": str(path),
        "KIND": kind,
        "DATE": timebase.date_from_filename(path),
        "HOUR": timebase.hour_from_filename(path),
        "NREC": total,
        "RECORD_SIZE": schema["record_size"],
        "SCHEMA": {
            "name": schema["name"],
            "source": schema["source"],
            "mode": schema["mode"],
        },
        "FIELDS": [campo["name"] for campo in schema["fields"]],
    }

    if kind == "rbd":
        cabecalho["DETECTOR_CHANNEL"] = detector_channel(schema)["name"]

    return cabecalho


def _calibrator(schema: Dict[str, Any]):
    """
    Para cada campo, como levar o valor cru ao valor de saída.

    Devolve uma lista de (nome, posição, decodifica_adc, ganho, offset). Resolver
    isso uma vez, fora do laço, importa: são cinco canais vezes 3,6 milhões de
    amostras por hora de observação.
    """
    posicao_de = schema_module.field_index(schema)
    plano = []
    for campo in schema["fields"]:
        nome = campo["name"]
        if nome not in posicao_de:
            continue
        plano.append((
            nome,
            posicao_de[nome],
            campo.get("origin") == "ad7770",
            float(campo.get("slope", 1.0)) if campo.get("convert") == "yes" else None,
            float(campo.get("offset", 0.0)) if campo.get("convert") == "yes" else None,
        ))
    return plano


def iter_calibrated_blocks(path: Path, schema: Dict[str, Any],
                           record_limit: Optional[int] = None) -> Iterator[Tuple[Dict[str, List], int]]:
    """
    Percorre o arquivo em blocos, entregando as colunas em duas versões.

    Produz tuplas ({nome_do_campo: lista_de_valores}, quantidade). Cada canal do
    AD7770 aparece duas vezes: `golay` com o valor físico e `golay_adcu` com a
    contagem de sinal estendido que o originou. São as estruturas cData e rData do
    HATS.py, e o level 0 precisa das duas.

    O POEMAS lê o arquivo inteiro de uma vez com `np.fromfile` e devolve o array.
    Aqui não: uma hora de HATS são 137 MB de binário antes de virar registro, e
    materializar isso só para depois montar 3,6 milhões de dicionários dobraria o
    pico de memória sem necessidade. O bloco é a unidade natural do
    `hats.records`, e é o que sai daqui.
    """
    plano = _calibrator(schema)

    for colunas, quantidade in records.iter_columns(path, schema, record_limit):
        bloco: Dict[str, List] = {}
        for nome, posicao, do_adc, ganho, offset in plano:
            coluna = colunas[posicao]
            if do_adc:
                coluna = calibration.decode_column(coluna)
                # A contagem com sinal estendido, antes da reta. É o conteúdo do
                # rData do HATS.py, que o level 0 precisa ter ao lado do cData.
                bloco[nome + SUFIXO_CRU] = coluna
            if ganho is not None:
                coluna = [valor * ganho + offset for valor in coluna]
            # Lista sempre, inclusive nos campos que não passam por calibração. O
            # `iter_columns` entrega tuplas, e um dicionário em que `husec` é tupla
            # e `golay` é lista obriga quem consome a saber de qual campo se trata.
            bloco[nome] = coluna if isinstance(coluna, list) else list(coluna)
        yield bloco, quantidade


def read_and_decode_hats_file(file_full_path: str, xml_dir: Optional[str] = None,
                              record_limit: Optional[int] = None):
    """
    Ponto de entrada do reader, equivalente ao `read_and_decode_poemas_file`.

    Devolve (cabeçalho, esquema, blocos). O esquema acompanha porque a ingestão
    precisa dele para saber quais campos têm correção de unidade declarada.

    Os blocos são um gerador: nada é lido do disco até a ingestão consumir.
    """
    path = Path(file_full_path)
    if not path.exists():
        raise ValueError("arquivo nao encontrado: {}".format(path))

    schema = load_schema(kind_for(path), xml_dir)
    cabecalho = read_header(path, schema)
    return cabecalho, schema, iter_calibrated_blocks(path, schema, record_limit)
