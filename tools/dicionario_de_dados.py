#!/usr/bin/env python3
"""
Dicionário de dados dos arquivos que o plugin do HATS gera.

Segue a estrutura da wiki do MackSun (POEMAS e SST):
https://www.macksun.org/pages/wiki/arquivos-telescopios.html

Cada produto tem, para FITS e para CSV, uma tabela de cabeçalho e uma de campos de
observação, mais um exemplo. O exemplo e as tabelas são **gerados a partir dos
arquivos que o plugin produz de verdade**, sobre dado real — não escritos à mão. Se
uma coluna mudar no código, este documento muda junto.

    tools/dicionario_de_dados.py --data-dir "$HATS_DATA_InputPath"
    tools/dicionario_de_dados.py --data-dir Amostra > dicionario.md
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
for entrada in (str(REPO_ROOT), str(PLUGIN_DIR)):
    if entrada not in sys.path:
        sys.path.insert(0, entrada)

import hats_export                                                           # noqa: E402
import hats_ingestion                                                        # noqa: E402
import hats_utils as utils                                                   # noqa: E402
from external_contracts.instrument_ingestion_runtime_spec import (            # noqa: E402
    InstrumentIngestionRuntimeSpec)

from hats import constants, discovery                                        # noqa: E402

# Nomes de tipo como a wiki do MackSun os escreve.
TIPO_NA_WIKI = {"time": "String", "int": "Integer", "float": "Float"}

CABECALHO_DESCRICAO = {
    "FILENAME": ("String", "Name of the generated file"),
    "ORIGIN": ("String", "Institution responsible for data generation"),
    "TELESCOP": ("String", "Name of the telescope used (HATS)"),
    "INSTRUME": ("String", "Name of the instrument used (HATS)"),
    "OBSERVAT": ("String", "Name of the observatory where the observation took place"),
    "STATION": ("String", "Geographic coordinates and altitude of the station"),
    "TZ": ("String", "Time zone reference. **UTC**, not GMT-3 as in POEMAS and SST — "
                     "the HATS timestamp counter is defined from 0 UT"),
    "DATE-OBS": ("Date", "Date of the observation (YYYY-MM-DD)"),
    "T_START": ("DateTime", "Start time of the observation in ISO 8601 format"),
    "T_END": ("DateTime", "End time of the observation in ISO 8601 format"),
    "N_RECORD": ("Integer", "Number of data records in the file"),
    "FREQUEN": ("String", "Observed frequency (15 THz / 20 um)"),
    "FILERES": ("String", "Temporal resolution of this file"),
    "LEVEL": ("String", "Data level of this file: raw or derived"),
}

PRODUTOS = (hats_export.PRODUTO_RAW, hats_export.PRODUTO_100MS, hats_export.PRODUTO_1S)


def um_spec():
    return InstrumentIngestionRuntimeSpec(
        instrument_name="HATS", time_field="UTC_TIME",
        native_resolutions_by_mode={"DEFAULT": "1ms"},
        scan_ignore_dirs=[], include_prefixes=["hats-"], include_suffixes=[],
        include_extensions=[".rbd"], exclude_extensions=[], exclude_suffixes=[],
        processed_dir_name="00_processed", ignored_dir_name="00_ignored",
        invalid_format_dir_name="99_formato_invalid")


def registros_reais(data_dir, day, hour, limite):
    indice = discovery.build_day_index(Path(data_dir))
    if not indice:
        raise SystemExit("nenhum arquivo do HATS em {}".format(data_dir))
    dia = day or sorted(indice)[0]
    horas = indice[dia]["hours"]
    escolhida = hour or next((h for h in sorted(horas) if h != "daily"), None)
    rbd = horas.get(escolhida, {}).get("rbd")
    if not rbd:
        raise SystemExit("nenhum .rbd para {} hora {}".format(dia, escolhida))

    xml_dir = os.environ.get("HATSXMLPATH") or str(REPO_ROOT / "XMLTables")
    ingestion = hats_ingestion.HatsIngestion(xml_dir=xml_dir, record_limit=limite)
    return rbd, ingestion.load_data(um_spec(), str(rbd)).data_records


def exemplo_do_csv(export, registros, produto, linhas=3):
    """As primeiras linhas de um CSV realmente gerado, cabeçalho e dados."""
    metodo = {
        utils.PRODUTO_RAW: export.GenerateCSVFileRawFormat,
        utils.PRODUTO_100MS: export.GenerateCSVFileDerivedFormat100ms,
        utils.PRODUTO_1S: export.GenerateCSVFileDerivedFormat1s,
    }[produto.chave]
    gerado = metodo(registros)
    caminho = Path(gerado.generated_file_full_path)
    conteudo = caminho.read_text(encoding="utf-8").splitlines()
    caminho.unlink()

    inicio_cabecalho = conteudo.index("#HEADER|")
    inicio_itens = conteudo.index("#ITEMS|")
    bloco_cabecalho = [l for l in conteudo[inicio_cabecalho + 1:inicio_itens] if l.strip("|")]
    bloco_itens = conteudo[inicio_itens + 1:inicio_itens + 1 + linhas + 1]
    return bloco_cabecalho, bloco_itens


def tabela(linhas_de_cabecalho, linhas):
    largura = len(linhas_de_cabecalho)
    saida = ["| " + " | ".join(linhas_de_cabecalho) + " |",
             "|" + "|".join([" --- "] * largura) + "|"]
    saida += ["| " + " | ".join(str(c) for c in linha) + " |" for linha in linhas]
    return saida


def documento(rbd, registros, export):
    linhas = []
    add = linhas.append

    add("# HATS — Solar Data Dictionary")
    add("")
    add("High Altitude THz Solar photometer · 15 THz (20 µm) · OAFA, San Juan, Argentina")
    add("")
    add("Este documento descreve a estrutura dos arquivos que o plugin do HATS gera. "
        "Segue a organização do dicionário de dados do MackSun para POEMAS e SST.")
    add("")
    add("**Gerado a partir dos arquivos que o plugin produz**, sobre dado real "
        "(`{}`). As tabelas e os exemplos não são escritos à mão: se uma coluna mudar "
        "no código, este documento muda junto.".format(rbd.name))
    add("")

    # ---------------------------------------------------------------- visão geral
    add("## Visão geral")
    add("")
    add("| Produto | Nível | Resolução | Formatos | Como é gerado |")
    add("| --- | --- | --- | --- | --- |")
    add("| `{}` | raw | 1 ms | FITS, CSV | o sinal do detector, sem transformação |".format(
        utils.PRODUTO_RAW))
    add("| `{}` | derived | 100 ms | FITS, CSV | média de {} amostras consecutivas |".format(
        utils.PRODUTO_100MS, hats_export.JANELA_100MS))
    add("| `{}` | derived | 1 s | FITS, CSV | média de {} amostras consecutivas |".format(
        utils.PRODUTO_1S, hats_export.JANELA_1S))
    add("")
    add("Para pesquisa científica o MackSun recomenda o FITS, e vale para o HATS: "
        "o CSV carrega os mesmos valores, mas como texto.")
    add("")

    # ------------------------------------------------------ calibração aplicada
    add("## Calibração aplicada na ingestão")
    add("")
    add("> A documentação do MackSun diz que o pipeline não aplica calibração, "
        "compensação ou correção adicional **salvo quando explicitamente declarado**. "
        "Para o HATS, é o caso — e está declarado aqui.")
    add("")
    add("Os arquivos `.rbd` trazem os canais como contagens de um conversor A/D "
        "AD7770, com 24 bits significativos dentro de uma palavra de 4 bytes e o sinal "
        "no bit 23. A ingestão faz duas coisas, ambas descritas nos XML de formato do "
        "CRAAM e nenhuma delas arbitrária:")
    add("")
    add("1. **extensão de sinal** dos 24 bits para inteiro com sinal;")
    add("2. **reta de calibração** declarada no XML, levando a contagem à unidade "
        "física — mV nos canais de sinal, °C nos de temperatura.")
    add("")
    add("Nenhuma outra correção é aplicada. Não há compensação atmosférica, subtração "
        "de linha de base nem calibração absoluta de fluxo.")
    add("")

    # ------------------------------------------------------- produtos derivados
    add("## Produtos derivados")
    add("")
    add("Os produtos derivados agrupam amostras consecutivas em janelas fixas e sem "
        "sobreposição, e reduzem cada janela a um valor representativo.")
    add("")
    add("| Produto | Janela | Redução | Carimbo de tempo |")
    add("| --- | --- | --- | --- |")
    for produto in PRODUTOS[1:]:
        add("| `{}` | {} amostras | **média aritmética** | amostra de índice {} "
            "da janela |".format(produto.chave, produto.janela, produto.janela // 2))
    add("")
    add("**Duas diferenças em relação ao POEMAS, ambas deliberadas.**")
    add("")
    add("A redução é **média aritmética**, não mediana. O POEMAS usa mediana; para o "
        "HATS a média foi definida pelo suporte do MackSun.")
    add("")
    add("As janelas têm {} e {} amostras, e não 10 e 100. Os números do POEMAS "
        "pressupõem amostragem de 10 ms; o HATS amostra a {:.0f} Hz, então chegar a "
        "100 ms e a 1 s exige janelas dez vezes maiores.".format(
            hats_export.JANELA_100MS, hats_export.JANELA_1S,
            constants.SAMPLING_FREQUENCY))
    add("")
    add("> **Atenção ao usar os produtos derivados.** O detector do HATS é uma célula "
        "Golay atrás de um *chopper* de 20 Hz, então o sinal bruto é uma portadora "
        "modulada. Cada janela derivada cobre um número inteiro de ciclos do chopper, "
        "de modo que a média estima o nível médio da modulação e **não o fluxo solar**. "
        "Medido em dado real: a média sobre 100 ms dá cerca de 2 mV enquanto a "
        "amplitude da modulação, que é a grandeza física, dá cerca de 124 mV.")
    add("")

    # ------------------------------------------------------------------- FITS
    add("## HATS FITS")
    add("")
    add("Format: FITS File | Encoding: Binary | Structure: Header + Observation Data Fields")
    add("")
    add("### Header")
    add("")
    cabecalho_exemplo = export._cabecalho("exemplo.fits", registros, PRODUTOS[0])
    linhas += tabela(["Field name", "Data type", "Description"],
                     [(chave,) + CABECALHO_DESCRICAO[chave] for chave, _ in cabecalho_exemplo])
    add("")
    add("`FILERES` e `LEVEL` não constam do dicionário do POEMAS, mas o plugin do "
        "POEMAS os grava. Para o HATS eles são necessários, porque os três produtos "
        "compartilham as mesmas colunas e só esses dois campos os distinguem.")
    add("")
    add("### Observation Data Fields")
    add("")
    linhas += tabela(
        ["Field name", "Data type", "FITS format", "Description"],
        [(coluna.nome, TIPO_NA_WIKI[coluna.tipo], coluna.fits, coluna.descricao)
         for coluna in hats_export.COLUNAS_RAW])
    add("")
    add("As mesmas colunas valem para os três produtos. O que muda entre eles é a "
        "quantidade de registros e os campos `FILERES` e `LEVEL` do cabeçalho.")
    add("")

    # -------------------------------------------------------------------- CSV
    add("## HATS CSV")
    add("")
    add("Delimiter: Pipe (|) | Encoding: UTF-8 | Structure: Documentation + Header + "
        "Observation Data Fields")
    add("")
    add("O CSV traz um bloco `#DOCUMENTATION` antes do `#HEADER`, com a nota de "
        "copyright e a descrição de cada campo, seguido do bloco `#ITEMS` com os dados.")
    add("")
    add("### Header")
    add("")
    linhas += tabela(["Field name", "Data type", "Description"],
                     [(chave,) + CABECALHO_DESCRICAO[chave] for chave, _ in cabecalho_exemplo])
    add("")
    add("### Observation Data Fields")
    add("")
    linhas += tabela(
        ["Position", "Column Name", "Data type", "Description"],
        [(posicao, coluna.nome, TIPO_NA_WIKI[coluna.tipo], coluna.descricao)
         for posicao, coluna in enumerate(hats_export.COLUNAS_RAW, start=1)])
    add("")

    # ---------------------------------------------------------------- exemplos
    add("### Exemplo")
    add("")
    add("Primeiras linhas de um arquivo real do produto `{}`:".format(utils.PRODUTO_RAW))
    add("")
    bloco_cabecalho, bloco_itens = exemplo_do_csv(export, registros, PRODUTOS[0])
    add("```")
    linhas.extend(bloco_cabecalho)
    add("")
    linhas.extend(bloco_itens)
    add("```")
    add("")
    add("E do produto `{}`, sobre os mesmos dados:".format(utils.PRODUTO_100MS))
    add("")
    _cab, itens_derivado = exemplo_do_csv(export, registros, PRODUTOS[1])
    add("```")
    linhas.extend(itens_derivado)
    add("```")
    add("")
    add("Compare a coluna `GOLAY` nos dois blocos. No bruto ela oscila em dezenas de "
        "mV; no derivado fica perto de 2 mV. Não é suavização: é o cancelamento da "
        "portadora ao se tirar a média de um número inteiro de ciclos do chopper. É o "
        "aviso da seção anterior, visível nos próprios exemplos.")
    add("")

    # ------------------------------------------------------------ observações
    add("## Observações sobre o carimbo de tempo")
    add("")
    add("`ISO_DATETIME` sai com **microssegundos**, e não com milissegundos como no "
        "POEMAS e no SST. O contador nativo do HATS é o *husec* — centésimos de "
        "milissegundo desde 0 UT —, com resolução de 0,1 ms, que três casas decimais "
        "não representam.")
    add("")
    add("A coluna `HUSEC` traz esse contador como inteiro. Como a persistência guarda "
        "o `UTC_TIME` com precisão de milissegundo, **é o `HUSEC` que preserva a "
        "resolução de 0,1 ms** depois da ida e volta pelo banco.")
    add("")
    add("Os carimbos estão em **UTC**. O POEMAS e o SST publicam em GMT-3; o husec do "
        "HATS é definido desde 0 UT, e converter introduziria um fuso onde o "
        "instrumento não tem nenhum.")
    add("")
    return "\n".join(linhas)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="dicionario_de_dados",
        description="Gera o dicionário de dados dos arquivos do HATS, em Markdown.")
    parser.add_argument("--data-dir", default=os.environ.get("HATS_DATA_InputPath", "Amostra"))
    parser.add_argument("--day", default=None)
    parser.add_argument("--hour", default=None)
    parser.add_argument("--record-limit", type=int, default=50000)
    parser.add_argument("--output", default=None, help="Arquivo de saída. Padrão: stdout.")
    args = parser.parse_args(argv)

    rbd, registros = registros_reais(args.data_dir, args.day, args.hour, args.record_limit)

    # os arquivos de exemplo são temporários e apagados depois de lidos
    with tempfile.TemporaryDirectory() as temporario:
        anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = temporario
        try:
            texto = documento(rbd, registros, hats_export.HatsExport())
        finally:
            if anterior is None:
                os.environ.pop("CRAAM_SPECIALIST_TMP_ROOT", None)
            else:
                os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = anterior

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(texto, encoding="utf-8")
        print("gravado em {} ({} bytes)".format(args.output, len(texto.encode("utf-8"))))
    else:
        print(texto)
    return 0


if __name__ == "__main__":
    sys.exit(main())
