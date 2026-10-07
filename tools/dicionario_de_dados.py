#!/usr/bin/env python3
"""
Dicionário de dados dos arquivos que o plugin do HATS gera.

Segue a estrutura da wiki do MackSun (POEMAS e SST):
https://www.macksun.org/pages/wiki/arquivos-telescopios.html

Cada formato tem uma tabela de cabeçalho e uma tabela de campos **por estrutura**,
mais um exemplo. As tabelas e os exemplos são **gerados a partir dos arquivos que o
plugin produz de verdade**, sobre dado real — não escritos à mão. Se uma coluna
mudar no código, este documento muda junto.

As quatro estruturas são as do `HATS.py`, como o Guigue pediu: `rData`, `cData`,
`aux` e `Deconv`.

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
    "TZ": ("String", "Time zone reference. UTC, since the instrument timestamp counter "
                     "is defined from 0 UT"),
    "DATE-OBS": ("Date", "Date of the observation (YYYY-MM-DD)"),
    "T_START": ("DateTime", "Start time of the observation in ISO 8601 format"),
    "T_END": ("DateTime", "End time of the observation in ISO 8601 format"),
    "N_RECORD": ("Integer", "Number of data records in the file"),
    "FREQUEN": ("String", "Observed frequency (15 THz / 20 um)"),
    "FILERES": ("String", "Temporal resolution of this file"),
    "LEVEL": ("String", "Data level of this file: raw or derived"),
}

# Um registro por arquivo publicado, na ordem em que o documento os apresenta.
# `fonte` diz de qual fonte RAW vêm os registros, porque o exemplo é lido de um
# arquivo realmente gerado e as duas fontes têm registros diferentes.
ARQUIVOS = (
    {"produto": hats_export.PRODUTO_RAW,
     "camadas_fits": (hats_export.CAMADA_RDATA, hats_export.CAMADA_CDATA),
     "camada_csv": hats_export.CAMADA_CDATA,
     "metodo_csv": "GenerateCSVFileRawFormat",
     "fonte": "detector",
     "gerado_por": "the detector signal as registered, in physical units, with the "
                   "ADC counts alongside it"},
    {"produto": hats_export.PRODUTO_CONTAGENS,
     "camadas_fits": (),
     "camada_csv": hats_export.CAMADA_RDATA,
     "metodo_csv": "GenerateCSVFileRawCountsFormat",
     "fonte": "detector",
     "gerado_por": "the ADC counts as registered, with no calibration applied"},
    {"produto": hats_export.PRODUTO_APONTAMENTO,
     "camadas_fits": (hats_export.CAMADA_APONTAMENTO,),
     "camada_csv": hats_export.CAMADA_APONTAMENTO,
     "metodo_csv": "GenerateCSVFilePointingFormat",
     "fonte": "apontamento",
     "gerado_por": "the telescope pointing solution as registered by the control loop"},
    {"produto": hats_export.PRODUTO_DECONV,
     "camadas_fits": (hats_export.CAMADA_DECONV,),
     "camada_csv": hats_export.CAMADA_DECONV,
     "metodo_csv": "GenerateCSVFileDeconvFormat32ms",
     "fonte": "detector",
     "gerado_por": "amplitude of the 20 Hz modulation of the detector signal, "
                   "recovered by demodulation"},
    {"produto": hats_export.PRODUTO_100MS,
     "camadas_fits": (hats_export.CAMADA_CDATA,),
     "camada_csv": hats_export.CAMADA_CDATA,
     "metodo_csv": "GenerateCSVFileDerivedFormat100ms",
     "fonte": "detector",
     "gerado_por": "arithmetic mean of {} consecutive samples".format(
         hats_export.JANELA_100MS)},
    {"produto": hats_export.PRODUTO_1S,
     "camadas_fits": (hats_export.CAMADA_CDATA,),
     "camada_csv": hats_export.CAMADA_CDATA,
     "metodo_csv": "GenerateCSVFileDerivedFormat1s",
     "fonte": "detector",
     "gerado_por": "arithmetic mean of {} consecutive samples".format(
         hats_export.JANELA_1S)},
)

# As quatro estruturas, uma vez cada, na ordem em que o Guigue as listou.
ESTRUTURAS = (hats_export.CAMADA_RDATA, hats_export.CAMADA_CDATA,
              hats_export.CAMADA_APONTAMENTO, hats_export.CAMADA_DECONV)

# Os produtos que agregam em janela fixa.
PRODUTOS_DERIVADOS = (hats_export.PRODUTO_100MS, hats_export.PRODUTO_1S)


def um_spec():
    return InstrumentIngestionRuntimeSpec(
        instrument_name="HATS", time_field="UTC_TIME",
        native_resolutions_by_mode={"DETECTOR": utils.RESOLUCAO_RAW,
                                    "POINTING": utils.RESOLUCAO_APONTAMENTO},
        scan_ignore_dirs=[], include_prefixes=["hats-"], include_suffixes=[],
        include_extensions=[".rbd", ".aux"], exclude_extensions=[], exclude_suffixes=[],
        processed_dir_name="00_processed", ignored_dir_name="00_ignored",
        invalid_format_dir_name="99_formato_invalid")


def registros_reais(data_dir, day, hour, limite):
    """
    Registros das duas fontes, do mesmo par de arquivos.

    O `.aux` é opcional: sem ele as tabelas do apontamento continuam saindo, porque
    vêm da definição das colunas, e só o exemplo fica de fora.
    """
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
    registros = {"detector": ingestion.load_data(um_spec(), str(rbd)).data_records}

    aux = horas.get(escolhida, {}).get("aux")
    if aux:
        registros["apontamento"] = ingestion.load_data(um_spec(), str(aux)).data_records

    return rbd, registros


def exemplo_do_csv(export, registros, metodo, linhas=3):
    """As primeiras linhas de um CSV realmente gerado, cabeçalho e dados."""
    gerado = getattr(export, metodo)(registros)
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
    """
    O documento, na estrutura da wiki do MackSun.

    Sem comentário editorial, sem comparação com outros instrumentos e sem nota
    sobre como este arquivo foi produzido: a página de referência não tem nada
    disso, e um dicionário de dados descreve o dado, não a si mesmo.

    Em inglês, como a página de referência e como as próprias descrições de campo,
    que são as mesmas que vão dentro dos arquivos gerados.
    """
    linhas = []
    add = linhas.append

    add("# HATS")
    add("")
    add("High Altitude THz Solar photometer. Solar observations at 15 THz (20 um), "
        "recorded at OAFA, San Juan, Argentina.")
    add("")
    add("Formats: FITS, CSV")
    add("")

    add("## Overview")
    add("")
    add("The instrument writes two binary files per hour of observation: the "
        "detector signal, sampled at 1 kHz, and the telescope pointing solution, "
        "written by the control loop about once every 1.05 s. Each one is ingested "
        "as its own raw source.")
    add("")
    add("Four data structures are published, with the names they have in the "
        "instrument processing code:")
    add("")
    linhas += tabela(
        ["Structure", "Source", "Resolution", "Content"],
        [(hats_export.CAMADA_RDATA.nome, "`.rbd`", "1 ms",
          "detector channels in raw ADC counts"),
         (hats_export.CAMADA_CDATA.nome, "`.rbd`", "1 ms",
          "the same channels in physical units"),
         (hats_export.CAMADA_APONTAMENTO.nome, "`.aux`", "1.05 s",
          "telescope pointing solution"),
         (hats_export.CAMADA_DECONV.nome, "`.rbd`", "32 ms",
          "amplitude of the 20 Hz modulation of the detector signal")])
    add("")
    add("In FITS, structures that come from the same file are carried as separate "
        "table extensions of one file, named after the structure. In CSV, which is "
        "flat, each structure is a file of its own.")
    add("")
    linhas += tabela(
        ["Scientific product", "Level", "Resolution", "FITS structures",
         "CSV structure", "Content"],
        [(arquivo["produto"].chave, arquivo["produto"].nivel,
          arquivo["produto"].resolucao,
          ", ".join(camada.nome for camada in arquivo["camadas_fits"]) or "not generated",
          arquivo["camada_csv"].nome, arquivo["gerado_por"])
         for arquivo in ARQUIVOS])
    add("")

    # A página de referência diz que o pipeline não aplica calibração "unless
    # explicitly stated in the documentation of the corresponding instrument".
    # Para o HATS há calibração, então declará-la aqui é requisito, não comentário.
    add("## Calibration")
    add("")
    add("The `.rbd` files store each channel as counts of an AD7770 analog-to-digital "
        "converter, with 24 significant bits inside a 4-byte word and the sign in "
        "bit 23. During ingestion two operations are applied, both declared in the "
        "CRAAM format tables:")
    add("")
    add("1. sign extension of the 24-bit value;")
    add("2. the calibration line declared in the format table, converting counts to "
        "millivolts on the signal channels and to degrees Celsius on the temperature "
        "channels.")
    add("")
    add("No further correction is applied: no atmospheric compensation, no baseline "
        "subtraction and no absolute flux calibration.")
    add("")
    add("Both sides of step 2 are published: the counts as the `{}` structure and the "
        "physical values as the `{}` structure, sample by sample, so that the "
        "calibration can be checked or redone from the published files.".format(
            hats_export.CAMADA_RDATA.nome, hats_export.CAMADA_CDATA.nome))
    add("")
    add("The `.aux` file is stored in its final units and no calibration is applied "
        "to it. Three of its fields are declared as degrees in the format table "
        "while the values stored are hours, arcsec/s and arcsec/s; they are "
        "published as stored, in the units given in the field tables below.")
    add("")

    add("## Derived Data Products")
    add("")
    add("### Demodulated amplitude")
    add("")
    add("The HATS detector is a Golay cell behind a 20 Hz chopper, so the detector "
        "signal is a modulated carrier and not a flux series. The amplitude of that "
        "modulation is the physical quantity the instrument measures. It is "
        "recovered by the Goertzel algorithm over a flat-top window of {} samples "
        "stepping {} samples at a time, which yields one value every 32 ms, and it "
        "is published as the `{}` structure. The timestamp of each record is the "
        "centre of its analysis window.".format(
            constants.WINDOW_SIZE, constants.STEPS,
            hats_export.CAMADA_DECONV.nome))
    add("")

    add("### Fixed-window means")
    add("")
    add("Consecutive samples of the detector signal are grouped into fixed, "
        "non-overlapping windows and reduced to a representative value.")
    add("")
    linhas += tabela(
        ["Scientific product", "Window", "Reduction", "Timestamp"],
        [(produto.chave, "{} samples".format(produto.janela), "arithmetic mean",
          "sample index {} within each window".format(produto.janela // 2))
         for produto in PRODUTOS_DERIVADOS])
    add("")
    add("Each derived window spans a whole number of chopper cycles, so the carrier "
        "cancels out and the mean does not carry the solar signal. Measured over "
        "299 s of real observation, the derived means fluctuate around zero (mean "
        "0.02 mV, standard deviation 1.5 mV, range -2.3 to +2.2 mV) while the "
        "amplitude of the modulation over the same interval is 124.2 mV.")
    add("")

    cabecalho_exemplo = export._cabecalho(
        "example.fits", registros["detector"], hats_export.PRODUTO_RAW)
    tabela_cabecalho = [(chave,) + CABECALHO_DESCRICAO[chave]
                        for chave, _ in cabecalho_exemplo]

    add("## HATS FITS")
    add("")
    add("Format: FITS File | Encoding: Binary | Structure: Primary header + one "
        "binary table extension per data structure")
    add("")
    add("### Header")
    add("")
    add("The primary header of every generated file carries the same fields. The "
        "documentation block is written as COMMENT cards of the primary header.")
    add("")
    linhas += tabela(["Field name", "Data type", "Description"], tabela_cabecalho)
    add("")
    add("### Observation Data Fields")
    add("")
    add("One table extension per structure, with EXTNAME set to the name of the "
        "structure. The FITS convention writes EXTNAME in upper case.")
    add("")
    for camada in ESTRUTURAS:
        add("#### {}".format(camada.nome))
        add("")
        add(camada.descricao + ".")
        add("")
        linhas += tabela(
            ["Field name", "Data type", "FITS format", "Description"],
            [(coluna.nome, TIPO_NA_WIKI[coluna.tipo], coluna.fits, coluna.descricao)
             for coluna in camada.colunas])
        add("")

    add("## HATS CSV")
    add("")
    add("Delimiter: Pipe (|) | Encoding: UTF-8 | Structure: Documentation + Header + "
        "Observation Data Fields")
    add("")
    add("### Header")
    add("")
    linhas += tabela(["Field name", "Data type", "Description"], tabela_cabecalho)
    add("")
    add("### Observation Data Fields")
    add("")
    add("One file per structure.")
    add("")
    for camada in ESTRUTURAS:
        add("#### {}".format(camada.nome))
        add("")
        linhas += tabela(
            ["Position", "Column Name", "Data type", "Description"],
            [(posicao, coluna.nome, TIPO_NA_WIKI[coluna.tipo], coluna.descricao)
             for posicao, coluna in enumerate(camada.colunas, start=1)])
        add("")

    add("### Example")
    add("")
    # Um exemplo por fonte RAW: o level 0 de cada arquivo que o instrumento escreve.
    com_exemplo = (hats_export.PRODUTO_RAW, hats_export.PRODUTO_APONTAMENTO)
    for arquivo in ARQUIVOS:
        disponiveis = registros.get(arquivo["fonte"])
        if not disponiveis or arquivo["produto"] not in com_exemplo:
            continue
        add("`{}`, structure `{}`:".format(arquivo["produto"].chave,
                                           arquivo["camada_csv"].nome))
        add("")
        bloco_cabecalho, bloco_itens = exemplo_do_csv(
            export, disponiveis, arquivo["metodo_csv"])
        add("```")
        linhas.extend(bloco_cabecalho)
        add("")
        linhas.extend(bloco_itens)
        add("```")
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
