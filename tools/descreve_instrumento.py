#!/usr/bin/env python3
"""
Descreve o HATS a partir do dado real, para quem mantém o catálogo do MackSun.

A saída deste comando é a informação que cabe ao nosso lado fornecer para os blocos
`ingestion`, `export` e `raw_sources` do documento de catálogo. Não é o documento:
esse é escrito pelo suporte do MackSun, e o guia é explícito que não deve ser
alterado pelo desenvolvedor.

O ponto de escrever isto como programa, e não como texto, é que a estrutura do
registro é **o que o load_data produz** — não uma descrição feita à mão que possa
divergir dele. Se o registro mudar, esta saída muda junto.

    tools/descreve_instrumento.py --data-dir "$HATS_DATA_InputPath"
    tools/descreve_instrumento.py --data-dir Amostra --json
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
for entrada in (str(REPO_ROOT), str(PLUGIN_DIR)):
    if entrada not in sys.path:
        sys.path.insert(0, entrada)

import hats_binary_reader as binary_reader                                   # noqa: E402
import hats_ingestion                                                        # noqa: E402
from external_contracts.instrument_ingestion_runtime_spec import (            # noqa: E402
    InstrumentIngestionRuntimeSpec)

from hats import constants, discovery, weather                               # noqa: E402

# Fatos do instrumento, medidos e não estimados. As cadências do apontamento e da
# estação vêm dos arquivos reais; o volume, de uma hora completa.
CADENCIAS = {
    ".rbd": ("detector, celula Golay atras de um chopper de 20 Hz", "1 kHz", "1ms"),
    ".aux": ("apontamento, do getPos/TheSkyX", "~0,95 Hz", "1050ms"),
    ".ws": ("estacao meteorologica, ASCII", "0,2 Hz", "5s"),
}

HORAS_DE_SOL_POR_DIA = 13

# O que cada arquivo seria como fonte RAW. Os nomes de `mode` e `source_key` são do
# catálogo, ou seja do suporte; aqui vai só o que descreve o instrumento.
FONTES = [
    {"arquivo": ".rbd", "conteudo": "sinal do detector", "resolucao": "1ms",
     "por": "hora", "cadencia_medida": "1,000 ms — passo de 10 husec em 3.599.864 de "
                                       "3.599.865 intervalos medidos"},
    {"arquivo": ".aux", "conteudo": "apontamento do telescopio", "resolucao": "1050ms",
     "por": "hora", "cadencia_medida": "1,0507 s em media, alternando entre 10480 e "
                                       "10520 husec — laco de software, nao relogio de hardware"},
    {"arquivo": ".ws", "conteudo": "estacao meteorologica", "resolucao": "5s",
     "por": "dia", "cadencia_medida": "5,008 s em media; 142 leituras perdidas no dia medido"},
]

# Produtos que o instrumento PERMITE, com o estado de cada um. Quais existem de fato
# é declaração do catálogo.
PRODUTOS = [
    {"conteudo": "sinal do detector sem transformacao", "nivel": "raw",
     "resolucao": "1ms", "fonte": ".rbd", "estado": "obrigatorio pelo §7.2"},
    {"conteudo": "media aritmetica de 100 amostras", "nivel": "derived",
     "resolucao": "100ms", "fonte": ".rbd",
     "estado": "obrigatorio pelo §7.2; janela e media CONFIRMADAS pelo suporte"},
    {"conteudo": "media aritmetica de 1000 amostras", "nivel": "derived",
     "resolucao": "1s", "fonte": ".rbd",
     "estado": "obrigatorio pelo §7.2; janela e media CONFIRMADAS pelo suporte"},
    {"conteudo": "AMPLITUDE DA MODULACAO DE 20 Hz — o dado cientifico do HATS",
     "nivel": "derived", "resolucao": "32ms (31,25 Hz)", "fonte": ".rbd",
     "estado": "PENDENTE: nao esta entre os seis obrigatorios e depende de o catalogo "
               "declarar. Sem ele o HATS publica apenas a portadora e medias dela"},
    {"conteudo": "apontamento como lido", "nivel": "raw", "resolucao": "1050ms",
     "fonte": ".aux", "estado": "PENDENTE: depende de fonte RAW propria"},
    {"conteudo": "estacao meteorologica como lida", "nivel": "raw", "resolucao": "5s",
     "fonte": ".ws", "estado": "PENDENTE: depende de fonte RAW propria"},
]


def spec_minimo():
    """O spec que o pipeline monta; aqui só o necessário para chamar o load_data."""
    return InstrumentIngestionRuntimeSpec(
        instrument_name="HATS", time_field="UTC_TIME",
        native_resolutions_by_mode={"DEFAULT": "1ms"},
        scan_ignore_dirs=[], include_prefixes=["hats-"], include_suffixes=[],
        include_extensions=[".rbd", ".aux", ".ws"], exclude_extensions=[],
        exclude_suffixes=[], processed_dir_name="00_processed",
        ignored_dir_name="00_ignored", invalid_format_dir_name="99_formato_invalid")


def serializa(valor):
    if isinstance(valor, datetime):
        return valor.isoformat()
    return valor


# Folga suficiente para passar do prefixo de registros defasados. Ele é pequeno em
# algumas horas (559 nas 18:00 de 2026-03-17) e grande em outras: nas 11:00 do mesmo
# dia o husec começa em 10 — o relógio do instrumento não estava ajustado — e mais de
# três mil registros ficam abaixo da hora nominal.
LIMITE_PADRAO = 50000


def coleta(data_dir, day=None, hour=None, record_limit=LIMITE_PADRAO):
    indice = discovery.build_day_index(Path(data_dir))
    if not indice:
        raise SystemExit("nenhum arquivo do HATS em {}".format(data_dir))

    dia = day or sorted(indice)[0]
    if dia not in indice:
        raise SystemExit("dia {} nao encontrado. Disponiveis: {}".format(
            dia, ", ".join(sorted(indice))))

    horas = indice[dia]["hours"]
    hora = hour or next((h for h in sorted(horas) if h != "daily"), None)
    arquivos = horas.get(hora, {})

    rbd = arquivos.get("rbd")
    if not rbd:
        raise SystemExit("nenhum .rbd para {} hora {}".format(dia, hora))

    # Rodando do repositório, os XML estão em XMLTables/. Em produção quem manda é
    # HATSXMLPATH, e o reader falha se ela não estiver definida — de propósito.
    xml_dir = os.environ.get("HATSXMLPATH") or str(REPO_ROOT / "XMLTables")
    ingestion = hats_ingestion.HatsIngestion(xml_dir=xml_dir, record_limit=record_limit)
    resultado = ingestion.load_data(spec_minimo(), str(rbd))
    registro = resultado.data_records[0]

    # Volume: o total real do arquivo, não o recorte lido.
    por_hora = resultado.instrument_metadata["records_in_file"]

    apontamento = amostra_do_aux(arquivos.get("aux"))
    estacao = amostra_do_ws(horas.get("daily", {}).get("ws"))
    descoberta = observa_diretorio(Path(data_dir))

    return {
        "instrumento": {
            "nome": "HATS",
            "descricao": "High Altitude THz Solar photometer",
            "observatorio": "OAFA, San Juan, Argentina",
            "frequencia_observada": "15 THz (20 um)",
            "amostragem": "{:.0f} Hz".format(constants.SAMPLING_FREQUENCY),
            "frequencia_do_chopper": "{:.0f} Hz".format(constants.TARGET_FREQUENCY),
        },
        "arquivos": [
            {"extensao": ext, "conteudo": conteudo, "taxa": taxa,
             "resolucao_nativa": resolucao,
             "exemplo": "hats-2026-03-17T1800" + ext if ext != ".ws" else "hats-2026-03-17.ws",
             "por_dia": "um por hora de observacao" if ext != ".ws" else "um por dia"}
            for ext, (conteudo, taxa, resolucao) in CADENCIAS.items()
        ],
        "convencao_de_nome": {
            "padrao": "hats-YYYY-MM-DDTHHMM.<ext>",
            "observacao": "o dia e a hora vem do NOME do arquivo, nao de estrutura de "
                          "pastas; os minutos sao sempre 00",
            "layout": "os .rbd ficam soltos no diretorio de dados; os .aux e os .ws "
                      "num subdiretorio aux/",
        },
        "registro_do_detector": {
            "observacao": "produzido pelo load_data a partir de {}".format(rbd.name),
            "campos": {chave: serializa(valor) for chave, valor in registro.items()},
            "tipos": {chave: type(valor).__name__ for chave, valor in registro.items()},
        },
        "volume": {
            "registros_por_hora": por_hora,
            "horas_de_sol_por_dia": HORAS_DE_SOL_POR_DIA,
            "registros_por_dia": por_hora * HORAS_DE_SOL_POR_DIA,
            "comparacao": "cerca de cinco vezes o volume diario do POEMAS a 10 ms",
        },
        "consistencia_temporal": {
            "carimbo_nativo": "husec, centesimos de milissegundo desde 0 UT",
            "resolucao_do_carimbo": "0,1 ms",
            "passo_esperado": "10 husec entre amostras consecutivas",
            "registros_descartados_antes_da_hora":
                resultado.instrument_metadata["records_dropped_before_hour"],
            "avisos_desta_leitura": resultado.processing_warnings,
        },
        "ja_definido_pelo_suporte": [
            "produtos derivados: MEDIA aritmetica (confirmado), em janelas de 100 e de "
            "1000 amostras — o alvo e a resolucao de 100ms e 1s, e nao a contagem de 10 "
            "e 100 do guia, que pressupoe amostragem de 10 ms como a do POEMAS",
        ],
        "fontes_raw": FONTES,
        "produtos_possiveis": PRODUTOS,
        "file_discovery_observado": descoberta,
        "registro_do_apontamento": apontamento,
        "registro_da_estacao": estacao,
        "decisoes_em_aberto": [
            "UTC_TIME esta com microssegundos: o husec tem resolucao de 0,1 ms e o "
            "exemplo do guia (secao 6) mostra milissegundo. Confirmar se a persistencia aceita.",
            "os valores numericos sao numero, nao string de 4 decimais como no exemplo "
            "do guia. O passo do canal de temperatura e 3e-5 C, que 4 decimais nao representam.",
            "o POEMAS grava tambem FILEPATH, FULL_FILE_PATH e TELESCOPE em cada registro, "
            "que o exemplo do guia nao mostra. Incluir?",
            "o .aux e o .ws precisam de raw_sources proprias, como FAST/INTG do SST?",
            "ha produto para o dado cientifico do HATS (amplitude demodulada a 31,25 Hz)? "
            "Com que nomes de metodo?",
        ],
    }


def amostra_do_aux(caminho):
    """
    Campos do apontamento, com valores reais, lidos pelo reader.

    O `load_data` do `.aux` ainda não existe — depende de o catálogo declarar uma
    fonte RAW para ele. Então aqui não há nome final de campo nem UTC_TIME: é o que
    o formato declara, que é justamente o que o suporte precisa para dimensionar.
    """
    if not caminho:
        return None
    xml_dir = os.environ.get("HATSXMLPATH") or str(REPO_ROOT / "XMLTables")
    esquema = binary_reader.load_schema("aux", xml_dir)
    bloco, quantidade = next(binary_reader.iter_calibrated_blocks(caminho, esquema,
                                                                 record_limit=200))
    # jd == 0 marca registro defasado: o husec esta certo, o apontamento e de ~1053 s
    # antes. Mostrar um desses daria a impressao errada do dado.
    bons = [i for i in range(quantidade) if bloco["jd"][i] != 0.0]
    posicao = bons[0] if bons else 0
    return {
        "observacao": "campos do {} como o formato declara; o load_data do .aux "
                      "depende de fonte RAW propria".format(caminho.name),
        "campos": {nome.upper(): serializa(coluna[posicao])
                   for nome, coluna in bloco.items()},
        "registros_defasados_nos_200_primeiros":
            "{} de {} trazem jd == 0, ou seja apontamento de cerca de 1053 s antes do "
            "proprio carimbo".format(quantidade - len(bons), quantidade),
        "unidades_erradas_no_xml":
            "o HATSAuxFormat.xml declara right_ascension em graus e as taxas em graus/s; "
            "os dados estao em horas e em arcsec/s. Verificado contra efemerides solares",
    }


def amostra_do_ws(caminho):
    """Campos da estação, com valores reais."""
    if not caminho:
        return None
    linhas, rejeitadas, reparadas = weather.read(caminho)
    if not linhas:
        return None
    return {
        "observacao": "campos do {} como o leitor entrega".format(caminho.name),
        "campos": {chave.upper(): serializa(valor) for chave, valor in linhas[0].items()},
        "linhas_no_dia": len(linhas),
        "linhas_rejeitadas": rejeitadas,
        "carimbos_reparados": "{} linha(s) vinham com byte de controle 0x7f colado no "
                              "carimbo e foram limpas".format(reparadas),
    }


def observa_diretorio(data_dir):
    """O que existe de fato no diretório, para o bloco file_discovery."""
    if not data_dir.is_dir():
        return None
    extensoes = {}
    subdiretorios = []
    for entrada in sorted(data_dir.iterdir()):
        if entrada.is_dir():
            subdiretorios.append(entrada.name)
        else:
            extensoes[entrada.suffix or "(sem extensao)"] = \
                extensoes.get(entrada.suffix or "(sem extensao)", 0) + 1
    aux = data_dir / "aux"
    if aux.is_dir():
        for entrada in sorted(aux.iterdir()):
            if entrada.is_file():
                chave = "aux/" + (entrada.suffix or "(sem extensao)")
                extensoes[chave] = extensoes.get(chave, 0) + 1
    return {
        "arquivos_por_extensao": extensoes,
        "subdiretorios": subdiretorios,
        "a_excluir": [ext for ext in extensoes if ext in (".txt", ".log")],
        "a_ignorar_na_varredura": [nome for nome in subdiretorios if nome != "aux"],
        "observacao": "o aux/ NAO deve ser ignorado: e onde ficam os .aux e os .ws",
    }


def imprime(dados):
    inst = dados["instrumento"]
    print()
    print("  " + "=" * 74)
    print("  {} — {}".format(inst["nome"], inst["descricao"]))
    print("  {} · {} · amostragem {} · chopper {}".format(
        inst["observatorio"], inst["frequencia_observada"],
        inst["amostragem"], inst["frequencia_do_chopper"]))
    print("  " + "=" * 74)

    print()
    print("  ARQUIVOS QUE O INSTRUMENTO PRODUZ")
    for arquivo in dados["arquivos"]:
        print("    {:<6} {:<52} {}".format(
            arquivo["extensao"], arquivo["conteudo"], arquivo["resolucao_nativa"]))
        print("    {:<6} {:<52} {}".format("", arquivo["exemplo"], arquivo["por_dia"]))
    convencao = dados["convencao_de_nome"]
    print()
    print("    padrao : {}".format(convencao["padrao"]))
    print("    nota   : {}".format(convencao["observacao"]))
    print("    layout : {}".format(convencao["layout"]))

    registro = dados["registro_do_detector"]
    print()
    print("  ESTRUTURA DO REGISTRO DO DETECTOR")
    print("    {}".format(registro["observacao"]))
    print()
    largura = max(len(c) for c in registro["campos"])
    for chave, valor in registro["campos"].items():
        print("      {:<{w}}  {:<26}  {}".format(
            chave, str(valor), registro["tipos"][chave], w=largura))

    apontamento = dados.get("registro_do_apontamento")
    if apontamento:
        print()
        print("  ESTRUTURA DO APONTAMENTO")
        print("    {}".format(apontamento["observacao"]))
        print()
        largura = max(len(c) for c in apontamento["campos"])
        for chave, valor in apontamento["campos"].items():
            print("      {:<{w}}  {}".format(chave, valor, w=largura))
        print()
        print("    defasados : {}".format(apontamento["registros_defasados_nos_200_primeiros"]))
        print("    ATENCAO   : {}".format(apontamento["unidades_erradas_no_xml"]))

    estacao = dados.get("registro_da_estacao")
    if estacao:
        print()
        print("  ESTRUTURA DA ESTACAO METEOROLOGICA")
        print("    {}".format(estacao["observacao"]))
        print()
        largura = max(len(c) for c in estacao["campos"])
        for chave, valor in estacao["campos"].items():
            print("      {:<{w}}  {}".format(chave, valor, w=largura))
        print()
        print("    {} linhas no dia, {} rejeitadas".format(
            estacao["linhas_no_dia"], estacao["linhas_rejeitadas"]))
        print("    {}".format(estacao["carimbos_reparados"]))

    print()
    print("  FONTES RAW (para o bloco raw_sources)")
    for fonte in dados["fontes_raw"]:
        print("    {:<6} {:<28} resolucao {:<8} um arquivo por {}".format(
            fonte["arquivo"], fonte["conteudo"], fonte["resolucao"], fonte["por"]))
        print("    {:<6} cadencia medida: {}".format("", fonte["cadencia_medida"]))

    print()
    print("  PRODUTOS QUE O INSTRUMENTO PERMITE (para o bloco export)")
    for produto in dados["produtos_possiveis"]:
        print("    {:<10} {:<14} {:<6} {}".format(
            produto["nivel"], produto["resolucao"], produto["fonte"], produto["conteudo"]))
        print("    {:<10} {}".format("", produto["estado"]))

    descoberta = dados.get("file_discovery_observado")
    if descoberta:
        print()
        print("  O QUE EXISTE NO DIRETORIO (para o bloco file_discovery)")
        for extensao, quantidade in sorted(descoberta["arquivos_por_extensao"].items()):
            print("    {:>4} x {}".format(quantidade, extensao))
        print("    subdiretorios: {}".format(", ".join(descoberta["subdiretorios"])))
        print("    a excluir    : {}".format(", ".join(descoberta["a_excluir"]) or "(nada)"))
        print("    a ignorar    : {}".format(", ".join(descoberta["a_ignorar_na_varredura"]) or "(nada)"))
        print("    {}".format(descoberta["observacao"]))

    volume = dados["volume"]
    print()
    print("  VOLUME (para o bloco partitioning)")
    print("    {:>12,} registros por hora de observacao".format(volume["registros_por_hora"]))
    print("    {:>12,} registros por dia, em {} horas de sol".format(
        volume["registros_por_dia"], volume["horas_de_sol_por_dia"]))
    print("    {}".format(volume["comparacao"]))

    tempo = dados["consistencia_temporal"]
    print()
    print("  CONSISTENCIA TEMPORAL")
    print("    carimbo nativo : {} (resolucao {})".format(
        tempo["carimbo_nativo"], tempo["resolucao_do_carimbo"]))
    print("    passo esperado : {}".format(tempo["passo_esperado"]))
    print("    descartados antes da hora nominal: {}".format(
        tempo["registros_descartados_antes_da_hora"]))
    for aviso in tempo["avisos_desta_leitura"]:
        print("    aviso: {}".format(aviso))

    print()
    print("  JA DEFINIDO PELO SUPORTE")
    for definido in dados["ja_definido_pelo_suporte"]:
        print("    - {}".format(definido))

    print()
    print("  DECISOES QUE DEPENDEM DO SUPORTE")
    for numero, decisao in enumerate(dados["decisoes_em_aberto"], 1):
        print("    {}. {}".format(numero, decisao))
    print()


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="descreve_instrumento",
        description="Descreve o HATS a partir do dado real, para quem mantém o catálogo.")
    parser.add_argument("--data-dir", default=os.environ.get("HATS_DATA_InputPath", "Amostra"))
    parser.add_argument("--day", default=None)
    parser.add_argument("--hour", default=None)
    parser.add_argument("--record-limit", type=int, default=LIMITE_PADRAO,
                        help="Quantos registros ler. Só o primeiro é mostrado; o volume "
                             "reportado é o total real do arquivo.")
    parser.add_argument("--json", action="store_true", help="Sai em JSON em vez de texto.")
    args = parser.parse_args(argv)

    dados = coleta(args.data_dir, args.day, args.hour, args.record_limit)
    if args.json:
        print(json.dumps(dados, indent=2, ensure_ascii=False))
    else:
        imprime(dados)
    return 0


if __name__ == "__main__":
    sys.exit(main())
