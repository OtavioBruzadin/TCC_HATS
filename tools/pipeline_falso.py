#!/usr/bin/env python3
"""
Executor de referência: faz o que o guia descreve que o pipeline do MackSun faz.

Serve para exercitar o plugin de ponta a ponta sem o MackSun real. O fluxo é o do
§1 do guia, passo por passo:

    arquivo RAW no filesystem
      -> load_data(spec, file_full_path)
      -> TimeSeriesLoadDataResult
      -> o pipeline valida e persiste
      -> o pipeline carrega os dados RAW de volta do banco
      -> Generate*(raw_records)
      -> TimeSeriesSpecialistGeneratedFile
      -> o pipeline renomeia, publica e registra

O que este executor NÃO é
-------------------------
**Não é o catálogo.** O `instrument_catalog.json` é documento da equipe de suporte,
e o §13 diz que não deve ser escrito nem alterado pelo desenvolvedor. Este programa
é o outro lado: ele *lê* o documento e executa o que estiver declarado lá. Enquanto
o do HATS não existir, as tarefas podem ser passadas na linha de comando com
`--tarefas`, que é explicitamente ad hoc e não finge ser declaração do instrumento.

Quando o documento chegar, `--catalogo arquivo.json --instrumento HATS` passa a ser
o teste de aceitação dele: se um nome de método divergir, se uma resolução não
casar, se um `source_key` apontar para fonte que não existe, aparece aqui e não em
produção.

Três coisas que só este executor alcança
----------------------------------------
1. **Carregar o plugin pelo caminho.** O §13.2 é explícito: "o pipeline não utiliza
   import tradicional. Ele carrega o arquivo e executa o entrypoint". Os testes
   unitários fazem `import instrument`, com o diretório do plugin no `sys.path` —
   o que esconde qualquer dependência que só resolva de dentro do repositório.

2. **O ida e volta pelo banco.** A exportação não recebe os registros que a
   ingestão produziu: recebe o que voltou da persistência. O suporte confirmou que
   ela guarda o `UTC_TIME` com precisão de milissegundo, então aqui os registros
   passam por JSON e pelo truncamento, como passariam lá.

3. **O confronto entre o que o plugin devolve e o que o catálogo declara.** O
   retorno traz `scientific_product_key`, `product_resolution`, `level` e
   `generated_file_format`; o catálogo declara os quatro. Divergência entre eles é
   falha em tempo de execução no sistema deles, e nada no plugin pega isso sozinho.

Nada é movido
-------------
O pipeline de verdade move o arquivo lido para `00_processed`. Este executor apenas
relata a classificação: os dados do HATS são 7 GB de observação real e um programa
de teste não tem o que mexer neles. A publicação, sim, acontece — numa pasta de
saída própria, para provar que o plugin não depende do caminho onde escreveu.
"""

import argparse
import importlib.util
import json
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------- catálogo

def _sem_dialeto_do_mongo(texto: str) -> str:
    """
    O documento deles é exportado no dialeto do shell do mongo, não JSON puro.

    Traz `NumberInt(150000000)`, `ObjectId("...")` e `ISODate("...")`, que o
    `json.loads` recusa. A conversão é textual e não altera valor nenhum.
    """
    for padrao, troca in (
            (r"NumberInt\(\s*(-?\d+)\s*\)", r"\1"),
            (r"NumberLong\(\s*(-?\d+)\s*\)", r"\1"),
            (r"NumberDecimal\(\s*\"([^\"]*)\"\s*\)", r"\1"),
            (r"ObjectId\(\s*(\"[^\"]*\")\s*\)", r"\1"),
            (r"ISODate\(\s*(\"[^\"]*\")\s*\)", r"\1")):
        texto = re.sub(padrao, troca, texto)
    return texto


def carrega_catalogo(caminho: Path, instrumento: str = None) -> dict:
    """
    O documento de um instrumento, do arquivo que o suporte mantém.

    O arquivo de referência traz mais de um documento concatenado (POEMAS e SST),
    então a leitura é incremental e o instrumento é escolhido por nome.
    """
    texto = _sem_dialeto_do_mongo(Path(caminho).read_text(encoding="utf-8"))
    leitor = json.JSONDecoder()
    documentos = []
    posicao = 0
    while posicao < len(texto):
        while posicao < len(texto) and texto[posicao] in " \t\r\n":
            posicao += 1
        if posicao >= len(texto):
            break
        documento, posicao = leitor.raw_decode(texto, posicao)
        documentos.append(documento)

    if not documentos:
        raise SystemExit("nenhum documento em {}".format(caminho))

    if instrumento:
        for documento in documentos:
            if str(documento.get("instrument", "")).upper() == instrumento.upper():
                return documento
        raise SystemExit("nenhum documento de {} em {}. Ha: {}".format(
            instrumento, caminho,
            ", ".join(str(d.get("instrument")) for d in documentos)))

    if len(documentos) > 1:
        raise SystemExit("{} tem {} documentos; escolha com --instrumento".format(
            caminho, len(documentos)))
    return documentos[0]


def catalogo_de_tarefas(tarefas: list, instrumento: str = "HATS",
                        extensoes: list = None) -> dict:
    """
    Um plano mínimo montado dos argumentos da linha de comando.

    Não é catálogo e não pretende ser: não tem `database`, não tem `partitioning`,
    não tem `publication`, e os nomes vêm de quem rodou o comando. Existe para o
    executor poder rodar antes de o documento do instrumento existir.

    Cada item aceita a forma `MODO:Metodo`, ou `MODO+OUTRO:Metodo` para um produto
    que precisa de mais de uma fonte — que é o caso do level 1. Sem prefixo, o
    método recebe tudo o que foi ingerido.

    O campo `_modos` leva underscore porque é nosso: no documento deles um produto
    aponta para UMA fonte, pelo `source_key`, e o esquema não tem como declarar
    duas. Marcar com underscore deixa claro onde o plano ad hoc vai além do que o
    catálogo sabe expressar — que é, por si, o achado a levar para o suporte.
    """
    produtos = []
    for item in tarefas:
        modos, _, nome = item.rpartition(":")
        produtos.append({
            "scientific_product_key": None,
            "level": None,
            "product_resolution": None,
            "source_key": None,
            "_modos": [m for m in modos.split("+") if m] or None,
            "tasks": [{"format": None, "call": nome}],
        })
    return {
        "instrument": instrumento,
        "_origem": "--tarefas na linha de comando, nao um catalogo",
        "ingestion": {"time_field": "UTC_TIME",
                      "file_discovery": {"include_extensions": list(extensoes or [])}},
        "export": {"raw_sources": [], "products": produtos},
    }


# ------------------------------------------------------------------- descoberta

def descobre_arquivos(raiz: Path, regras: dict):
    """
    Aplica o bloco `ingestion.file_discovery` do catálogo sobre um diretório.

    Devolve (aceitos, recusados). A classificação é a deles; o destino dos
    recusados (`ignored_dir_name`) é só relatado.
    """
    ignorar = set(regras.get("scan_ignore_dirs") or ())
    prefixos = tuple(regras.get("include_prefixes") or ())
    sufixos = tuple(regras.get("include_suffixes") or ())
    extensoes = tuple(e.lower() for e in (regras.get("include_extensions") or ()))
    extensoes_fora = tuple(e.lower() for e in (regras.get("exclude_extensions") or ()))
    sufixos_fora = tuple(regras.get("exclude_suffixes") or ())

    aceitos, recusados = [], []
    for caminho in sorted(Path(raiz).rglob("*")):
        if not caminho.is_file():
            continue
        if ignorar.intersection(parte for parte in caminho.relative_to(raiz).parts[:-1]):
            continue

        nome = caminho.name
        extensao = caminho.suffix.lower()

        if extensao in extensoes_fora or (sufixos_fora and nome.endswith(sufixos_fora)):
            recusados.append((caminho, "excluido por extensao ou sufixo"))
            continue
        if prefixos and not nome.startswith(prefixos):
            recusados.append((caminho, "prefixo fora de include_prefixes"))
            continue
        if sufixos and not nome.endswith(sufixos):
            recusados.append((caminho, "sufixo fora de include_suffixes"))
            continue
        if extensoes and extensao not in extensoes:
            recusados.append((caminho, "extensao fora de include_extensions"))
            continue
        aceitos.append(caminho)

    return aceitos, recusados


# ----------------------------------------------------------- carga do plugin

def carrega_plugin(source_file: Path, entrypoint: str = "build_instrument"):
    """
    Carrega o arquivo do plugin pelo caminho e executa o entrypoint — §13.2.

    Sem `import` e sem mexer no `sys.path`: é o core que faz isso, e é aqui que
    aparece qualquer dependência que o plugin só resolva de dentro do repositório.
    """
    source_file = Path(source_file).resolve()
    if not source_file.exists():
        raise SystemExit("plugin.source_file nao existe: {}".format(source_file))

    especificacao = importlib.util.spec_from_file_location(
        "plugin_carregado_pelo_caminho", source_file)
    modulo = importlib.util.module_from_spec(especificacao)
    try:
        especificacao.loader.exec_module(modulo)
    except ImportError as erro:
        # O plugin pode explicar a própria falta — e o do HATS explica. Quando
        # explicar, a mensagem dele é a que serve; quando não, fica a do catálogo.
        raise SystemExit(
            "o plugin {} nao pode ser carregado.\n\n{}\n\n"
            "O bloco plugin.dependencies do catalogo diz: \"o instrumento executa no "
            "mesmo ambiente Python do core. Todas as dependencias importadas no "
            "arquivo do instrumento devem estar instaladas no ambiente do "
            "job.\"".format(source_file, erro)) from erro

    construtor = getattr(modulo, entrypoint, None)
    if construtor is None:
        raise SystemExit("o arquivo {} nao expoe {}()".format(source_file, entrypoint))
    if not callable(construtor):
        raise SystemExit("{} nao e callable".format(entrypoint))

    adapter = construtor()
    if adapter is None:
        raise SystemExit("{}() devolveu None".format(entrypoint))
    return modulo, adapter


def monta_spec(catalogo: dict, modulo) -> object:
    """
    Transforma o bloco `ingestion` do catálogo no `spec` que o plugin consome.

    O §13.2 diz que é o pipeline que faz isso: "o pipeline transforma essas
    configuracoes em um objeto spec e o entrega ao plugin. O plugin nao constroi
    esse objeto, apenas o consome."

    Repare na tradução de nome: o catálogo declara `native_resolution_modes` (ou
    `native_resolution`, no POEMAS, que tem uma fonte só) e o contrato recebe
    `native_resolutions_by_mode`. Os dois nomes diferentes são do sistema deles.

    A classe vem da cópia dos contratos que o plugin carrega. No sistema real ela é
    do core; aqui não há outro lugar de onde tirá-la sem duplicar o contrato.
    """
    from external_contracts.instrument_ingestion_runtime_spec import (
        InstrumentIngestionRuntimeSpec)

    ingestao = catalogo.get("ingestion") or {}
    descoberta = ingestao.get("file_discovery") or {}

    por_modo = dict(ingestao.get("native_resolution_modes") or {})
    if not por_modo and ingestao.get("native_resolution"):
        por_modo = {"DEFAULT": ingestao["native_resolution"]}

    return InstrumentIngestionRuntimeSpec(
        instrument_name=str(catalogo.get("instrument") or ""),
        time_field=ingestao.get("time_field") or "UTC_TIME",
        native_resolutions_by_mode=por_modo,
        scan_ignore_dirs=list(descoberta.get("scan_ignore_dirs") or []),
        include_prefixes=list(descoberta.get("include_prefixes") or []),
        include_suffixes=list(descoberta.get("include_suffixes") or []),
        include_extensions=list(descoberta.get("include_extensions") or []),
        exclude_extensions=list(descoberta.get("exclude_extensions") or []),
        exclude_suffixes=list(descoberta.get("exclude_suffixes") or []),
        processed_dir_name=descoberta.get("processed_dir_name") or "00_processed",
        ignored_dir_name=descoberta.get("ignored_dir_name") or "00_ignored",
        invalid_format_dir_name=(descoberta.get("invalid_format_dir_name")
                                 or "99_formato_invalid"),
    )


# ------------------------------------------------------------ persistência falsa

CAMPOS_OBRIGATORIOS_DO_RESULTADO = ("data_records", "data_time_resolution",
                                    "data_level", "observation_day")


def valida_resultado_da_ingestao(resultado, caminho: Path) -> list:
    """O §5.4 lista os campos obrigatórios do retorno. Confere os quatro."""
    problemas = []
    for campo in CAMPOS_OBRIGATORIOS_DO_RESULTADO:
        if not hasattr(resultado, campo):
            problemas.append("{}: o retorno nao tem {}".format(caminho.name, campo))
    if problemas:
        return problemas

    if not resultado.data_records:
        problemas.append("{}: data_records vazio".format(caminho.name))
    if str(resultado.data_level).lower() != "raw":
        problemas.append("{}: data_level e {!r} e a ingestao sempre gera raw".format(
            caminho.name, resultado.data_level))
    if not isinstance(resultado.observation_day, datetime):
        problemas.append("{}: observation_day nao e datetime".format(caminho.name))

    tempo = (resultado.data_records[0] or {}).get("UTC_TIME")
    if tempo is None:
        problemas.append(
            "{}: o §6 exige um campo UTC_TIME em cada registro".format(caminho.name))
    return problemas


def persiste_e_recarrega(registros: list, destino: Path, time_field: str = "UTC_TIME"):
    """
    O meio do caminho: persistir e carregar de volta.

    A exportação não recebe os registros que a ingestão montou — recebe o que
    voltou do banco. Duas coisas acontecem nessa volta, e as duas estão aqui:

      - **o tipo passa por serialização.** O que vai como `datetime` volta como
        `datetime`, mas o que for de um tipo que o banco não guarda volta diferente,
        e é melhor descobrir isso aqui.
      - **o `UTC_TIME` perde os microssegundos abaixo do milissegundo**, porque o
        suporte confirmou que a persistência guarda com precisão de milissegundo.
        Para o HATS isso importa: a amostragem é de 1 ms, e são os 0,1 ms do
        contador que o campo `HUSEC` existe para preservar. Se algum produto
        dependesse do microssegundo do carimbo em vez do husec, quebraria aqui.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)

    def para_json(valor):
        if isinstance(valor, datetime):
            truncado = valor.replace(microsecond=(valor.microsecond // 1000) * 1000)
            return {"__datetime__": truncado.isoformat()}
        return valor

    with destino.open("w", encoding="utf-8") as arquivo:
        for registro in registros:
            arquivo.write(json.dumps(
                {chave: para_json(valor) for chave, valor in registro.items()}) + "\n")

    def do_json(valor):
        if isinstance(valor, dict) and "__datetime__" in valor:
            return datetime.fromisoformat(valor["__datetime__"])
        return valor

    recarregados = []
    with destino.open(encoding="utf-8") as arquivo:
        for linha in arquivo:
            cru = json.loads(linha)
            recarregados.append({chave: do_json(valor) for chave, valor in cru.items()})
    return recarregados


# ------------------------------------------------------------------ exportação

def confronta_com_o_catalogo(gerado, produto: dict, tarefa: dict) -> list:
    """
    O retorno do plugin contra o que o catálogo declarou.

    O §13.2 avisa que divergência de NOME de método é falha em tempo de execução.
    Os outros quatro campos têm o mesmo problema e ninguém os confere: o produto
    declara `scientific_product_key`, `level` e `product_resolution`, a tarefa
    declara `format`, e o plugin devolve os quatro. Se não casarem, o pipeline
    registra o arquivo sob identidade errada.
    """
    problemas = []
    esperado_e_obtido = (
        ("scientific_product_key", produto.get("scientific_product_key"),
         getattr(gerado, "scientific_product_key", None)),
        ("level", produto.get("level"), getattr(gerado, "level", None)),
        ("product_resolution", produto.get("product_resolution"),
         getattr(gerado, "product_resolution", None)),
        ("generated_file_format", tarefa.get("format"),
         getattr(gerado, "generated_file_format", None)),
    )
    for campo, declarado, devolvido in esperado_e_obtido:
        if declarado is None:
            continue            # --tarefas nao declara nada; nao ha o que confrontar
        if str(declarado) != str(devolvido):
            problemas.append(
                "{}: o catalogo declara {!r} e o plugin devolveu {!r}".format(
                    campo, declarado, devolvido))

    caminho = getattr(gerado, "generated_file_full_path", None)
    if not caminho:
        problemas.append("generated_file_full_path vazio")
    elif not Path(caminho).exists():
        problemas.append("o arquivo devolvido nao existe: {}".format(caminho))
    elif Path(caminho).stat().st_size == 0:
        problemas.append("o arquivo devolvido esta vazio: {}".format(caminho))
    return problemas


def publica(gerado, raiz: Path, padrao: str, instrumento: str) -> Path:
    """
    O §11: renomear, mover e registrar são responsabilidade exclusiva do pipeline.

    Aqui isso é feito de verdade, numa pasta de saída própria, porque é a prova de
    que o plugin não depende do caminho onde escreveu nem do nome que deu.
    """
    origem = Path(gerado.generated_file_full_path)
    data = gerado.target_date
    diretorio = raiz / (padrao or "{instrument_lower}/{yyyy}/{mm}/{dd}").format(
        instrument_lower=str(instrumento).lower(),
        yyyy="{:04d}".format(data.year), mm="{:02d}".format(data.month),
        dd="{:02d}".format(data.day))
    diretorio.mkdir(parents=True, exist_ok=True)

    # nome final, como o pipeline o montaria: identidade científica e não o nome
    # interno do plugin, que o §9 diz não ter valor semântico
    final = diretorio / "{}_{}_{}_{}.{}".format(
        str(instrumento).lower(), gerado.scientific_product_key,
        data.strftime("%Y%m%d"), str(gerado.generated_file_format).lower(),
        origem.suffix.lstrip("."))
    shutil.move(str(origem), str(final))
    return final


# ----------------------------------------------------------------------- relato

def tamanho_legivel(bytes_: int) -> str:
    for unidade in ("B", "KB", "MB", "GB"):
        if bytes_ < 1024 or unidade == "GB":
            return "{:.1f} {}".format(bytes_, unidade)
        bytes_ /= 1024.0


def primeiras_horas(aceitos: list, quantas: int) -> list:
    """
    Recorta a entrada pelas primeiras horas de observação, não por contagem.

    O pipeline processa um dia; para rodar o executor sobre um pedaço, a unidade
    que faz sentido é a hora, porque o `.rbd` e o `.aux` da mesma hora têm de
    andar juntos — e eles têm o mesmo nome, só a extensão difere. Cortar por
    contagem de arquivo separaria o par, ou pegaria dois `.aux` e nenhum `.rbd`,
    porque eles ficam em subdiretório e a ordenação é por caminho.
    """
    if not quantas:
        return aceitos
    escolhidos = []
    for caminho in aceitos:
        if caminho.stem not in escolhidos:
            escolhidos.append(caminho.stem)
        if len(escolhidos) >= quantas:
            break
    conjunto = set(escolhidos)
    return [caminho for caminho in aceitos if caminho.stem in conjunto]


def executa(catalogo: dict, data_dir: Path, saida: Path, source_file: Path,
            limite: int = None, verboso: bool = True) -> dict:
    """O fluxo do §1, do começo ao fim, com relato do caminho."""
    instrumento = catalogo.get("instrument") or "?"
    bloco_plugin = catalogo.get("plugin") or {}
    entrypoint = bloco_plugin.get("entrypoint") or "build_instrument"
    exportacao = catalogo.get("export") or {}
    publicacao = catalogo.get("publication") or {}

    falhas = []
    def diz(texto=""):
        if verboso:
            print(texto)

    diz()
    diz("=" * 78)
    diz("  EXECUTOR DE REFERENCIA — o fluxo do §1 do guia, sobre o plugin do {}".format(
        instrumento))
    diz("=" * 78)
    if catalogo.get("_origem"):
        diz("  plano de execucao: {}".format(catalogo["_origem"]))
    diz()

    # ---------------------------------------------------------------- 1. plugin
    diz("  [1/6] carregando o plugin pelo caminho, sem import (§13.2)")
    diz("        source_file: {}".format(source_file))
    modulo, adapter = carrega_plugin(source_file, entrypoint)
    diz("        {}() devolveu {}".format(entrypoint, type(adapter).__name__))

    declarada = bloco_plugin.get("adapter_class_name")
    if declarada and declarada != type(adapter).__name__:
        falhas.append("o catalogo declara adapter_class_name {!r} e veio {!r}".format(
            declarada, type(adapter).__name__))

    # ------------------------------------------------------------------ 2. spec
    spec = monta_spec(catalogo, modulo)
    diz()
    diz("  [2/6] spec montado do bloco ingestion")
    diz("        time_field: {}   modos: {}".format(
        spec.time_field, spec.native_resolutions_by_mode or "(nenhum declarado)"))

    # ------------------------------------------------------------ 3. descoberta
    regras = (catalogo.get("ingestion") or {}).get("file_discovery") or {}
    aceitos, recusados = descobre_arquivos(data_dir, regras)
    aceitos = primeiras_horas(aceitos, limite)
    diz()
    diz("  [3/6] descoberta em {}".format(data_dir))
    diz("        {} aceitos, {} recusados (classificacao relatada, nada e movido)".format(
        len(aceitos), len(recusados)))
    for caminho in aceitos:
        diz("          + {}".format(caminho.name))
    for caminho, motivo in recusados[:5]:
        diz("          - {}  ({})".format(caminho.name, motivo))
    if len(recusados) > 5:
        diz("          - ... e {} outros".format(len(recusados) - 5))
    if not aceitos:
        raise SystemExit("nenhum arquivo aceito pela regra de descoberta")

    # -------------------------------------------------------------- 4. ingestão
    diz()
    diz("  [4/6] load_data por arquivo, e a volta pelo banco")
    por_modo = {}
    formato_invalido = []
    for caminho in aceitos:
        inicio = time.time()
        try:
            resultado = adapter.load_data(spec, str(caminho))
        except Exception as erro:                            # noqa: BLE001
            # Comportamento do pipeline, não falha do plugin: um arquivo cujo
            # formato o plugin recusa vai para a pasta que o catálogo chama de
            # `invalid_format_dir_name`. O que se exige da recusa é que ela diga o
            # motivo, porque é a mensagem que explica a pasta depois.
            formato_invalido.append((caminho, "{}: {}".format(
                type(erro).__name__, erro)))
            diz("        {:38s} -> {} ({})".format(
                caminho.name, spec.invalid_format_dir_name, erro))
            continue

        problemas = valida_resultado_da_ingestao(resultado, caminho)
        falhas.extend(problemas)

        modo = getattr(resultado, "acquisition_mode", "") or "DEFAULT"
        banco = saida / "banco" / "{}_{}.jsonl".format(modo.lower(), caminho.stem)
        recarregados = persiste_e_recarrega(
            resultado.data_records, banco, spec.time_field)

        anterior = resultado.data_records[0][spec.time_field]
        depois = recarregados[0][spec.time_field]
        por_modo.setdefault(modo, []).extend(recarregados)

        diz("        {:38s} modo {:9s} {:>9} registros  {:5.1f}s".format(
            caminho.name, modo, len(recarregados), time.time() - inicio))
        diz("          resolucao: {}   persistido em {} ({})".format(
            resultado.data_time_resolution, banco.name,
            tamanho_legivel(banco.stat().st_size)))
        if anterior != depois:
            diz("          o carimbo perdeu precisao na volta: {} -> {}".format(
                anterior.isoformat(), depois.isoformat()))
        for aviso in (getattr(resultado, "processing_warnings", None) or [])[:3]:
            diz("          aviso: {}".format(aviso))

    # Uma consulta ao banco devolveria os registros ordenados pelo campo temporal.
    # Sem isso, duas horas do mesmo modo chegariam na ordem em que os arquivos
    # foram varridos, e a janela deslizante da demodulação pressupõe ordem.
    for modo, lista in por_modo.items():
        lista.sort(key=lambda registro: (registro[spec.time_field],
                                         registro.get("HUSEC", 0)))

    if not por_modo:
        raise SystemExit(
            "nenhum arquivo foi ingerido: os {} aceitos pela descoberta foram todos "
            "recusados pelo plugin. Sem catalogo, a regra de descoberta esta vazia e "
            "aceita qualquer arquivo do diretorio; com o catalogo, o bloco "
            "file_discovery filtra antes.".format(len(aceitos)))

    # ------------------------------------------------------------ 5. exportação
    diz()
    diz("  [5/6] exportacao dirigida pelo plano, via getattr")
    fontes = {fonte.get("source_key"): fonte
              for fonte in (exportacao.get("raw_sources") or [])}
    gerados = []
    nao_se_aplica = []
    for produto in exportacao.get("products") or []:
        chave = produto.get("source_key")
        fonte = fontes.get(chave)
        if chave is not None and fonte is None:
            falhas.append("o produto {} aponta para source_key {!r}, que nao esta "
                          "em raw_sources".format(
                              produto.get("scientific_product_key"), chave))
            continue

        modo = (fonte or {}).get("mode")
        if modo is None and produto.get("_modos"):
            pedidos = produto["_modos"]
            faltando = [m for m in pedidos if m not in por_modo]
            if faltando:
                falhas.append("o produto pediu os modos {} e nao foi ingerido "
                              "nenhum arquivo de {}".format(
                                  "+".join(pedidos), ", ".join(faltando)))
                continue
            registros = [r for m in pedidos for r in por_modo[m]]
            modo = "+".join(pedidos)
        elif modo is None:
            # sem catálogo e sem modo pedido: tudo o que foi ingerido
            registros = [r for lista in por_modo.values() for r in lista]
        elif modo in por_modo:
            registros = por_modo[modo]
        else:
            falhas.append("o produto {} precisa do modo {!r} e nenhum arquivo "
                          "ingerido declarou esse modo (ingeridos: {})".format(
                              produto.get("scientific_product_key"), modo,
                              ", ".join(sorted(por_modo)) or "nenhum"))
            continue

        diz("        produto {}  ({} registros do modo {})".format(
            produto.get("scientific_product_key") or "(nao declarado)",
            len(registros), modo or "todos"))

        for tarefa in produto.get("tasks") or []:
            nome = tarefa.get("call")
            metodo = getattr(adapter, nome, None)
            if metodo is None:
                falhas.append("o catalogo manda chamar {!r} e o adapter nao tem esse "
                              "metodo".format(nome))
                diz("          {:42s} AUSENTE NO PLUGIN".format(nome))
                continue

            inicio = time.time()
            try:
                gerado = metodo(registros)
            except Exception as erro:                       # noqa: BLE001
                # O plugin distingue "este produto nao se aplica a esta entrada" de
                # "algo quebrou", e a distincao e util justamente aqui: um arquivo de
                # 441 amostras nao contem uma janela de 1 s, e nenhum produto de 1 s
                # pode sair dele. Isso nao e divergencia entre o plugin e o plano.
                #
                # A classe e reconhecida pelo nome porque este executor nao importa
                # nada do plugin: ele o carrega pelo caminho, como o core faz, e o
                # core tambem nao conhece as subclasses de excecao do instrumento.
                if type(erro).__name__ == "InsufficientRecords":
                    nao_se_aplica.append((nome, str(erro)))
                    diz("          {:42s} nao se aplica: {}".format(
                        nome, str(erro)[:80]))
                    continue
                falhas.append("{} levantou {}: {}".format(
                    nome, type(erro).__name__, erro))
                diz("          {:42s} {}: {}".format(
                    nome, type(erro).__name__, str(erro)[:90]))
                continue

            problemas = confronta_com_o_catalogo(gerado, produto, tarefa)
            falhas.extend("{}: {}".format(nome, p) for p in problemas)

            tamanho = Path(gerado.generated_file_full_path).stat().st_size
            final = publica(gerado, saida / "publicado",
                            publicacao.get("path_pattern"), instrumento)
            gerados.append((nome, gerado, final, tamanho))
            diz("          {:42s} {:>9}  {:5.1f}s  -> {}".format(
                nome, tamanho_legivel(tamanho), time.time() - inicio,
                final.relative_to(saida)))
            for problema in problemas:
                diz("            DIVERGENCIA: {}".format(problema))

    # ------------------------------------------------------------ 6. resultado
    diz()
    diz("  [6/6] resultado")
    diz("        {} arquivos publicados em {}".format(len(gerados), saida / "publicado"))
    if nao_se_aplica:
        diz("        {} produto(s) nao se aplicam a esta entrada:".format(
            len(nao_se_aplica)))
        for nome, motivo in nao_se_aplica:
            diz("          {}  ({})".format(nome, motivo))
    if formato_invalido:
        diz("        {} arquivo(s) roteado(s) para {} pelo plugin:".format(
            len(formato_invalido), spec.invalid_format_dir_name))
        for caminho, motivo in formato_invalido:
            diz("          {}  ({})".format(caminho.name, motivo))
    if falhas:
        diz()
        diz("        {} DIVERGENCIA(S):".format(len(falhas)))
        for falha in falhas:
            diz("          - {}".format(falha))
    else:
        diz("        nenhuma divergencia entre o plugin e o plano de execucao")
    diz()

    return {"gerados": gerados, "falhas": falhas, "por_modo": por_modo,
            "aceitos": aceitos, "recusados": recusados, "adapter": adapter,
            "formato_invalido": formato_invalido, "nao_se_aplica": nao_se_aplica}


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="pipeline_falso",
        description="Executa o fluxo do pipeline do MackSun sobre o plugin, "
                    "localmente, sem o MackSun.")
    parser.add_argument("--data-dir", default="Amostra",
                        help="Diretorio varrido pela regra de descoberta.")
    parser.add_argument("--saida", default="SaidaPipelineFalso",
                        help="Onde ficam o banco falso e os arquivos publicados.")
    parser.add_argument("--catalogo", default=None,
                        help="O instrument_catalog.json do suporte. Quando o do HATS "
                             "existir, e por aqui que ele entra.")
    parser.add_argument("--instrumento", default=None,
                        help="Qual documento usar, se o arquivo tiver mais de um.")
    parser.add_argument("--tarefas", default=None,
                        help="Nomes de metodo separados por virgula, para rodar sem "
                             "catalogo. Explicitamente ad hoc.")
    parser.add_argument("--source-file", default=None,
                        help="instrument.py do plugin. O padrao vem do catalogo, ou "
                             "e o do repositorio.")
    parser.add_argument("--extensoes", default=None,
                        help="Extensoes aceitas na descoberta, separadas por virgula. "
                             "Sem catalogo a regra e vazia e aceita tudo.")
    parser.add_argument("--limite-de-horas", type=int, default=None,
                        help="Roda sobre as primeiras N horas de observacao. O .rbd e "
                             "o .aux da mesma hora andam juntos.")
    args = parser.parse_args(argv)

    if args.catalogo:
        catalogo = carrega_catalogo(Path(args.catalogo), args.instrumento)
    elif args.tarefas:
        catalogo = catalogo_de_tarefas(
            [nome.strip() for nome in args.tarefas.split(",") if nome.strip()],
            args.instrumento or "HATS",
            [e.strip() for e in (args.extensoes or "").split(",") if e.strip()])
    else:
        raise SystemExit(
            "passe --catalogo (o documento do suporte) ou --tarefas (nomes de metodo "
            "na linha de comando).\n\nO catalogo do instrumento e documento da equipe "
            "de suporte: o §13 diz que nao e implementado pelo plugin e nao deve ser "
            "alterado pelo desenvolvedor. Este executor le o documento deles; nao "
            "escreve um.")

    source_file = (args.source_file
                   or (catalogo.get("plugin") or {}).get("source_file")
                   or str(REPO_ROOT / "plugins" / "hats" / "instrument.py"))
    if not Path(source_file).exists() and (catalogo.get("plugin") or {}).get("source_file"):
        # o catálogo aponta para /app/plugins/..., que é o caminho no container deles
        local = REPO_ROOT / "plugins" / Path(source_file).parent.name / "instrument.py"
        if local.exists():
            print("  o catalogo aponta para {} (caminho do container deles); usando {}".format(
                source_file, local))
            source_file = str(local)

    saida = Path(args.saida)
    if saida.exists():
        shutil.rmtree(saida)
    saida.mkdir(parents=True, exist_ok=True)

    resultado = executa(catalogo, Path(args.data_dir), saida,
                        Path(source_file), args.limite_de_horas)
    return 1 if resultado["falhas"] else 0


if __name__ == "__main__":
    sys.exit(main())
