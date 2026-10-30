# Plugins

Implementações de instrumento para o pipeline de séries temporais do **MackSun**,
seguindo o *Guia de implementação — instrumentos de séries temporais* (rev. 1,
29.04.2026) mantido pelo suporte do projeto.

Hoje há um: `hats/`.

## O que é um plugin, nesta arquitetura

O pipeline do MackSun é agnóstico em relação aos instrumentos: não conhece formato
de arquivo e não contém lógica científica. A regra central, nas palavras do guia:

> Pipeline = executa · JSON = define o que executar · Plugin = implementa como
> executar

A comunicação acontece **exclusivamente por contratos**. O plugin implementa dois:
`TimeSeriesIngestionAdapter`, que converte um arquivo do instrumento em registros,
e `TimeSeriesExportAdapter`, que gera os arquivos publicáveis a partir dos
registros que voltam do banco.

O fluxo completo, do §1 do guia:

```
arquivo bruto no filesystem
  → load_data(spec, file_full_path)
  → TimeSeriesLoadDataResult
  → o pipeline valida e persiste
  → o pipeline carrega os dados brutos de volta
  → Generate*(raw_records)
  → TimeSeriesSpecialistGeneratedFile
  → o pipeline renomeia, publica e registra
```

## `external_contracts/` — o que é deles e o que é nosso

Os cinco arquivos de contrato são **cópias** dos que o MackSun distribuiu junto com
o guia, no `poemas_plugin_macksun.zip`. Não são reimplementações: são os arquivos
deles, linha por linha, com o mínimo de desvio necessário para que importem fora do
container do pipeline.

Os desvios abaixo foram conferidos por comparação direta com o zip original. **São
estes e nenhum outro.**

| Arquivo | Desvio |
|---|---|
| todos os cinco | quebras de linha normalizadas de CRLF para LF |
| todos os cinco | um bloco de comentário no topo, declarando o desvio do próprio arquivo |
| `time_series_load_data_result.py` | `from core.contracts.errors import ValidationError` → `from external_contracts.errors import ValidationError` |
| `time_series_specialist_generated_file.py` | o mesmo import de `ValidationError` |
| `time_series_export_adapter.py` | três imports de `core.external_contracts.time_series_ingestion.*` trazidos para o caminho plano do diretório |

Fora disso, o conteúdo é idêntico — incluindo a linha `# path: core/...` que o
arquivo deles traz, que foi preservada, e incluindo dois imports que o `Protocol`
não usa mas que estão no original.

### `errors.py` é nosso

É o único arquivo deste diretório que não vem do zip. Dentro do container do
MackSun os contratos importam `ValidationError` de `core.contracts.errors`, pacote
que não existe fora dele — e sem um substituto nenhum contrato poderia ser
importado nem testado.

A tentativa de importar o original vem primeiro, então **em produção vale a classe
do core**. Isso importa: o pipeline provavelmente trata essa exceção por tipo, e um
substituto que ficasse na frente quebraria esse tratamento em silêncio.

### Por que não foram reescritos

O guia diz que a configuração do instrumento é mantida pelo suporte e não deve ser
alterada pelo desenvolvedor. O mesmo espírito vale para os contratos: eles são a
fronteira do sistema deles. Reescrevê-los "mais limpos" criaria uma divergência que
só apareceria no dia da integração, e `tests/test_contracts.py` existe para fixar o
comportamento de cada regra de validação deles — 23 testes, um por regra.

## `hats/` — o plugin do HATS

Estrutura recomendada pelo §12 do guia, com os nomes correspondentes:

| Arquivo | Papel |
|---|---|
| `instrument.py` | o que o catálogo carrega; expõe `build_instrument()` |
| `hats_binary_reader.py` | leitura dos binários e resolução das tabelas de formato |
| `hats_ingestion.py` | `load_data`, com a verificação de consistência temporal |
| `hats_export.py` | os produtos, em FITS e CSV |
| `hats_level1.py` | a grade temporal uniforme e as duas interpolações |
| `hats_utils.py` | metadados, formatação e os valores pendentes de definição |

O fluxo interno é o que o guia recomenda:
`arquivo → reader → ingestion → registros → export`.

### Requisito de ambiente

O plugin depende do pacote Python `hats/`, na raiz deste repositório — a engenharia
do instrumento: leitura dos `.rbd` e `.aux`, calibração do AD7770 e demodulação,
toda confrontada com o código que o CRAAM distribui.

O requisito é do próprio catálogo, no bloco `plugin.dependencies`:

> O instrumento executa no mesmo ambiente Python do core. Todas as dependências
> importadas no arquivo do instrumento devem estar instaladas no ambiente do job.

O pacote é **Python puro, sem nenhuma dependência externa**, então atender isso é
colocar o diretório no ambiente do job — instalado, ou num diretório do
`PYTHONPATH`. Não há nada a empacotar e nada foi vendorizado.

Carregado pelo caminho, como o §13.2 manda, o `instrument.py` confere essa
dependência na primeira linha e falha com uma mensagem que diz o que falta e onde
procurou, em vez de um `ModuleNotFoundError` seco três módulos abaixo.

A geração de FITS exige `astropy` no ambiente do job. O CSV não depende dela.

### Valores pendentes de definição

`scientific_product_key`, `product_resolution` e os nomes de método são
**declaração do catálogo**, e o catálogo é documento do suporte. Enquanto o do HATS
não existir, os valores usados são provisórios e ficam todos em `hats_utils.py`,
num bloco marcado como pendente — com teste conferindo que não se repetem em
nenhum outro arquivo, para que trocá-los seja mexer num lugar só.

O mesmo vale para as duas decisões do nível 1 que a mensagem do criador do
instrumento não resolve. Elas ficam no mesmo arquivo, e **o arquivo gerado declara
qual caminho foi aplicado** — para que ninguém descubra a escolha lendo o
código-fonte.

## Como exercitar o plugin sem o MackSun

`tools/pipeline_falso.py` faz o fluxo do §1 localmente: descobre os arquivos pela
regra do catálogo, carrega o plugin pelo caminho, chama a ingestão, persiste os
registros e os lê de volta, executa os métodos declarados e publica o resultado.

```bash
make pipeline-falso-demo     # sobre a amostra de 1000 registros
make pipeline-falso          # sobre uma hora de dado real
```

Ele **não** escreve catálogo. Lê o documento do suporte quando ele existir:

```bash
make pipeline-falso-catalogo CATALOGO=caminho/instrument_catalog.json
```

A partir daí ele é o teste de aceitação do documento: nome de método que não
existe, resolução que não casa com o que o plugin devolve, `source_key` apontando
para fonte não declarada — tudo isso aparece ali, e não em produção.

## Documentos gerados a partir do código

```bash
make descreve        # a descrição do instrumento, para quem mantém o catálogo
make dicionario      # o dicionário de dados dos arquivos publicados
make dicionario-pdf  # idem, mais HTML e PDF para anexar
```

A estrutura dos registros e as tabelas de campos são **extraídas da saída do
código**, não escritas à mão, justamente para não divergirem do que o plugin
produz.
