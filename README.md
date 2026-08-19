# TCC_HATS

Processamento e análise dos arquivos gerados pelo instrumento HATS
(High Altitude THz Solar telescope — CRAAM/Mackenzie, instalado no OAFA, San Juan/AR).

O HATS observa o Sol em 15 THz (20 µm). O detector é uma célula Golay, que só
responde a sinal modulado: a radiação é picada por um chopper a 20 Hz, amostrada
a 1 kHz, e a amplitude em 20 Hz é extraída por software — um lock-in digital.

---

## Como rodar

Sem dependência nenhuma. Se numpy estiver instalado, o backend acelerado é
escolhido sozinho; os resultados são os mesmos.

```bash
make run
```

Equivale a `python3 hats_report.py --export-csv`. `make` sozinho lista todos os
alvos.

| comando | o que faz |
|---|---|
| `make run` | processa o `Data/` e gera os relatórios |
| `make run-full` | idem, incluindo o CSV do sinal bruto de 1 kHz |
| `make test` | suíte completa: 48 testes, sem precisar de dados nem de numpy |
| `make clean` | apaga o `Reports/` |

Para controle fino, chame o script direto:

```bash
python3 hats_report.py --day 2026-03-17 --export-csv --backend stdlib
```

| flag | efeito |
|---|---|
| `--day 2026-03-17` | processa só um dia |
| `--export-csv` | CSV do apontamento, da estação e da amplitude demodulada |
| `--export-rbd-csv` | também o CSV do sinal bruto (ver "Saídas") |
| `--csv-limit 1000` | limita as linhas dos CSV |
| `--record-limit 100000` | lê só os N primeiros registros de cada binário |
| `--no-demod` | pula a demodulação de 20 Hz |
| `--fft-bin-mode exact` | usa o bin fracionário em vez do `floor()` do `HATS_fft.c` |
| `--backend stdlib` | força a biblioteca padrão mesmo com numpy instalado |
| `--backends` | mostra o que está disponível |

---

## Testar e comparar

### Testes

```bash
make test
```

48 testes, cerca de 0,2 s. Não precisam do `Data/` nem de numpy: cada teste monta
os binários de que precisa num diretório temporário. Os que exigem numpy são
pulados automaticamente quando ele não está instalado.

Cobrem a decodificação do AD7770 nas bordas do bit de sinal, a equivalência exata
da calibração afim, o formatador rápido de tempo contra o caminho com `datetime`,
o bin e a contagem de janelas do `HATS_fft.c`, a recuperação de uma amplitude
conhecida, a concordância entre a recursão de Goertzel e o produto interno, o
demodulador deslizante alimentado em blocos irregulares, a separação dos registros
defasados, a medição da defasagem, a recuperação de carimbos com byte de controle,
e a CLI de ponta a ponta nos dois backends.

### Comparar com o pipeline do CRAAM

Uma vez, para montar o ambiente de referência:

```bash
make setup-reference
```

Isso cria `.refvenv/` com numpy, scipy, pandas e astropy — dependências do
`HATS.py` deles, não do nosso — e compila o `HATS_fft` a partir de
`Docs/upstream/`, já que o binário do zip é ELF Linux x86-64.

```bash
make compare-craam
```

Confronta campo a campo: valores brutos, calibrados, apontamento e demodulação.

```bash
make side-by-side
```

Imprime uma linha de cada pipeline lado a lado, marcando `=` onde bate e a
diferença absoluta e relativa onde não bate. Útil para conferir a olho.

Os dois aceitam `DAY=` e `HOUR=`:

```bash
make compare-craam DAY=2026-03-18 HOUR=2000
```

### Comparar com os protótipos

```bash
make compare-versions
```

Roda as versões de `prototypes/` sobre os mesmos dados e confronta os CSV byte a
byte e os JSON chave a chave, quantificando as diferenças numéricas quando
existem.

### Desempenho

```bash
make bench
```

Gera uma hora sintética de dados — 3,6 milhões de registros, 137 MB — e mede os
backends. Com o ambiente de referência montado:

```bash
make bench-craam
```

acrescenta o `HATS.py` do CRAAM à mesma tabela:

```
 implementação                         segundos   memória MB    janelas    amplitude média
 -----------------------------------------------------------------------------------------
 pacote hats, backend stdlib               4.28           50     112496          99.018669
 pacote hats, backend numpy                0.18          131     112496          99.018669
 HATS.py do CRAAM 2026-04-17T0902BST       1.07          878     112496          99.018289
```

Para medir sobre um arquivo real em vez do sintético:

```bash
python3 tools/benchmark.py --rbd Data/2026-03-17/hats-2026-03-17T1800.rbd
```

---

## Estrutura do projeto

```
TCC_HATS/
├── hats_report.py          entrada
├── hats/                   o pacote
├── tools/
│   ├── compare_with_reference.py   confronta com o HATS.py do CRAAM
│   └── side_by_side.py             imprime uma linha de cada, lado a lado
├── prototypes/             versões anteriores, mantidas como registro
├── Docs/upstream/          código do CRAAM, para referência
├── XMLTables/              descrição do formato dos registros
├── Data/                   não versionado
└── Reports/                não versionado
```

### O pacote

Cada módulo responde por uma coisa, e a ordem abaixo é mais ou menos a ordem em
que os dados atravessam o sistema:

| módulo | responsabilidade |
|---|---|
| `constants.py` | grandezas fixas do instrumento e códigos de operação |
| `timebase.py` | conversões entre husec e datetime, com os formatadores rápidos |
| `schema.py` | formato dos registros, lido dos XML; correções de unidade do AUX |
| `records.py` | iteração sobre os binários, em registros ou em colunas |
| `calibration.py` | decodificação do AD7770 e conversão para unidades físicas |
| `statistics.py` | acumuladores em streaming e ajuste linear |
| `demodulation.py` | janela flat-top, bin de análise, demodulador deslizante |
| `rbd.py` | análise do sinal do detector — as duas implementações |
| `pointing.py` | análise do apontamento, unidades e registros defasados |
| `weather.py` | leitura e validação dos arquivos da estação |
| `discovery.py` | estrutura de pastas e descoberta dos arquivos |
| `exporters.py` | escrita de CSV e JSON |
| `reports.py` | montagem dos relatórios e dos resumos |
| `backends.py` | escolha entre o caminho numpy e o stdlib |
| `cli.py` | interface de linha de comando |

Onde há duas implementações da mesma coisa, elas ficam **no mesmo módulo**, lado a
lado — `rbd.analyse_stdlib` e `rbd.analyse_numpy`, `exporters.export_rbd_csv` e
`exporters.export_rbd_csv_numpy`. É mais fácil manter as duas coerentes vendo uma
ao lado da outra do que em árvores paralelas. Quem escolhe é o `backends.py`, e só
ele sabe que existe essa dualidade.

A pasta `Data/` **não é versionada** e deve seguir este layout, com os `.aux` e `.ws`
num subdiretório `aux/`, como o CRAAM organiza:

```
Data/
└── 2026-03-17/
    ├── hats-2026-03-17T1800.rbd
    └── aux/
        ├── hats-2026-03-17T1800.aux
        └── hats-2026-03-17.ws
```

Há uma amostra reconstruída de 1000 registros em `Data/2026-03-17/` para os testes
rodarem sem depender do download — veja o `LEIA-ME.txt` de lá. O `Data/` completo
está no Drive:
https://drive.google.com/drive/folders/1_aWg-CdfVP4UcG06CKtlhWi68MCRzkFz?usp=sharing

---

## Saídas

### CSV

| arquivo | linhas por hora | conteúdo |
|---|---|---|
| `__deconv.csv` | ~112.000 | husec, datetime, amplitude demodulada em mV |
| `__aux.csv` | ~3.420 | apontamento, mais as colunas corrigidas e `record_valid` |
| `__ws.csv` | ~17.280/dia | tempo validado, estação, temperatura, umidade, pressão |
| `__rbd.csv` | ~3.600.000 | sinal bruto de 1 kHz — **só com `--export-rbd-csv`** |

O CSV do RBD é opt-in porque custa ~24 s e ~767 MB por hora de dados, contra 0,4 s
da análise inteira. Um arquivo de 3,6 milhões de linhas do sinal cru não se abre em
planilha nenhuma; para análise o que serve é o `deconv.csv`. Ele existe como
ferramenta de inspeção, e `--csv-limit` continua valendo.

Quando pedido, é usado um exportador dedicado (`exporters.export_rbd_csv_numpy`,
com fallback para `exporters.export_rbd_csv`). A saída é **byte-idêntica** à do caminho stdlib; o que muda é
como chega lá:

| | s/hora |
|---|---|
| exportador stdlib | ~53 |
| exportador dedicado (numpy) | ~24 |

O ganho veio quase todo dos timestamps: construir um `datetime` por registro custava
22,3 s/hora só na coluna do husec, e a formatação por aritmética inteira faz o mesmo
em 3,1 s. O que sobra — ~6 s de `str()` nas colunas float e ~11 s de `writerows` — é
serialização de texto, e não dá para reduzir sem mudar a saída.

Todos os CSV são gravados com `encoding="utf-8"` explícito, para não depender do
locale do sistema.

### JSON

Um arquivo por objeto, e os agregadores apontam para eles em vez de copiá-los:

```
Reports/
├── summary.json                          <- aponta para os day_report
└── json/
    ├── 2026-03-17__day_report.json       <- aponta para os relatórios da hora
    ├── 2026-03-17__1800__rbd_report.json <- completo
    ├── 2026-03-17__1800__aux_report.json <- completo
    └── 2026-03-17__ws_report.json        <- completo
```

O `day_report` e o `summary` carregam um resumo útil de cada filho — contagem de
registros, integridade, janelas, amplitude média, registros defasados, defasagem
estimada — mais o campo `report_file` com o nome do arquivo completo.

Antes, o mesmo conteúdo era gravado três vezes: o relatório da hora, embutido inteiro
no `day_report`, embutido inteiro no `summary`. Com um dia de 10 horas o `summary`
passava de 1 MB, e processando um mês crescia para dezenas de MB de dados repetidos.
Na amostra atual, `summary.json` caiu de 37.517 para 1.653 bytes.

## Formatos

**`.rbd`** — 38 bytes por registro, 1 kHz (≈3,6 M registros/hora). O layout vem do
`HATSDataFormat.xml`, não está no código. Os canais analógicos chegam do AD7770 com
24 bits significativos dentro de um inteiro de 4 bytes, sinal no bit 23.

**`.aux`** — 80 bytes por registro, ≈1 registro/segundo. Ponteria do telescópio vinda
do `getPos`/TheSkyX.

**`.ws`** — ASCII, uma linha a cada 5 s, formato `tempo,0R2,Ta=..C,Ua=..P,Pa=..H`.

---

## Correções aplicadas nesta versão

Verificadas contra efemérides solares independentes nos dias 2026-03-17/18/19.

**1. Unidades erradas no `HATSAuxFormat.xml`.** O XML declara graus, mas os dados
estão em outras unidades:

| campo | declarado | real |
|---|---|---|
| `right_ascension` | degrees | **hours** (fator 15) |
| `ra_rate` | degrees/s | **arcsec/s** |
| `dec_rate` | degrees/s | **arcsec/s** |

Os campos originais são preservados; versões convertidas são adicionadas como
`right_ascension_deg`, `ra_rate_deg_s`, `dec_rate_deg_s`.

**2. Registros `.aux` defasados.** Entre 12% e 23% dos registros trazem `jd == 0` e
uma solução de ponteria coerente porém **antiga** (1053,5 s ≈ 17,6 min, defasagem
constante nos três dias, σ = 0,04 s), enquanto o `husec` está correto. São marcados
com `record_valid = False`, excluídos de todas as estatísticas e contabilizados no
relatório, junto com a defasagem estimada.

Isso importa: quase todos os registros com `opmode` 7 e 8 (varreduras em declinação
e ascensão reta) caem nesse conjunto. Analisar varreduras sem filtrar casa flag de
scan real com coordenadas de 17 minutos antes.

**3. Timestamps da estação meteorológica.** Os arquivos brutos contêm linhas com um
byte `0x7f` colado antes do timestamp. Agora são limpos e validados; linhas
irrecuperáveis são descartadas e contadas.

**4. Demodulação em 20 Hz.** Implementada — é o produto científico do instrumento, e
não existia nas versões anteriores. Goertzel com janela flat-top (ISO 18431-1),
janela de 128 amostras, passo de 32, saída a 31,25 Hz. Espelha o `HATS_fft.c`.

**5. Estatísticas e integridade de arquivo inteiro.** As versões anteriores
amostravam 10 registros de 3,6 milhões e não calculavam nada. Agora há min/max/média/
desvio por canal, contagem de saltos em `sample` e `husec`, e contagem de registros
anteriores à hora nominal do arquivo.

---

## Notas sobre a demodulação

**Bin de frequência.** O `HATS_fft.c` usa `floor(20·128/1000) = 2`, ou seja
15,625 Hz e não 20 Hz. A janela flat-top tem lóbulo principal largo e absorve quase
todo o erro, mas sobra um *scalloping* dependente de fase. Medido com seno sintético
de 100 mV:

| modo | média | desvio | erro |
|---|---|---|---|
| `reference` (bin 2) | 99,018 mV | 0,104 | −0,98% |
| `exact` (bin 2,56) | 99,223 mV | 0,0016 | −0,78% |

O padrão é `reference`, para permitir comparação direta com a saída do CRAAM.
Em sinal sintético monocromático, `exact` reduz a dispersão em ~65×.

Em dado real o quadro é outro: o sinal picado não é monocromático (o chopper mede
20,02 Hz e há energia em 19, 21 e 22 Hz), então o bin exato responde mais estreito e
acompanha a variação real da amplitude em vez de suavizá-la. Na hora de 2026-03-17
T1800 a dispersão sobe de 1,03 para 2,56 mV. Nenhum dos dois está errado — medem
coisas diferentes, e vale dizer qual foi usado ao reportar resultado.

O viés residual de −0,78% é intrínseco à normalização da flat-top em N=128 e está
presente também no código C original. É sistemático e linear (verificado em 50 e
100 mV), então não afeta razões nem variações relativas.

**Diferenças conhecidas em relação ao `HATS.py` de referência:**

- O `HATS.py` descarta registros com `husec < hora×36000000`. Aqui eles são mantidos
  e apenas contados — no arquivo `T1800` de 2026-03-17 são 559 registros, com
  `sample` e `husec` perfeitamente contínuos, ou seja, dados bons.
- O `windowed_dft.c` encerra a recursão de Goertzel um passo antes do devido (usa
  `s[N-2]` e `s[N-3]` em vez de `s[N-1]` e `s[N-2]`). A diferença medida contra uma
  DFT direta é de 0,003%; aqui foi usada a forma correta.
- O número de janelas segue a fórmula do C, `(N - window + 1) // steps`, que descarta
  a última janela válida. Mantido para as saídas terem o mesmo comprimento.

---

## Fontes upstream

Código de referência: https://github.com/guigue/CRAAM-Instruments (diretório `HATS/`).
Wiki: `HATSpy: User Manual V0.01` e `HATS Pointing Model`.

**Atenção à versão.** O `HATS_software.zip` traz `HATS.py` em `2025-10-17T1145BST`;
o repositório está em `2026-04-17T0902BST`. A revisão de 2026-04-17 corrige o
`extract_scans()`, que antes retornava só a última varredura da série **e usava os
códigos de `opmode` trocados**. O mapeamento correto é:

| opmode | significado |
|---|---|
| 0 | tracking |
| 7 | varredura em ascensão reta |
| 8 | varredura em declinação |
| 10 | skydip |

Isso não é verificável a partir dos dados de 2026-03: praticamente todo registro com
`opmode` 7 ou 8 é um dos defasados (61 de 62), então a excursão real em RA e Dec
durante a varredura não aparece. Seguimos o upstream.

**Unidades: o manual repete os erros do XML.** A wiki descreve `ra_rate`/`dec_rate`
como degrees/second e `ms` como "milliseconds since 0 UT". Medido, as taxas estão em
arcsec/s e `ms` é o milissegundo dentro do segundo (`sec` é unix time, não segundos
desde 0 UT). A documentação e o XML concordam entre si e discordam do dado.

A wiki **não** documenta: códigos de opmode, o procedimento de demodulação, o skydip,
constantes de calibração, nem o `extract_scans`. Para isso, a fonte é o código.


## Desempenho

Medido sobre um `.rbd` de hora inteira (3.600.000 registros, 137 MB), MacBook arm64,
Python 3.9.6, melhor de 3 execuções.

| | tempo | memória de pico |
|---|---|---|
| `HATS.py` de referência (numpy + binário C) | 1,08 s | 828 MB |
| pacote `hats`, backend stdlib | 4,8 s | 34 MB |
| **pacote `hats`, backend numpy** | **0,13 s** | **140 MB** |

O backend numpy é **~8,3× mais rápido que a referência** usando **~6,5× menos memória**. O caminho
stdlib, em Python puro, é ~4,4× mais lento que a referência mas ~24× mais leve.

Numa primeira execução com cache de página frio fica em torno de 0,26 s, o que
ainda é ~4× mais rápido que a referência.

### De onde vem o ganho sobre o CRAAM

Não é "Python contra C". O `getFFT()` do `HATS.py` grava o sinal calibrado e os husec
em dois arquivos temporários (~58 MB por hora), lança o binário `HATS_fft`, que relê
tudo, calcula e regrava, e então o Python lê de volta. Esse vai-e-volta em disco custa
mais que a conta. O v5 monta as janelas sobrepostas com `sliding_window_view` e troca
a projeção por uma multiplicação matriz-vetor em BLAS, tudo em memória.

### Otimizações do caminho stdlib

O gargalo não era o Goertzel — era o laço por registro, 20 s contra 5 s.

1. **Processamento por coluna.** `zip(*Struct.iter_unpack(...))` transpõe cada bloco
   em C, tirando ~40 operações interpretadas por registro do interpretador. 20 s → 5,6 s.
2. **Decodificação AD7770 sem ramo nem chamada de função:**
   `(v & 0xFFFFFF) - ((v & 0x800000) << 1)` dentro de uma list comprehension. ~2,5×.
3. **Estatística calibrada derivada analiticamente.** A conversão é afim, então
   min/max/média/desvio se transformam exatamente a partir dos valores em ADCu.
4. **Integridade em C:** `list(map(operator.sub, ...)).count(1)` e
   `sum(map(limiar.__gt__, coluna))`.
5. **Demodulação como produto interno** com a janela pré-multiplicada pelos twiddles,
   via `sum(map(operator.mul, ...))`, no lugar da recursão de Goertzel. ~3×.
6. **Demodulação em streaming**, com descarte do prefixo consumido: memória constante.
7. **Bloco de 16384 registros** (4,3 s / 34 MB contra 6,2 s / 151 MB a 131072).

O caminho numpy usa bloco de 262144, onde o custo por chamada é o que domina.

## Validação contra o código de referência do CRAAM

A demodulação foi verificada contra o `HATS_fft.c` original, compilado do fonte
(o binário distribuído no `HATS_software.zip` é ELF Linux x86-64). O `HATS_fft.c`
só chama `goertzel_amplitude()`, que não usa FFTW; a `windowed_dft()`, que usa,
nunca é chamada — então a compilação foi feita com um stub de `fftw3.h` apenas para
satisfazer o linker. O caminho de código exercitado é o original, sem alteração.

Entrada: os mesmos 1000 registros de `2026-03-17T1800`, sinal Golay já calibrado em
mV, gravados em `hats_data_rbd.bin` / `hats_husec.bin` exatamente como o
`HATS.py getFFT()` faz.

| | janelas | husec | erro relativo máx. |
|---|---|---|---|
| v4 `--fft-bin-mode reference` | 27 = 27 | idênticos | **3,2 × 10⁻⁵** |
| v4 `--fft-bin-mode exact` | 27 = 27 | idênticos | 2,1 × 10⁻² |

O resíduo de 3×10⁻⁵ no modo `reference` é **integralmente** o encerramento antecipado
da recursão de Goertzel no `windowed_dft.c`. Reimplementando esse mesmo off-by-one em
Python, a concordância com o binário C cai para **3 × 10⁻¹⁴**, o limite da precisão
de ponto flutuante.

Ou seja: a diferença entre este pipeline e o do CRAAM é conhecida, medida, e vale
0,003% — abaixo de qualquer relevância física para o instrumento.

### Conferindo a olho nu

`tools/side_by_side.py` imprime no console uma linha de cada pipeline, campo a campo,
para inspeção visual rápida:

```bash
/tmp/refvenv/bin/python tools/side_by_side.py
```

Mostra um registro bruto, o mesmo calibrado, uma janela demodulada e um registro de
apontamento, marcando cada campo com `=` quando bate e com a diferença absoluta e
relativa quando não bate. `--record N` escolhe qual registro exibir — vale usar um
índice dentro de uma varredura (por exemplo `--record 300`) para ver um registro
defasado sendo marcado com `record_valid = False`.

Roda direto sobre `Data/2026-03-17`, que traz uma amostra reconstruída de 1000
registros (veja `Data/2026-03-17/LEIA-ME.txt`). Com o `Data/` completo do Drive,
funciona igual.

### Reproduzindo a comparação

`tools/compare_with_reference.py` roda os dois pipelines sobre o mesmo par de
arquivos e reporta as diferenças campo a campo. Precisa de um interpretador com
numpy/scipy/pandas/astropy, porque o `HATS.py` de referência depende deles — o
projeto em si continua sem dependências.

```bash
python3 -m venv /tmp/refvenv && /tmp/refvenv/bin/pip install numpy scipy pandas astropy
```

```bash
/tmp/refvenv/bin/python tools/compare_with_reference.py --day-dir Data/2026-03-17 --date 2026-03-17 --hour 1800
```

Requer o binário `HATS_fft` em `Docs/upstream/` (ou via `--fft-program`); veja
`Docs/upstream/README.md` para compilar. `--json-out` grava o resultado estruturado.

### Contra o `HATS.py` completo (2026-04-17)

Além do binário C, o pipeline foi comparado com o `HATS.py` de referência rodando de
verdade, num virtualenv separado com numpy/scipy/pandas/astropy. O projeto do TCC
continua sem dependências; o venv existe só para executar o código do CRAAM.

Mesmo arquivo `.rbd` e `.aux` de entrada, `hats-2026-03-17T1800`:

| comparação | resultado |
|---|---|
| `rData`, todos os 9 campos (ADCu) | **diferença exatamente 0** |
| `cData`, todos os campos calibrados | **diferença exatamente 0** |
| `aux.Data`, valores brutos | **diferença exatamente 0** |
| `Deconv`, mesma entrada de 441 amostras | 9 = 9 janelas, husec idênticos, erro rel. máx. **2,8 × 10⁻⁵** |

A decodificação e a calibração são idênticas bit a bit. A única divergência numérica
em todo o pipeline é o off-by-one do Goertzel, já caracterizado acima.

**Diferença de contagem, esperada e documentada:** o `HATS.py` reportou
`N_Records_Deleted: 559` e ficou com 441 registros dos 1000, produzindo 9 janelas de
demodulação em vez das 27 que este projeto produz sobre o arquivo inteiro. São os
registros anteriores à hora nominal, que aqui são mantidos e contados em
`integrity.records_before_nominal_hour`.

**Confirmação da unidade de RA:** o `HATS.py` entrega
`aux.Data['right_ascension'][0] = 23.826897` — o mesmo valor bruto que este projeto lê,
em horas, enquanto o docstring dele diz "degrees". Convertido: 357,403450°, contra
357,3905° das efemérides.

### Equivalência entre os backends

Sobre a hora inteira, 3.600.000 registros e 112.496 janelas, ou seja com muitas
fronteiras de bloco no streaming:

| | resultado |
|---|---|
| `total_records`, contagens de integridade | **idênticos** |
| número de janelas e todos os `husec` | **idênticos** |
| amplitude demodulada | erro relativo máx. **9,1 × 10⁻¹⁶** |
| estatísticas por canal | erro relativo máx. **6,6 × 10⁻⁹** |

Pela CLI, os CSV de `rbd`, `aux` e `ws` saem **byte-idênticos** entre os dois
backends; o `deconv.csv` difere no máximo 5,7 × 10⁻¹⁴ mV.

Os 6,6 × 10⁻⁹ das estatísticas são ordem de soma: a variância é calculada como
E[x²] − E[x]², que cancela catastroficamente, e numpy soma aos pares enquanto o
caminho stdlib soma em sequência. Sobre 3,6 milhões de amostras isso é o esperado, e
o valor do numpy é o mais preciso dos dois.

O backend numpy também foi comparado diretamente com o `HATS.py`:
zero em `rData`/`cData`/`aux` e 2,76 × 10⁻⁵ no `Deconv`.

```bash
/tmp/refvenv/bin/python tools/compare_with_reference.py --backend numpy --day-dir Data/2026-03-17 --date 2026-03-17 --hour 1800
```

### Regressão dos protótipos para o pacote

Do v3 para o v4, o CSV do RBD saiu **byte-idêntico**: as correções não tocaram no
que já estava certo. No AUX, as únicas divergências em colunas comuns foram as 234
linhas de `pointing_valid` que passaram a ser `False`. Na WS, 2 timestamps deixaram
de sair com o byte `0x7f`.

Do v5 para o pacote `hats/`, os **quatro CSV saem byte-idênticos** e os relatórios
JSON só diferem no texto de duas notas explicativas, que foram traduzidas. Todos os
números — estatísticas, integridade, demodulação — são iguais. O refactor foi
reorganização, não reescrita.


## Dependências

O pacote `hats/` não tem nenhuma: só a biblioteca padrão do Python 3.

Quando numpy está instalado, `backends.py` escolhe o caminho acelerado sozinho. Não
há funcionalidade exclusiva dele — a diferença é só velocidade.

`tools/compare_with_reference.py` precisa de numpy, scipy, pandas e astropy, porque
executa o `HATS.py` de referência do CRAAM.
