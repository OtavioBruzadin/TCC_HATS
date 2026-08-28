# TCC_HATS

Processamento e análise dos dados do telescópio solar HATS
(CRAAM/Mackenzie, instalado no OAFA, San Juan/AR).

O HATS observa o Sol em 15 THz (20 µm). O detector é uma célula Golay, que só
responde a sinal modulado: a radiação é picada por um chopper a 20 Hz, amostrada
a 1 kHz, e a amplitude em 20 Hz é extraída por software — um lock-in digital.

**Este pipeline reproduz o software original do CRAAM bit a bit.** É essa a
afirmação que ele sustenta: uma reimplementação independente, reorganizada em
módulos, com testes e cerca de dez vezes mais rápida, chegando exatamente ao
mesmo resultado do código de referência.

```bash
make diff
```

```
diff -r Reports/craam Reports/nosso/craam-csv
----------------------------------------------------------------
(sem diferenças — as tabelas são idênticas byte a byte)
```

Não é só a série demodulada: são os **três arquivos** que o `toCSV()` do
`HATS.py` grava, com os mesmos nomes, as mesmas colunas na mesma ordem, os
mesmos valores e a mesma formatação. Mesmo SHA-256 nos três.

| arquivo | conteúdo | linhas |
|---|---|---|
| `-deconv.csv` | `time,husec,amplitude` | 9 |
| `-rbd_cal.csv` | `golay,chopper,temp_hics,temp_env,temp_golay` | 441 |
| `-rbd_adcu.csv` | o apontamento — ver abaixo | 1000 |

O `make run` grava esses três em `Reports/craam-csv/`, ao lado dos relatórios
próprios do pacote.

### Detalhes que a reprodução exigiu

Três coisas não saem de graça, e valem registro porque nenhuma é óbvia.

**O carimbo de tempo trunca.** O `husec2dt()` da referência calcula os
microssegundos em ponto flutuante: para o husec 648000643 o valor exato seria
64300 µs, mas `643/1e4 = 0.0643` e `0.0643*1e6 = 64299.999999999993`, que `int()`
leva a 64299. A referência grava `.064299`, então
`timebase.craam_datetime()` reproduz esse caminho em vez de calcular certo.

**O nome de arquivo está repetido no `toCSV()`.** As linhas 368 e 387 usam ambas
`rootname+'-rbd_adcu.csv'`: primeiro grava o sinal bruto do detector, depois o
apontamento por cima. O conteúdo final desse arquivo é o **apontamento**, e os
dados brutos não sobrevivem à chamada. Isso é reproduzido; o que não é
reproduzido é a escrita descartada, já que o resultado é o mesmo e custaria
centenas de MB por hora.

**Os floats saem no repr de round-trip mais curto**, que é o que o Python produz
nativamente — `50.690450199999994`, não `50.69045020`.

## Fidelidade, e o que ela custa

Reproduzir exatamente significa reproduzir também os defeitos. Duas
consequências, ambas deliberadas:

- os registros anteriores à hora nominal são **descartados**, como na referência,
  embora `sample` e `husec` sigam contínuos neles;
- a recursão de Goertzel encerra um passo antes do devido, reproduzindo o
  off-by-one do `windowed_dft.c`, o que custa cerca de 0,003% na amplitude.

**As correções desses dois pontos estão no projeto irmão `TCC_HATS_corrigido`**,
separado justamente porque mudam o resultado. Só faz sentido afirmar o que muda
com elas depois de ter estabelecido que a reimplementação é fiel.

O que **não** é sacrificado, porque não altera nenhum valor que a referência
produz: a conversão da ascensão reta de horas para graus e das taxas de arcsec/s
para graus/s, a marcação dos registros defasados do apontamento, o reparo dos
carimbos corrompidos da estação meteorológica, e a estatística de arquivo
inteiro. Todas são aditivas — acrescentam colunas e campos.

---

## Como rodar

Sem dependência nenhuma. Se numpy estiver instalado, o backend acelerado é
escolhido sozinho; os resultados são os mesmos.

```bash
make run
```

`make` sozinho lista todos os alvos.

| comando | o que faz |
|---|---|
| `make run` | processa o `Data/` e gera os relatórios |
| `make run-full` | idem, incluindo o CSV do sinal bruto de 1 kHz |
| `make test` | suíte completa: 53 testes, sem precisar de dados nem de numpy |
| `make clean` | apaga o `Reports/` |

---|---|---|
| registros anteriores à hora nominal | descartados, como na referência | mantidos — são dados bons |
| recursão de Goertzel | com o off-by-one do `windowed_dft.c` | forma correta |
| saída | idêntica à referência | numericamente melhor, e por isso diferente |

**As correções de unidade do apontamento e a marcação dos registros defasados não
dependem do modo** — valem sempre. Elas são aditivas: acrescentam colunas e campos
sem tocar em nenhum valor que a referência produz, então não conflitam com a
reprodução bit a bit.

O modo usado fica registrado no `summary.json` e em cada relatório de hora, junto
com a contagem de registros descartados e a marcação `goertzel_c_legacy`.

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

### Rodar os dois pipelines separados

```bash
make run-ambos
```

Cada um grava na sua própria pasta, sem misturar:

```
Reports/
├── nosso/     este projeto
└── craam/     HATS.py do CRAAM
```

Ou um de cada vez, com `make run-nosso` e `make run-craam`.

**As duas saídas não são comparáveis linha a linha.** O `HATS.py` descarta os
registros anteriores à hora nominal — 559 no arquivo `T1800` de 2026-03-17 —, o
que desloca o início da janela deslizante. Como `559 = 17×32 + 15`, a defasagem
não é múltipla do passo, e as duas grades de janelas ficam separadas por 15
amostras: nenhuma janela nossa cai no mesmo instante que uma dele.

No modo `craam`, que é o padrão, o descarte é reproduzido e as janelas coincidem:

```
       husec               CRAAM               NOSSO     dif rel
   648000643     122.59035467314     122.59299471498    2.15e-05
   648000963     124.87169590452     124.87007551462    1.30e-05
   648001283     124.85157177433     124.85255332264    7.86e-06
```

As 9 janelas batem, com erro relativo máximo de 2,762×10⁻⁵ — o off-by-one do
Goertzel, e nada mais.

Fora dessa comparação o descarte não se justifica: os registros descartados têm
`sample` e `husec` contínuos, são dados bons.

### Gerar as duas tabelas e comparar com `diff`

Três comandos independentes. Cada um roda um pipeline sozinho e grava a série
demodulada num formato único.

```bash
make csv-craam
```

```bash
make csv-nosso
```

```bash
diff Reports/diff/craam.csv Reports/diff/nosso.csv
```

**O diff sai vazio.** Os dois arquivos são idênticos bit a bit, mesmo SHA-256,
em precisão total. Ou `make diff`, que faz os três de uma vez.

O formato é `husec,amplitude_mV` nos dois. Isso é necessário: os CSV nativos de
cada pipeline têm colunas e formatos de tempo diferentes —
`time,husec,amplitude` contra `husec,datetime_utc,amplitude_mV` —, então o `diff`
acusaria toda linha como divergente sem que número nenhum tivesse mudado.

#### O que foi preciso para o diff zerar

Quatro condições, cada uma apurada por medição.

**1. Reproduzir o descarte de registros.** O `HATS.py` joga fora os registros
anteriores à hora nominal, o que desloca o início da janela deslizante. No
arquivo `T1800` de 2026-03-17 são 559 registros, e `559 = 17×32 + 15`: a
defasagem não é múltipla do passo, então sem isso as duas grades de janelas nunca
coincidem e o `diff` compararia instantes diferentes.

**2. Reproduzir o off-by-one do Goertzel.** O `windowed_dft.c` devolve `s[N-2]` e
`s[N-3]` no lugar de `s[N-1]` e `s[N-2]`. Isso equivale a somar apenas as `N-1`
primeiras amostras da janela, ainda dividindo por `N` — equivalência verificada
contra um porte fiel da recursão, com erro de 2×10⁻¹⁴.

**3. Usar a própria recursão, não o produto interno equivalente.** Os dois dão o
mesmo número em precisão infinita, mas arredondam diferente e divergem no décimo
terceiro dígito. Medido: produto interno, 0 de 9 janelas idênticas; recursão
fiel, 9 de 9. Por isso o modo `craam` abre mão da otimização do produto interno
na demodulação — e só nela.

**4. Compilar o `HATS_fft` com `-ffp-contract=off`.** Sem isso o compilador funde
multiplicação e soma, muda o arredondamento e a saída deixa de ser reproduzível:
com FMA, 0 de 9 janelas batem; sem, 9 de 9. O `setup_reference.sh` já compila
assim.

#### Comparando com o Goertzel correto

As correções estão no projeto irmão `TCC_HATS_corrigido`, e o `make diff` de lá
mostra o quanto elas mudam o resultado. Aí o `diff` mostra as 9 linhas
divergindo, e `DECIMALS` diz em quantas casas os dois concordam:

| | linhas divergentes |
|---|---|
| `DECIMALS=6` | 9 de 9 |
| `DECIMALS=3` | 8 de 9 |
| `DECIMALS=2` | 1 de 9 |

A diferença média é 0,0018 mV num sinal de 124 mV — cerca de 1,5 vezes o passo de
quantização do conversor A/D, ou seja abaixo do que o instrumento distingue. O
sinal dela alterna, o que é assinatura de artefato numérico e não de erro
sistemático.

Outro dia ou outra hora: `make diff DAY=2026-03-18 HOUR=2000`.

### Rodar o código original do CRAAM

```bash
make craam-shell
```

Abre uma sessão Python interativa com o `HATS.py` deles carregado e o objeto já
pronto na variável `h`:

```
  HATS.py 2026-04-17T0902BST — código original do CRAAM
  ----------------------------------------------------------
  h.rbd.rData         441 registros brutos
  h.rbd.cData         441 registros calibrados
  h.rbd.Deconv          9 janelas demoduladas
  h.aux.Data         1000 registros de apontamento
  descartados         559 anteriores à hora nominal
```

Outro dia ou outra hora:

```bash
make craam-shell DAY=2026-03-18 HOUR=2000
```

O script resolve três detalhes que fazem o `HATS.py` falhar quando configurado
pela documentação:

- A variável do XML chama-se `HATSXMLPATH`. O cabeçalho do próprio `HATS.py`
  manda exportar `HATSXMLTABLES`, que o código nunca lê.
- `HATS_DATA_InputPath` aponta para a pasta **do dia**, não para o `Data/`. Os
  caminhos são montados como `InputPath + arquivo.rbd` e
  `InputPath + 'aux/' + arquivo.aux`.
- `HATS_FFTProgram` precisa do caminho completo do binário, compilado nesta
  máquina — o do zip é ELF Linux x86-64.

Para montar o ambiente à mão, sem o script:

```bash
export HATSXMLPATH=$PWD/XMLTables HATS_DATA_InputPath=$PWD/Data/2026-03-17 HATS_WS_InputPath=$PWD/Data/2026-03-17/aux HATS_FFTProgram=$PWD/Docs/upstream/HATS_fft PYTHONPATH=$PWD/Docs/upstream
```

```bash
.refvenv/bin/python -c "import HATS; h = HATS.hats('2026-03-17 1800'); print(h.rbd.Deconv['amplitude'][:5])"
```

Note que `h.aux.Data['right_ascension']` sai em **horas**, apesar de o docstring
dizer graus — é o achado descrito em "Correções aplicadas".

### Comparar com os protótipos

```bash
make compare-versions
```

Roda as versões de `prototypes/` sobre os mesmos dados e confronta os CSV byte a
byte e os JSON chave a chave, quantificando as diferenças numéricas quando
existem.

### Desempenho

Medido sobre um `.rbd` de hora inteira (3.600.000 registros, 137 MB), MacBook arm64,
Python 3.9.6, melhor de 3 execuções.

```bash
make bench-craam
```

| implementação | tempo | memória |
|---|---|---|
| `HATS.py` do CRAAM (numpy + binário C) | 1,16 s | 896 MB |
| pacote, backend stdlib | 6,14 s | 65 MB |
| **pacote, backend numpy** | **0,11 s** | **93 MB** |

Com numpy o pacote é **~10× mais rápido que a referência** usando **~10× menos
memória** — e produzindo saída idêntica bit a bit. Sem numpy é ~5× mais lento,
que é o preço de não ter dependência nenhuma.

`make bench` mede só os backends, sem precisar do ambiente de referência.

### A recursão também é vetorizada

A reprodução bit a bit exige a recursão de Goertzel, não o produto interno — a
ordem das operações muda o arredondamento. Isso parecia condenar o pipeline a ser
lento, e por um momento foi: numpy dava 6,21 s contra 6,41 s do stdlib, ou seja
ganho nenhum.

A saída foi observar que a recursão é sequencial **dentro** de uma janela, mas as
janelas são **independentes entre si**. Vetorizando através delas, a ordem das
operações dentro de cada janela fica preservada — que é o que garante o bit a bit
— e 112 mil laços de 128 passos viram 128 operações sobre vetores de 112 mil
elementos. O resultado é 0,11 s, verificado idêntico ao caminho stdlib em todos
os bits, por teste.

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
