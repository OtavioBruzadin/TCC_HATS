# TCC_HATS

Reimplementação do processamento de dados do telescópio solar HATS
(CRAAM/Mackenzie, instalado no OAFA, San Juan/AR).

O HATS observa o Sol em 15 THz (20 µm). O detector é uma célula Golay, que só
responde a sinal modulado: a radiação é picada por um chopper a 20 Hz, amostrada
a 1 kHz, e a amplitude em 20 Hz é extraída por software — um lock-in digital.

**O objetivo é um só: produzir exatamente as mesmas tabelas que o `HATS.py` do
CRAAM produz, byte a byte, mais rápido.**

Nada é acrescentado à saída. Sem colunas extras, sem arquivos extras, sem
relatórios. A organização é a deles.

## Os três comandos

```bash
make run
```
Roda o nosso. Grava as tabelas em `Saida/` e os relatórios em `Diagnostico/`.

```bash
make run-craam
```
Roda o do CRAAM. Grava em `SaidaCRAAM/`.

```bash
diff -r SaidaCRAAM Saida
```
Sai vazio. `make diff` faz os três de uma vez.

### Quanto, e não só se

O `diff` responde "igual ou diferente". Para o número que se cita num texto:

```bash
make erro
```

```
  2026-03-17T1800-rbd_cal.csv
  --------------------------------------------------------------------------
    coluna              linhas   diferentes          RMSE  erro abs máx  erro rel máx
    golay              3599307            0  0.000000e+00  0.000000e+00  0.000000e+00
    chopper            3599307            0  0.000000e+00  0.000000e+00  0.000000e+00
    temp_hics          3599307            0  0.000000e+00  0.000000e+00  0.000000e+00
    ...
  ==========================================================================
  3715201 linhas comparadas. Erro exatamente zero em todas as colunas.
```

Erro quadrático médio, erro absoluto e relativo máximos, viés e contagem de
valores divergentes, coluna a coluna. As tabelas são percorridas em fluxo, então
os 225 MB do `-rbd_cal.csv` não vão para a memória.

A ferramenta foi conferida nas duas pontas: dá zero entre saídas idênticas, e
recupera o valor certo quando um erro é injetado de propósito — 100 amplitudes
perturbadas em 3×10⁻⁵ produzem `erro rel máx = 3.000000e-05` e RMSE
1,05×10⁻⁴. Uma métrica que sempre dá zero não mede nada.

Os dois processam **a mesma hora**, para que as pastas sejam comparáveis. Escolha
com `DAY` e `HOUR`; para o dia inteiro há `make run-dia`, que não tem contraparte
do outro lado e por isso não entra no `diff`.

```bash
make diff DAY=2026-03-18 HOUR=2000
```

### Onde estão os dados

Uma vez, apontando para a pasta `data` do instrumento:

```bash
make config-data DATA="$HOME/.../Hats Data"
```

Isso grava `local.mk`, que não é versionado. Depois disso os comandos acima
funcionam sem mais nada. Alternativas: exportar `HATS_DATA_InputPath`, que é a
variável do manual do CRAAM, ou passar `DATA=` em cada comando.

**Não há queda silenciosa para a amostra.** Sem configuração, os comandos param e
dizem o que fazer. Um padrão que usasse os 1000 registros de exemplo faria uma
comparação passar sem provar nada — foi exatamente assim que duas divergências
reais ficaram escondidas por uma sessão inteira de trabalho.

### Experimentar sem os dados reais

```bash
make run-demo
make diff-demo
```

Usam `Amostra/`, com 1000 registros reais do instrumento remontados a partir de
CSV. Servem para ver o pipeline funcionando; **não servem como validação** — a
demodulação sai com 9 janelas em vez de ~112.000, e as horas que expuseram os
dois defeitos não estão cobertas.

## As tabelas

`Saida/` contém as mesmas três que o `toCSV()` da referência grava, e **nada
além disso** — é o que torna a comparação um `diff -r` limpo entre dois
diretórios.

| arquivo | colunas |
|---|---|
| `-deconv.csv` | `time,husec,amplitude` |
| `-rbd_cal.csv` | `golay,chopper,temp_hics,temp_env,temp_golay` |
| `-rbd_adcu.csv` | o apontamento — ver abaixo |

## Os diagnósticos

`Diagnostico/` recebe relatórios JSON que a referência não produz. Eles ficam
fora de `Saida/` de propósito: são uma leitura **sobre** os dados, não alteram
tabela nenhuma, e se estivessem junto quebrariam o `diff`.

| arquivo | o que traz |
|---|---|
| `-rbd.json` | estatística por canal, saltos em `sample` e `husec`, quantos registros a referência descartou, resumo da demodulação |
| `-aux.json` | quantos registros de apontamento estão defasados e por quanto tempo, mais as unidades que o XML declara errado |
| `-ws.json` | estação meteorológica: linhas rejeitadas e carimbos de tempo recuperados |

Exemplo do que eles dizem sobre o arquivo de teste:

```
rbd : 441 registros lidos, 559 descartados antes da hora
      saltos: sample=0 husec=0
      demodulação: 9 janelas, média 124.195919 mV
aux : 1000 registros, 234 defasados (23.4%), defasagem 1053.5 s
ws  : 17253 linhas, 1 rejeitada, 2 carimbos recuperados
```

Custam uma passada a mais sobre o arquivo. `--sem-diagnostico` desliga, e aí só
as tabelas são geradas.

## Desempenho

Uma hora de dados: 3.600.000 registros, 137 MB. Melhor de três execuções.

```bash
make bench-craam
```

| implementação | tempo | memória |
|---|---|---|
| `HATS.py` do CRAAM (numpy + binário C) | 1,11 s | 813 MB |
| pacote, backend stdlib | 4,29 s | 64 MB |
| **pacote, backend numpy** | **0,06 s** | **84 MB** |

Com numpy, **~18× mais rápido** usando **~10× menos memória**. Sem numpy é ~4×
mais lento, que é o preço de não ter dependência alguma — o pacote roda com a
biblioteca padrão e usa numpy só se ele estiver instalado.

### De onde vem o ganho

Não é "Python contra C". O `getFFT()` da referência grava o sinal calibrado e os
carimbos de tempo em dois arquivos temporários — cerca de 58 MB por hora —, lança
o binário `HATS_fft`, que relê tudo, calcula e regrava, e então o Python lê de
volta. Esse ida e volta em disco custa mais que a conta. Aqui tudo acontece em
memória.

No caminho sem dependências, o que rendeu foi abandonar o laço por registro em
favor de processamento por coluna: `zip(*Struct.iter_unpack(...))` transpõe cada
bloco em C e tira do interpretador cerca de quarenta operações por registro,
vezes 3,6 milhões.

## O que a fidelidade exigiu

Nenhum destes pontos é óbvio, e sem qualquer um deles o `diff` não zera.

**1. Descartar os registros anteriores à hora nominal.** A referência joga fora
tudo com `husec < hora × 36000000`. No arquivo `T1800` de 2026-03-17 são 559
registros. Isso desloca o início da janela deslizante e, como `559 = 17×32 + 15`,
a defasagem não é múltipla do passo: sem reproduzir o descarte, as duas grades de
janelas nunca coincidem.

**2. Reproduzir o off-by-one da recursão de Goertzel.** O `windowed_dft.c`
devolve `s[N-2]` e `s[N-3]` no lugar de `s[N-1]` e `s[N-2]`, encerrando um passo
antes do devido. Custa cerca de 0,003% na amplitude.

**3. Usar a recursão, não o produto interno equivalente.** Os dois dão o mesmo
número em precisão infinita, mas arredondam diferente e divergem no décimo
terceiro dígito. Medido: produto interno, 0 de 9 janelas idênticas; recursão
fiel, 9 de 9.

**4. Compilar o `HATS_fft` com `-ffp-contract=off`.** Sem isso o compilador funde
multiplicação e soma, muda o arredondamento e a saída deixa de ser reproduzível:
com FMA, 0 de 9 janelas batem; sem, 9 de 9.

Isso não é escolha arbitrária, e sim o que faz a compilação corresponder ao
binário real deles — ver a seção seguinte.

**5. Descartar os registros anteriores à hora nominal também no apontamento.**
O filtro aparece **duas vezes** no `HATS.py`, nas linhas 654 e 711 — uma no
`aux.from_file` e outra no `rbd.from_file`. É fácil implementar só o segundo.

No `.aux` o filtro tem de ser por registro, não por deslocamento inicial: o
`np.delete` da referência usa máscara booleana e remove um registro que case
esteja ele onde estiver.

**6. Truncar os carimbos de tempo como o `husec2dt()` da referência.** O cálculo
dos microssegundos passa por ponto flutuante: para o husec 648000643 o valor
exato seria 64300 µs, mas `643/1e4 = 0.0643` e `0.0643*1e6 = 64299.999999999993`,
que `int()` leva a 64299. A referência grava `.064299`.

**7. Gravar os floats no repr de round-trip mais curto** — `50.690450199999994`,
não `50.69045020`.

**8. Reproduzir a sobrescrita do `-rbd_adcu.csv`.** As linhas 368 e 387 do
`toCSV()` usam ambas `rootname+'-rbd_adcu.csv'`: a primeira grava o sinal bruto
do detector, a segunda grava o apontamento por cima. O conteúdo final desse
arquivo é o **apontamento**, e o sinal bruto não sobrevive à chamada. O resultado
é reproduzido; a escrita descartada não, já que o conteúdo final é o mesmo e
custaria centenas de MB por hora.

## Por que a validação em volume importa

O item 5 acima ficou de fora da implementação por uma sessão inteira de trabalho,
e nenhuma comparação o revelou — porque a hora usada em todas elas,
`2026-03-17T1800`, não tem nenhum registro de apontamento antes das 18:00. Batia
por coincidência.

A varredura expôs duas divergências que nenhuma comparação de uma hora só teria
encontrado — e a segunda apareceu justamente depois de corrigir a primeira e
declarar a hora 1800 idêntica.

A lição vale registrar: uma amostra pequena, ou uma única hora, não estabelece
equivalência. O que estabelece é varrer o conjunto inteiro e ver o `diff` calar
em todos.

## Validado sobre 32 horas de dados reais

Os três dias disponíveis, hora a hora, com os dois pipelines rodando de forma
independente e um `diff -r` entre as pastas de saída.

| | |
|---|---|
| horas comparadas | **32** |
| divergências | **0** |
| registros do detector | 100.200.805 |
| registros de apontamento | 92.680 |
| janelas demoduladas | ~3,1 milhões |
| dados lidos | 3,8 GB |

Três horas ficaram de fora: `2026-03-18` às 12, 13 e 14, onde o arquivo `.aux`
não existe nos dados. A referência levanta erro e não produz saída nessas horas,
então não há o que comparar. O pipeline daqui gera as duas tabelas que dependem
só do `.rbd` — uma diferença de comportamento, não de resultado.

### O que a varredura encontrou

Duas divergências reais, ambas invisíveis em qualquer comparação de uma hora só:

**O filtro de husec no apontamento.** Ele aparece duas vezes no `HATS.py`, nas
linhas 654 e 711, e só o segundo estava implementado. Dez das doze horas do dia
17 divergiam por isso.

**A formatação da coluna de tempo.** O pandas decide o formato pela coluna
inteira; o `str()` de um datetime decide valor a valor. Nas horas em que alguma
janela cai sobre um segundo exato, a referência escreve `.000000` e nós
omitíamos a fração.

As duas passaram despercebidas pelo mesmo motivo: a hora usada em todas as
comparações anteriores, `2026-03-17T1800`, não tem registro de apontamento antes
das 18:00 **e** nenhuma janela sobre segundo exato. Batia por coincidência, duas
vezes seguidas.

## Contra o binário que o CRAAM distribui

Compilar o fonte deles não é a mesma coisa que reproduzir o binário deles. Com as
flags que o cabeçalho do `HATS_fft.c` indica (`-Wall -g`), numa máquina arm64, o
resultado difere em **todas** as janelas — porque a FMA é baseline em ARM e o
compilador a usa por padrão.

O binário distribuído no `HATS_software.zip` é ELF Linux x86-64 e **não contém
nenhuma instrução FMA** (verificado por `objdump`: zero ocorrências de `vfmadd`
no binário inteiro), porque o gcc em x86-64 sem `-march` não as emite.

```bash
make verify-binario
```

Executa o binário original sob emulação x86-64 num container e confronta:

```
    instruções FMA: 0
    janelas: 9
    bit a bit iguais: 9/9

    A nossa implementação reproduz o binário distribuído pelo CRAAM,
    bit a bit, em todas as janelas.
```

Exige docker e o `HATS_software.zip` (padrão: `~/Downloads/`, ou passe
`ZIP=/caminho`). É a evidência mais forte do projeto: não estamos comparando com
uma compilação nossa do código deles, e sim com o executável que eles publicaram.

## O pacote

```
hats/
  constants.py     grandezas fixas do instrumento
  timebase.py      husec para datetime, incluindo o porte fiel do husec2dt
  schema.py        formato dos registros, lido dos XML do CRAAM
  records.py       iteração sobre os binários
  calibration.py   AD7770 e conversão para unidades físicas
  demodulation.py  Goertzel com janela flat-top
  rbd.py           leitura e demodulação do sinal do detector
  discovery.py     descoberta dos arquivos no disco
  pipeline.py      orquestração e escrita das tabelas
  backends.py      escolha entre numpy e stdlib
  cli.py           linha de comando
```

Sem dependências. Se numpy estiver instalado, o backend acelerado é escolhido
sozinho e o resultado é o mesmo — igualdade bit a bit exigida por teste.

## Estrutura de dados

A mesma que o manual do CRAAM descreve, e a que o instrumento entrega:

```
data/
  hats-2026-03-17T1800.rbd
  hats-2026-03-17T1900.rbd
  ...
  aux/
    hats-2026-03-17T1800.aux
    hats-2026-03-17.ws
```

Os `.rbd` ficam todos juntos; o dia vem do nome do arquivo, não de pasta. Não há
nível por dia — ele seria redundante, já que a convenção é
`hats-YYYY-MM-DDTHH00.rbd`, com os minutos sempre em 00 e um par RBD/AUX novo a
cada hora.

### Variáveis de ambiente

As mesmas do manual. Quem configurou a máquina seguindo o CRAAM roda este
pipeline sem passar flag nenhuma:

```bash
export HATS_DATA_InputPath="$HOME/.../Hats Data"
export HATSXMLPATH="$PWD/XMLTables"
```

```bash
python3 hats_report.py --day 2026-03-17 --hour 1800
```

| variável | equivale a |
|---|---|
| `HATS_DATA_InputPath` | `--data-dir` |
| `HATSXMLPATH` | `--xml-dir` |

As flags, quando dadas, têm precedência. `HATS_FFTProgram` não se aplica: aqui a
demodulação roda em processo, sem lançar binário externo.

## Projeto irmão

As correções dos defeitos apurados durante a validação — manter os registros
anteriores à hora nominal, usar a forma correta da recursão, converter as
unidades erradas do apontamento, marcar os registros defasados — estão em
`TCC_HATS_corrigido`. Elas mudam o resultado, e por isso não cabem aqui.

## Fontes

Código de referência: https://github.com/guigue/CRAAM-Instruments (diretório
`HATS/`), cópia em `Docs/upstream/`. O `HATS.py` de lá está em
`2026-04-17T0902BST`.
