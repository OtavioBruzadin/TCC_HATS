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

Outro dia ou outra hora: `make run DAY=2026-03-18`, `make diff DAY=2026-03-18 HOUR=2000`.

Antes da primeira comparação, uma vez: `make setup-reference`.

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
com FMA, 0 de 9 janelas batem; sem, 9 de 9. O `setup_reference.sh` já compila
assim.

**5. Truncar os carimbos de tempo como o `husec2dt()` da referência.** O cálculo
dos microssegundos passa por ponto flutuante: para o husec 648000643 o valor
exato seria 64300 µs, mas `643/1e4 = 0.0643` e `0.0643*1e6 = 64299.999999999993`,
que `int()` leva a 64299. A referência grava `.064299`.

**6. Gravar os floats no repr de round-trip mais curto** — `50.690450199999994`,
não `50.69045020`.

**7. Reproduzir a sobrescrita do `-rbd_adcu.csv`.** As linhas 368 e 387 do
`toCSV()` usam ambas `rootname+'-rbd_adcu.csv'`: a primeira grava o sinal bruto
do detector, a segunda grava o apontamento por cima. O conteúdo final desse
arquivo é o **apontamento**, e o sinal bruto não sobrevive à chamada. O resultado
é reproduzido; a escrita descartada não, já que o conteúdo final é o mesmo e
custaria centenas de MB por hora.

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

`Data/` não é versionada:

```
Data/
└── 2026-03-17/
    ├── hats-2026-03-17T1800.rbd
    └── aux/
        └── hats-2026-03-17T1800.aux
```

Há uma amostra reconstruída de 1000 registros em `Data/2026-03-17/`; veja o
`LEIA-ME.txt` de lá. O `Data/` completo está no Drive:
https://drive.google.com/drive/folders/1_aWg-CdfVP4UcG06CKtlhWi68MCRzkFz?usp=sharing

## Projeto irmão

As correções dos defeitos apurados durante a validação — manter os registros
anteriores à hora nominal, usar a forma correta da recursão, converter as
unidades erradas do apontamento, marcar os registros defasados — estão em
`TCC_HATS_corrigido`. Elas mudam o resultado, e por isso não cabem aqui.

## Fontes

Código de referência: https://github.com/guigue/CRAAM-Instruments (diretório
`HATS/`), cópia em `Docs/upstream/`. O `HATS.py` de lá está em
`2026-04-17T0902BST`.
