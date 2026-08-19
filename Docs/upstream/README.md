# Código upstream do CRAAM (referência, não modificar)

Cópia do software original do HATS, mantida aqui só para consulta e para validar a
saída deste projeto. **Nada aqui é executado pelo pipeline do TCC.**

| arquivo | origem | versão |
|---|---|---|
| `HATS.py` | https://github.com/guigue/CRAAM-Instruments `HATS/HATS.py` (baixado 2026-08-18) | `2026-04-17T0902BST` |
| `HATSTools.py`, `OAFA.py` | `HATS_software.zip` | 2025-10-09 / — |
| `HATS_fft.c`, `HATS_fft.h`, `windowed_dft.c`, `windowed_dft.h` | `HATS_software.zip` | 2021-12-17 |
| `getPos.h` | `HATS_software.zip` | — |

O `HATS.py` aqui é a versão **do repositório**, mais nova que a do zip
(`2025-10-17T1145BST`). A única diferença entre as duas é o `extract_scans()`:
códigos de `opmode` trocados (7 = ascensão reta,
8 = declinação), retorno virou lista em vez de dicionário sobrescrito, e o
`update()` passou para dentro do `if`, que antes reaproveitava arrays da iteração
anterior quando uma varredura vinha vazia.

## Compilando o HATS_fft

O binário distribuído no zip é ELF Linux x86-64. Para rodar em macOS/arm64 é preciso
compilar. O `HATS_fft.c` só chama `goertzel_amplitude()`, que não usa FFTW — a
`windowed_dft()` usa, mas nunca é chamada — então um stub de `fftw3.h` basta:

```
cc -O2 -I. windowed_dft.c HATS_fft.c -lm -o HATS_fft
```

## Bugs abertos no upstream (verificados em 2026-04-17)

Relevantes para quem for comparar resultados:

- `right_ascension` documentado como degrees, dado em **hours**.
- `ra_rate`/`dec_rate` documentados como degrees/s, dados em **arcsec/s**.
- `sec` documentado como "seconds since 0 UT", é **unix time**; `ms` documentado como
  "milliseconds since 0 UT", é o **milissegundo dentro do segundo**.
- Nenhum tratamento de registros `.aux` com `jd == 0`.
- `np.zero` (sem o "s") em `contiguo()` e `seqLims()`.
- `h` global indefinido em `rbd.from_file()`.
- `deleted_records` em `aux.from_file()` compara com um array vazio.
- `__add__` concatena `Hour` no lugar do campo certo em 5 dos 6 ramos.
- `toCSV()` grava o aux por cima do CSV do raw.
- `getFFT()` usa nomes de arquivo fixos no diretório corrente.
- `SkyDip()` faz `pop()` na lista durante iteração indexada.
- O XML de formato é escolhido pelo nome, nunca pelos atributos `InitialDate`/`FinalDate`.
