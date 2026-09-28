#!/bin/sh
#
# Monta o ambiente necessário para comparar com o pipeline original do CRAAM.
#
#   tools/setup_reference.sh            # cria .refvenv/ e compila o HATS_fft
#   REFVENV=/outro/caminho tools/setup_reference.sh
#
# Faz duas coisas:
#
#   1. Um virtualenv separado com numpy, scipy, pandas e astropy, que são as
#      dependências do HATS.py de referência. O pacote hats/ continua sem
#      dependência nenhuma; este ambiente serve só para executar o código deles.
#
#   2. Compila o HATS_fft a partir de Docs/upstream/. O binário distribuído no
#      HATS_software.zip é ELF Linux x86-64 e não roda em macOS nem em ARM.
#
# O HATS_fft.c só chama goertzel_amplitude(), que não usa FFTW — a windowed_dft()
# usa, mas nunca é chamada. Por isso um stub de fftw3.h basta para linkar, e o
# caminho de código exercitado continua sendo o original, sem alteração.

set -e

ROOT=$(cd "$(dirname "$0")/.." && pwd)
REFVENV=${REFVENV:-"$ROOT/.refvenv"}
UPSTREAM="$ROOT/Docs/upstream"

echo "==> Ambiente de referência em $REFVENV"

if [ ! -d "$UPSTREAM" ]; then
    echo "ERRO: $UPSTREAM não existe."
    echo "      Ele guarda o código do CRAAM e não é versionado (o .gitignore"
    echo "      ignora /Docs/). Veja a seção 'Fontes upstream' do README."
    exit 1
fi

# Escolha do interpretador. O pacote hats/ roda em 3.9, mas os contratos do pipeline
# do MackSun declaram @dataclass(frozen=True, slots=True), e o argumento slots só
# existe a partir do 3.10 — em 3.9 aqueles arquivos não podem nem ser importados. O
# python3 do sistema no macOS é 3.9, então a busca começa no 3.10, que é o mínimo, e
# sobe a partir dele.
#
#   PYBIN=/caminho/para/python tools/setup_reference.sh    escolhe à mão
if [ -z "${PYBIN:-}" ]; then
    for candidate in python3.10 python3.11 python3.12 python3.13 python3.14 python3; do
        if command -v "$candidate" >/dev/null 2>&1 &&
           "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
            PYBIN=$candidate
            break
        fi
    done
fi

if [ -z "${PYBIN:-}" ]; then
    echo "ERRO: nenhum Python 3.10 ou mais novo encontrado."
    echo "      No macOS: brew install python@3.10"
    echo "      Ou aponte um à mão: PYBIN=/caminho/para/python tools/setup_reference.sh"
    exit 1
fi

echo "--> interpretador: $PYBIN ($("$PYBIN" -V 2>&1))"

if [ ! -d "$REFVENV" ]; then
    echo "--> criando virtualenv"
    "$PYBIN" -m venv "$REFVENV"
elif ! "$REFVENV/bin/python" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
    echo "--> virtualenv existente é anterior ao 3.10; refazendo"
    rm -rf "$REFVENV"
    "$PYBIN" -m venv "$REFVENV"
fi

# Versões fixas, e não as mais recentes, por dois motivos.
#
# O primeiro é obrigatório: o HATS.py do CRAAM NÃO RODA em numpy 2. Na linha 1066
# ele faz np.delete(whole_data, flag, 0) com uma máscara booleana vinda de
# whole_data['husec']. O dtype é montado com (nome, tipo, dim) e dim vale 1; o
# numpy 1.x colapsava esse 1 para um campo escalar, e o numpy 2 o preserva como
# subarray de shape (1,). O campo passa a ter shape (N, 1), a máscara fica 2-D e
# o np.delete a recusa:
#
#   ValueError: boolean array argument obj to delete must be one dimensional
#
# O segundo é de método: a afirmação central deste projeto é igualdade bit a bit
# com a saída da referência, e ela foi verificada com estas versões. Um ambiente
# que flutua com o índice do PyPI não sustenta essa afirmação.
echo "--> instalando numpy, scipy, pandas e astropy (versões fixas)"
"$REFVENV/bin/pip" install --quiet --disable-pip-version-check \
    "numpy==1.26.4" "scipy==1.13.1" "pandas==2.3.3" "astropy==6.0.1"

echo "--> compilando o HATS_fft"
BUILD=$(mktemp -d)
trap 'rm -rf "$BUILD"' EXIT
cp "$UPSTREAM/HATS_fft.c" "$UPSTREAM/HATS_fft.h" \
   "$UPSTREAM/windowed_dft.c" "$UPSTREAM/windowed_dft.h" "$BUILD/"

cat > "$BUILD/fftw3.h" <<'STUB'
/* Stub de fftw3.h. O HATS_fft.c nunca chama a windowed_dft(), que é a única
   função que usa FFTW; isto existe apenas para satisfazer o compilador. */
#ifndef FFTW3_STUB_H
#define FFTW3_STUB_H
#include <complex.h>
typedef double _Complex fftw_complex;
typedef void * fftw_plan;
#define FFTW_ESTIMATE 64u
static inline fftw_plan fftw_plan_dft_r2c_1d(int n, double *in, fftw_complex *out, unsigned flags)
{ (void)n; (void)in; (void)out; (void)flags; return (fftw_plan)0; }
static inline void fftw_execute(fftw_plan p) { (void)p; }
static inline void fftw_destroy_plan(fftw_plan p) { (void)p; }
#endif
STUB

# -ffp-contract=off desliga a fusão de multiplicação e soma.
#
# Não é uma licença poética: é o que faz esta compilação corresponder ao binário
# que o CRAAM distribui. O HATS_fft do HATS_software.zip é ELF Linux x86-64 e
# não contém nenhuma instrução FMA — verificado por objdump, zero ocorrências de
# vfmadd no binário inteiro —, porque o gcc em x86-64 sem -march não emite FMA.
# Em arm64 a FMA é baseline e o compilador a usa por padrão, o que altera o
# arredondamento.
#
# Verificado executando o binário original sob emulação x86-64: a saída dele bate
# bit a bit com esta compilação, 9 de 9 janelas. Compilando com as flags que o
# cabeçalho do HATS_fft.c indica (-Wall -g, sem esta), batem 0 de 9.
(cd "$BUILD" && cc -O2 -ffp-contract=off -I. windowed_dft.c HATS_fft.c -lm -o HATS_fft 2>/dev/null)
cp "$BUILD/HATS_fft" "$UPSTREAM/HATS_fft"
chmod +x "$UPSTREAM/HATS_fft"

echo
echo "Pronto."
echo "  interpretador : $REFVENV/bin/python"
echo "  binário       : $UPSTREAM/HATS_fft"
echo
echo "Agora dá para rodar:"
echo "  make run-craam     roda o código do CRAAM"
echo "  make diff          roda os dois e compara"
echo "  make bench-craam   compara o desempenho"
echo "  make craam-shell   sessão Python com o HATS.py deles"
