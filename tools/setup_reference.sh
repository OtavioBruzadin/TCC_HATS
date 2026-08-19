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

if [ ! -d "$REFVENV" ]; then
    echo "--> criando virtualenv"
    python3 -m venv "$REFVENV"
fi

echo "--> instalando numpy, scipy, pandas e astropy"
"$REFVENV/bin/pip" install --quiet --disable-pip-version-check numpy scipy pandas astropy

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

(cd "$BUILD" && cc -O2 -I. windowed_dft.c HATS_fft.c -lm -o HATS_fft 2>/dev/null)
cp "$BUILD/HATS_fft" "$UPSTREAM/HATS_fft"
chmod +x "$UPSTREAM/HATS_fft"

echo
echo "Pronto."
echo "  interpretador : $REFVENV/bin/python"
echo "  binário       : $UPSTREAM/HATS_fft"
echo
echo "Agora dá para rodar:"
echo "  make compare-craam"
echo "  make side-by-side"
echo "  make bench-craam"
