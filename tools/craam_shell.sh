#!/bin/sh
#
# Abre uma sessão Python interativa com o HATS.py original do CRAAM carregado.
#
#   tools/craam_shell.sh                 # 2026-03-17 18:00 UT
#   tools/craam_shell.sh 2026-03-18 2000
#
# O objeto já vem pronto na variável `h`. Para carregar outra hora sem sair:
#
#   >>> g = HATS.hats('2026-03-18 2000')
#
# Três detalhes do HATS.py que este script resolve:
#
#   1. A variável de ambiente do XML chama-se HATSXMLPATH. O cabeçalho do
#      próprio HATS.py manda exportar HATSXMLTABLES, que o código não lê.
#
#   2. HATS_DATA_InputPath aponta para a pasta DO DIA, não para a Data/. O
#      HATS.py monta os caminhos como InputPath + arquivo.rbd e
#      InputPath + 'aux/' + arquivo.aux.
#
#   3. HATS_FFTProgram precisa do caminho completo do binário, que tem de ser
#      compilado nesta máquina: o do HATS_software.zip é ELF Linux x86-64.

set -e

ROOT=$(cd "$(dirname "$0")/.." && pwd)
DAY=${1:-2026-03-17}
HOUR=${2:-1800}
REFVENV=${REFVENV:-"$ROOT/.refvenv"}

if [ ! -x "$REFVENV/bin/python" ]; then
    echo "Ambiente de referência não encontrado em $REFVENV."
    echo "Rode:  make setup-reference"
    exit 1
fi

if [ ! -x "$ROOT/Docs/upstream/HATS_fft" ]; then
    echo "HATS_fft não compilado."
    echo "Rode:  make setup-reference"
    exit 1
fi

DATA=${HATS_DATA_InputPath:-"$ROOT/Data"}
if [ ! -f "$DATA/hats-${DAY}T${HOUR}.rbd" ]; then
    echo "Não encontrei hats-${DAY}T${HOUR}.rbd em $DATA"
    exit 1
fi

HATSXMLPATH=${HATSXMLPATH:-"$ROOT/XMLTables"}
HATS_DATA_InputPath="$DATA"
HATS_WS_InputPath="$DATA/aux"
HATS_FFTProgram="$ROOT/Docs/upstream/HATS_fft"
PYTHONPATH="$ROOT/Docs/upstream"
export HATSXMLPATH HATS_DATA_InputPath HATS_WS_InputPath HATS_FFTProgram PYTHONPATH

PREAMBLE=$(mktemp)
trap 'rm -f "$PREAMBLE"' EXIT
cat > "$PREAMBLE" <<PYEOF
import HATS
import numpy as np

h = HATS.hats('$DAY $HOUR')

print()
print('  HATS.py {} — codigo original do CRAAM'.format(HATS.__version__))
print('  ' + '-' * 58)
print('  h.rbd.rData    {:>8} registros brutos'.format(h.rbd.rData.shape[0]))
print('  h.rbd.cData    {:>8} registros calibrados'.format(h.rbd.cData.shape[0]))
print('  h.rbd.Deconv   {:>8} janelas demoduladas'.format(h.rbd.Deconv.shape[0]))
print('  h.aux.Data     {:>8} registros de apontamento'.format(h.aux.Data.shape[0]))
print('  descartados    {:>8} anteriores a hora nominal'.format(
      h.rbd.MetaData.get('N_Records_Deleted', 0)))
print()
print('  Experimente:')
print('    h.rbd.rData.dtype.names')
print('    h.rbd.cData[\'golay\'][:5]')
print('    h.rbd.Deconv[\'amplitude\'][:5]')
print('    h.aux.Data[\'right_ascension\'][0]      # em HORAS, apesar do docstring')
print('    h.check()                             # estatisticas e saltos')
print('    thats, fig = h.plot()                 # precisa de display')
print('    ws = HATS.ws(\'$DAY\')                 # estacao meteorologica')
print()
PYEOF

exec "$REFVENV/bin/python" -i "$PREAMBLE"
