#!/bin/sh
#
# Gera as tabelas CSV do código original do CRAAM, chamando h.toCSV().
#
#   tools/craam_csv.sh                 # 2026-03-17 18:00 UT
#   tools/craam_csv.sh 2026-03-18 2000
#
# A saída vai para Reports/craam/. Note que o toCSV() do HATS.py grava o AUX por
# cima do CSV do sinal bruto — os dois usam o nome '-rbd_adcu.csv' — então o
# arquivo com esse nome contém apontamento, e os dados brutos do detector não
# sobrevivem à chamada. O comportamento é reproduzido como está, sem correção.

set -e

ROOT=$(cd "$(dirname "$0")/.." && pwd)
DAY=${1:-2026-03-17}
HOUR=${2:-1800}
REFVENV=${REFVENV:-"$ROOT/.refvenv"}
OUT=${OUTDIR:-"$ROOT/Reports/craam"}

if [ ! -x "$REFVENV/bin/python" ]; then
    echo "Ambiente de referência não encontrado. Rode: make setup-reference"
    exit 1
fi

mkdir -p "$OUT"

HATSXMLPATH="$ROOT/XMLTables"
HATS_DATA_InputPath="$ROOT/Data/$DAY"
HATS_WS_InputPath="$ROOT/Data/$DAY/aux"
HATS_FFTProgram="$ROOT/Docs/upstream/HATS_fft"
PYTHONPATH="$ROOT/Docs/upstream"
export HATSXMLPATH HATS_DATA_InputPath HATS_WS_InputPath HATS_FFTProgram PYTHONPATH

# o toCSV() grava no diretório corrente, então roda-se de dentro dele
cd "$OUT"
"$REFVENV/bin/python" -c "
import HATS
h = HATS.hats('$DAY $HOUR')
h.toCSV(rootname='${DAY}T${HOUR}')
print('HATS.py {}'.format(HATS.__version__))
print('  rData  {:>6} registros   ({} descartados antes da hora nominal)'.format(
      h.rbd.rData.shape[0], h.rbd.MetaData.get('N_Records_Deleted', 0)))
print('  Deconv {:>6} janelas'.format(h.rbd.Deconv.shape[0]))
print('  aux    {:>6} registros'.format(h.aux.Data.shape[0]))
"

echo
echo "Gerado em $OUT"
for f in "$OUT"/${DAY}T${HOUR}-*.csv; do
    [ -e "$f" ] || continue
    printf "  %-40s %8s linhas\n" "$(basename "$f")" "$(( $(wc -l < "$f") - 1 ))"
done
