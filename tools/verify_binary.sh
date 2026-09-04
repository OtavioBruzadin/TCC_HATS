#!/bin/sh
#
# Confronta a nossa compilação do HATS_fft com o binário que o CRAAM distribui.
#
#   tools/verify_binary.sh /caminho/para/HATS_software.zip
#
# O binário distribuído é ELF Linux x86-64 e não roda em macOS nem em ARM, então
# ele é executado sob emulação num container. Exige docker.
#
# Isto responde a uma pergunta que a comparação normal não responde: a nossa
# compilação corresponde ao binário real deles, ou só ao código-fonte compilado
# do nosso jeito? São coisas diferentes — compilando com as flags que o cabeçalho
# do HATS_fft.c indica, em arm64, o resultado difere em todas as janelas.

set -e

ROOT=$(cd "$(dirname "$0")/.." && pwd)
ZIP=${1:-"$HOME/Downloads/HATS_software.zip"}
REFVENV=${REFVENV:-"$ROOT/.refvenv"}

command -v docker >/dev/null || { echo "docker não encontrado."; exit 1; }
docker info >/dev/null 2>&1 || { echo "O daemon do docker não está respondendo."; exit 1; }
[ -f "$ZIP" ] || { echo "Zip não encontrado: $ZIP"; exit 1; }
[ -x "$REFVENV/bin/python" ] || { echo "Rode antes: make setup-reference"; exit 1; }

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

unzip -qo "$ZIP" -d "$WORK/zip"
BINARIO="$WORK/zip/HATS/HATS_fft"
[ -f "$BINARIO" ] || { echo "HATS_fft não encontrado dentro do zip."; exit 1; }

echo "==> binário distribuído"
file "$BINARIO" | sed 's/^/    /'
if command -v objdump >/dev/null; then
    FMA=$(objdump -d "$BINARIO" 2>/dev/null | grep -cE "vfmadd|vfmsub|vfnmadd" || true)
    echo "    instruções FMA: $FMA"
fi

mkdir -p "$WORK/run"
cp "$BINARIO" "$WORK/run/"

echo "==> gerando a entrada a partir de Saida/"
"$REFVENV/bin/python" - "$ROOT" "$WORK/run" <<'PYEOF'
import sys, struct, csv
from pathlib import Path
root, work = Path(sys.argv[1]), Path(sys.argv[2])
tabela = next(root.joinpath("Saida").glob("*-rbd_cal.csv"), None)
if tabela is None:
    raise SystemExit("Nenhum *-rbd_cal.csv em Saida/. Rode antes: make run")
sinal = [float(linha["golay"]) for linha in csv.DictReader(tabela.open())]
(work / "hats_data_rbd.bin").write_bytes(struct.pack("<%dd" % len(sinal), *sinal))
(work / "hats_husec.bin").write_bytes(struct.pack("<%dQ" % len(sinal), *range(len(sinal))))
print("    {} amostras de {}".format(len(sinal), tabela.name))
PYEOF

echo "==> executando o binário original sob emulação x86-64"
docker run --rm --platform linux/amd64 -v "$WORK/run":/w -w /w debian:stable-slim \
    sh -c "apt-get -qq update >/dev/null 2>&1 && \
           apt-get -qq install -y libfftw3-double3 >/dev/null 2>&1 && \
           ./HATS_fft 128 32"

echo "==> comparando"
"$REFVENV/bin/python" - "$ROOT" "$WORK/run" <<'PYEOF'
import sys, struct, csv
from pathlib import Path
root, work = Path(sys.argv[1]), Path(sys.argv[2])
sys.path.insert(0, str(root))
from hats import demodulation

bruto = (work / "hats_data_rbd.bin").read_bytes()
original = struct.unpack("<%dd" % (len(bruto) // 8), bruto)

tabela = next(root.joinpath("Saida").glob("*-rbd_cal.csv"))
sinal = [float(linha["golay"]) for linha in csv.DictReader(tabela.open())]
_, nosso = demodulation.demodulate(sinal, list(range(len(sinal))))

iguais = sum(1 for a, b in zip(original, nosso) if a == b)
print("    janelas: {}".format(len(original)))
print("    bit a bit iguais: {}/{}".format(iguais, len(original)))
if iguais != len(original):
    pior = max(abs(a - b) / abs(a) for a, b in zip(original, nosso) if a)
    print("    erro relativo máximo: {:.3e}".format(pior))
    raise SystemExit(1)
print()
print("    A nossa implementação reproduz o binário distribuído pelo CRAAM,")
print("    bit a bit, em todas as janelas.")
PYEOF
