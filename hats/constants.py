"""Grandezas fixas do instrumento e do formato dos dados."""

# --- Aquisição -------------------------------------------------------------

SAMPLING_FREQUENCY = 1000.0     # Hz, taxa de amostragem do .rbd
TARGET_FREQUENCY = 20.0         # Hz, frequência do chopper

# --- Demodulação, valores do HATS_fft.h do CRAAM ---------------------------

WINDOW_SIZE = 128               # amostras por janela
STEPS = 32                      # deslocamento entre janelas

# Correção de amplitude da janela flat-top (ISO 18431-1), do windowed_dft.c.
# Escolhida de modo que dividir a saída pelo comprimento da janela já entregue a
# amplitude de uma senoide pura.
FLATTOP_CORRECTION = 2.000122419602196

# --- Base de tempo ---------------------------------------------------------

HUSEC_PER_SECOND = 10000        # husec = centésimos de milissegundo
HUSEC_PER_MINUTE = 600000
HUSEC_PER_HOUR = 36000000
HUSEC_PER_DAY = 864000000
HUSEC_PER_SAMPLE = 10           # a 1 kHz

# --- Leitura ---------------------------------------------------------------

# Registros lidos por bloco. O caminho stdlib prefere blocos pequenos, que cabem
# em cache: medido numa hora de dados, 16384 dá 4,3 s e 34 MB contra 6,2 s e
# 151 MB a 131072. O caminho numpy prefere blocos grandes, porque o custo por
# chamada é o que domina: 262144 dá 0,26 s contra 0,40 s a 65536.
STDLIB_CHUNK_RECORDS = 16384
NUMPY_CHUNK_RECORDS = 262144

# --- Modos de operação -----------------------------------------------------

# Do extract_scans() e do SkyDip() do HATS.py. A correspondência de 7 e 8 estava
# invertida no HATS.py até 2025-10-17; a revisão de 2026-04-17 corrigiu, com a
# nota "Corrected extract_scans: opmode were wrong". Os valores abaixo seguem a
# versão corrigida.
#
# Isso não é verificável a partir dos dados de 2026-03: quase todo registro com
# opmode 7 ou 8 é um dos defasados (61 de 62), então a excursão real em ascensão
# reta e declinação durante a varredura não aparece.
OPMODE_NAMES = {
    0: "tracking",
    7: "right_ascension_scan",
    8: "declination_scan",
    10: "skydip",
}
