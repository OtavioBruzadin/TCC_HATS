"""
Leitura, validação e redução dos dados do telescópio solar HATS.

O HATS (CRAAM/Mackenzie, instalado no OAFA) observa o Sol em 15 THz. O detector é
uma célula Golay, que só responde a sinal modulado: a radiação é picada por um
chopper a 20 Hz, amostrada a 1 kHz, e a amplitude em 20 Hz é extraída por software.

Organização do pacote
---------------------
    constants      grandezas fixas do instrumento
    timebase       conversões entre husec e datetime
    schema         formato dos registros, lido dos XML do CRAAM
    records        iteração sobre os arquivos binários
    calibration    decodificação do AD7770 e conversão para unidades físicas
    demodulation   extração da amplitude em 20 Hz
    rbd            leitura e demodulação do sinal do detector
    discovery      descoberta dos arquivos no disco
    pipeline       orquestração e escrita das tabelas
    backends       escolha entre o caminho numpy e o stdlib
    cli            interface de linha de comando

Este pacote reproduz a saída do HATS.py do CRAAM byte a byte. Não acrescenta
colunas, arquivos nem relatórios: o que ele oferece é a mesma saída, mais rápido.

O pacote funciona só com a biblioteca padrão. Quando numpy está instalado, o
backend acelerado é escolhido automaticamente; os resultados são os mesmos.
"""

__version__ = "1.0.0"

from hats import constants  # noqa: F401

__all__ = ["constants", "__version__"]
