"""
Geração de arquivos de teste sintéticos.

Os testes não dependem dos dados reais: cada um monta os binários de que precisa
num diretório temporário, com valores escolhidos para que a resposta certa seja
conhecida de antemão.
"""

import math
import struct

RBD_FORMAT = "<IIHQiiiii"
RBD_RECORD_SIZE = 38
AUX_FORMAT = "<Qddddddddii"
AUX_RECORD_SIZE = 80

GOLAY_SLOPE = 0.001154


def encode_ad7770(value):
    """Inverso de calibration.decode_ad7770: valor com sinal -> palavra de 24 bits."""
    return value & 0x00FFFFFF


def write_rbd(path, samples=2000, amplitude_mv=100.0, frequency=20.0,
              sampling=1000.0, start_husec=18 * 36000000, start_sec=1773770400):
    """
    Escreve um .rbd com uma senoide pura no canal do detector.

    A amplitude em mV é conhecida, então a demodulação pode ser conferida contra
    um valor esperado.
    """
    with path.open("wb") as handle:
        for index in range(samples):
            husec = start_husec + index * 10
            volts = amplitude_mv * math.sin(2.0 * math.pi * frequency * index / sampling)
            golay = int(round(volts / GOLAY_SLOPE))
            handle.write(struct.pack(
                RBD_FORMAT,
                1000 + index,                       # sample
                start_sec + index // 1000,          # sec
                index % 1000,                       # ms
                husec,
                encode_ad7770(golay),
                encode_ad7770(2000),                # chopper
                encode_ad7770(1045255),             # temp_hics
                encode_ad7770(1001714),             # temp_env
                encode_ad7770(1694898),             # temp_golay
            ))
    return samples


def write_aux(path, records=100, stale_every=4, start_husec=18 * 36000000):
    """
    Escreve um .aux em que um registro a cada `stale_every` é defasado.

    Os defasados têm jd == 0 e apontamento de um instante anterior, que é o
    padrão observado nos dados reais.
    """
    stale_count = 0
    with path.open("wb") as handle:
        for index in range(records):
            husec = start_husec + index * 10500
            is_stale = stale_every and index % stale_every == 0
            if is_stale:
                stale_count += 1
            # o defasado carrega o estado de 1000 posições atrás
            source = index - 1000 if is_stale else index
            handle.write(struct.pack(
                AUX_FORMAT,
                husec,
                0.0 if is_stale else 2461117.25 + index * 1.2e-6,
                # sid avança 1,05 s por registro, corrigido pela razão entre o
                # dia sideral e o solar: 1,05 * 1,0027 / 3600 h por registro.
                1.0665 + source * 2.9245e-4,        # sid, em horas
                54.62 - source * 0.0002,            # elevation
                326.58 - source * 0.0007,           # azimuth
                23.8268,                            # right_ascension, em HORAS
                -1.0859,                            # declination
                0.0375385,                          # ra_rate, em arcsec/s
                0.0164809,                          # dec_rate, em arcsec/s
                11,                                 # object
                7 if is_stale else 0,               # opmode
            ))
    return records, stale_count


def write_ws(path, rows=20):
    """Escreve um .ws incluindo uma linha com o byte 0x7f e uma inutilizável."""
    with path.open("w", encoding="utf-8") as handle:
        for index in range(rows):
            handle.write("2026-03-17T{:02d}:{:02d}:00,0R2,Ta=18.4C,Ua=9.0P,Pa=757.5H\n".format(
                index // 60, index % 60))
        handle.write("\x7f2026-03-17T23:59:59,0R2,Ta=15.0C,Ua=9.0P,Pa=757.5H\n")
        handle.write("linha sem virgulas nenhuma\n")
    return rows + 1, 1, 1   # linhas válidas, rejeitadas, recuperadas


def build_day(root, date="2026-03-17", hour="1800", **kwargs):
    """Monta uma árvore Data/ completa e devolve os caminhos criados."""
    day_dir = root / "Data" / date
    aux_dir = day_dir / "aux"
    aux_dir.mkdir(parents=True, exist_ok=True)

    rbd_path = day_dir / "hats-{}T{}.rbd".format(date, hour)
    aux_path = aux_dir / "hats-{}T{}.aux".format(date, hour)
    ws_path = aux_dir / "hats-{}.ws".format(date)

    samples = write_rbd(rbd_path, **kwargs)
    records, stale = write_aux(aux_path)
    write_ws(ws_path)
    return {"root": root, "day_dir": day_dir, "rbd": rbd_path, "aux": aux_path,
            "ws": ws_path, "samples": samples, "aux_records": records, "aux_stale": stale}
