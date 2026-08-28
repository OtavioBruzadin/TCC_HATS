"""
Exporta a série demodulada de um dos dois pipelines, num formato único.

Existe para permitir `diff` direto entre as duas saídas. Os CSV nativos de cada
pipeline não são comparáveis assim: o do CRAAM tem colunas
`time,husec,amplitude` e o nosso tem `husec,datetime_utc,amplitude_mV`, com
formatos de tempo diferentes — o `diff` acusaria toda linha como divergente sem
que nenhum número tivesse mudado.

    python3      tools/deconv_csv.py --source nosso --out Reports/diff/nosso.csv
    .refvenv/bin/python tools/deconv_csv.py --source craam --out Reports/diff/craam.csv

O modo `craam` precisa do ambiente de referência; o modo `nosso` roda com a
biblioteca padrão.

Alinhamento
-----------
O HATS.py descarta os registros anteriores à hora nominal, o que desloca o
início da janela deslizante. No arquivo T1800 de 2026-03-17 são 559 registros, e
559 = 17x32 + 15: a defasagem não é múltipla do passo, então sem reproduzir o
descarte as duas grades de janelas nunca coincidem. O modo `nosso` aplica o mesmo
descarte por padrão, justamente para o `diff` fazer sentido. Use
--sem-descarte para ver a saída sem essa concessão.
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

HEADER = ["husec", "amplitude_mV"]


def write_csv(rows, destination, decimals):
    import csv

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        for husec, amplitude in rows:
            value = "{:.{}f}".format(amplitude, decimals) if decimals is not None else repr(amplitude)
            writer.writerow([int(husec), value])
    return len(rows)


def from_craam(day, hour, xml_dir, day_dir, upstream_dir, fft_program):
    os.environ["HATSXMLPATH"] = str(xml_dir)
    os.environ["HATS_DATA_InputPath"] = str(day_dir)
    os.environ["HATS_WS_InputPath"] = str(day_dir / "aux")
    os.environ["HATS_FFTProgram"] = str(fft_program)
    sys.path.insert(0, str(upstream_dir))

    try:
        import HATS
    except ImportError:
        raise SystemExit("Não foi possível importar o HATS.py de {}.".format(upstream_dir))

    previous = Path.cwd()
    with tempfile.TemporaryDirectory() as scratch:
        # o getFFT() grava arquivos temporários no diretório corrente
        os.chdir(scratch)
        try:
            h = HATS.hats("{} {}".format(day, hour))
        finally:
            os.chdir(previous)

    rows = list(zip(h.rbd.Deconv["husec"].tolist(), h.rbd.Deconv["amplitude"].tolist()))
    return rows, "HATS.py {}".format(HATS.__version__), h.rbd.MetaData.get("N_Records_Deleted", 0)


def from_ours(day, hour, xml_dir, day_dir, mode):
    from hats import backends, constants, schema as schema_module

    settings = constants.PROCESSING_MODES[mode]

    backend_name, analyser, _exporter = backends.resolve("auto")
    rbd_path = day_dir / "hats-{}T{}.rbd".format(day, hour)
    if not rbd_path.exists():
        raise SystemExit("Arquivo não encontrado: {}".format(rbd_path))

    result = analyser(rbd_path, schema_module.load(xml_dir, "rbd"), {
        "demodulate": True,
        "window_size": constants.WINDOW_SIZE,
        "steps": constants.STEPS,
        "target_frequency": constants.TARGET_FREQUENCY,
        "sampling_frequency": constants.SAMPLING_FREQUENCY,
        "bin_mode": "reference",
        "record_limit": None,
        "drop_before_hour": settings["drop_before_hour"],
        "goertzel_c_legacy": settings["goertzel_c_legacy"],
    })
    if "_deconv" not in result:
        raise SystemExit("Nenhuma janela demodulada — o arquivo é curto demais?")

    husecs, amplitudes, _date = result["_deconv"]
    dropped = result["integrity"].get("records_dropped_before_hour", 0)
    origem = "pacote hats, backend {}, modo {}".format(backend_name, mode)
    return list(zip(husecs, amplitudes)), origem, dropped


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, choices=["craam", "nosso"])
    parser.add_argument("--out", required=True)
    parser.add_argument("--day", default="2026-03-17")
    parser.add_argument("--hour", default="1800")
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--xml-dir", default=str(PROJECT_ROOT / "XMLTables"))
    parser.add_argument("--upstream-dir", default=str(PROJECT_ROOT / "Docs" / "upstream"))
    parser.add_argument("--fft-program", default=None)
    parser.add_argument("--decimals", type=int, default=6,
                        help="Casas decimais na amplitude. Use -1 para precisão total.")
    parser.add_argument("--modo", choices=["craam", "corrigido"], default="craam",
                        help="Modo 'nosso': 'craam' (padrão) reproduz a referência bit a bit; "
                             "'corrigido' aplica as correções e deixa de bater linha a linha.")
    args = parser.parse_args()

    day_dir = Path(args.data_dir) if args.data_dir else (PROJECT_ROOT / "Data" / args.day)
    xml_dir = Path(args.xml_dir)
    destination = Path(args.out)
    decimals = None if args.decimals is not None and args.decimals < 0 else args.decimals

    if args.source == "craam":
        upstream_dir = Path(args.upstream_dir)
        fft_program = Path(args.fft_program) if args.fft_program else upstream_dir / "HATS_fft"
        if not fft_program.exists():
            raise SystemExit("HATS_fft não encontrado em {}.\nRode: make setup-reference".format(fft_program))
        rows, origin, dropped = from_craam(args.day, args.hour, xml_dir, day_dir, upstream_dir, fft_program)
    else:
        rows, origin, dropped = from_ours(args.day, args.hour, xml_dir, day_dir, args.modo)

    count = write_csv(rows, destination, decimals)
    print("  origem     : {}".format(origin))
    print("  arquivo    : {}".format(args.day + "T" + args.hour))
    print("  descartados: {} registros anteriores à hora nominal".format(dropped))
    print("  janelas    : {}".format(count))
    print("  gravado em : {}".format(destination))


if __name__ == "__main__":
    main()
