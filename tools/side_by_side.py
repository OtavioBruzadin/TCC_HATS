"""
Imprime lado a lado uma linha de cada pipeline, para conferência a olho nu.

Roda o HATS.py original do CRAAM e o deste projeto sobre o mesmo arquivo e mostra,
campo a campo, o que cada um produz: um registro bruto, o mesmo registro calibrado,
uma janela demodulada e um registro de apontamento.

Precisa de um interpretador com numpy/scipy/pandas/astropy, porque executa o
HATS.py de referência:

    /tmp/refvenv/bin/python tools/side_by_side.py

Opções: --date, --hour, --day-dir, --record, --module, --backend.
"""

import argparse
import sys
from array import array
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_with_reference import load_reference

GREEN = "\033[32m"
YELLOW = "\033[33m"
DIM = "\033[2m"
RESET = "\033[0m"


def supports_colour():
    return sys.stdout.isatty()


def paint(text, colour):
    return "{}{}{}".format(colour, text, RESET) if supports_colour() else text


def title(text):
    print()
    print("=" * 78)
    print(" " + text)
    print("=" * 78)
    print(" {:<20} {:>24} {:>24}".format("campo", "CRAAM (HATS.py)", "NOSSO"))
    print(" " + "-" * 76)


def row(name, reference_value, our_value, tolerance=0.0):
    if isinstance(reference_value, float) or isinstance(our_value, float):
        left = "{:.9f}".format(reference_value)
        right = "{:.9f}".format(our_value)
        difference = abs(float(reference_value) - float(our_value))
        equal = difference <= tolerance
        if equal:
            mark = paint("=", GREEN)
        elif reference_value:
            relative = difference / abs(float(reference_value))
            mark = paint("dif {:.1e} mV  ({:.1e} rel)".format(difference, relative), YELLOW)
        else:
            mark = paint("dif {:.1e}".format(difference), YELLOW)
    else:
        left, right = str(reference_value), str(our_value)
        equal = reference_value == our_value
        mark = paint("=", GREEN) if equal else paint("DIFERE", YELLOW)
    print(" {:<20} {:>24} {:>24}   {}".format(name, left, right, mark))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--day-dir", default="Data/2026-03-17")
    parser.add_argument("--date", default="2026-03-17")
    parser.add_argument("--hour", default="1800")
    parser.add_argument("--xml-dir", default="XMLTables")
    parser.add_argument("--upstream-dir", default="Docs/upstream")
    parser.add_argument("--project-dir", default=".")
    parser.add_argument("--fft-program", default=None)
    parser.add_argument("--record", type=int, default=0,
                        help="Índice do registro a mostrar, contado sobre o que a referência manteve.")
    parser.add_argument("--backend", choices=["auto", "numpy", "stdlib"], default="auto")
    args = parser.parse_args()

    project_dir = Path(args.project_dir).resolve()
    sys.path.insert(0, str(project_dir))

    from hats import backends, calibration, constants, demodulation, pointing, records, schema as schema_module

    backend_name, _analyser, _exporter = backends.resolve(args.backend)

    xml_dir = project_dir / args.xml_dir
    upstream_dir = project_dir / args.upstream_dir
    day_dir = Path(args.day_dir).resolve()
    fft_program = Path(args.fft_program).resolve() if args.fft_program else (upstream_dir / "HATS_fft")

    stem = "hats-{}T{}".format(args.date, args.hour)
    rbd_path = day_dir / (stem + ".rbd")
    aux_path = day_dir / "aux" / (stem + ".aux")

    HATS, reference = load_reference(upstream_dir, xml_dir, day_dir, fft_program, args.date, args.hour)

    print()
    print(" arquivo   : {}".format(rbd_path.name))
    print(" referência: HATS.py {}".format(HATS.__version__))
    print(" nosso     : pacote hats (backend {})".format(backend_name))
    print(paint(" nota: a referência descarta {} registros do início (husec < hora*36000000);"
                .format(reference.rbd.MetaData.get("N_Records_Deleted", 0)), DIM))
    print(paint("       os índices abaixo são contados sobre o que ela manteve.", DIM))

    rbd_schema = schema_module.load(xml_dir, "rbd")
    aux_schema = schema_module.load(xml_dir, "aux")
    field_index = {f["name"]: i for i, f in enumerate(rbd_schema["fields"])}
    ours = list(records.iter_records(rbd_path, rbd_schema))

    reference_raw = reference.rbd.rData
    reference_calibrated = reference.rbd.cData
    position = {int(values[field_index["husec"]]): i for i, values in enumerate(ours)}

    index = max(0, min(args.record, reference_raw.shape[0] - 1))
    our_index = position[int(reference_raw["husec"][index])]

    title("RBD bruto — registro #{} (husec {})".format(index, int(reference_raw["husec"][index])))
    for field in rbd_schema["fields"]:
        name = field["name"]
        value = ours[our_index][field_index[name]]
        if field.get("origin") == "ad7770":
            value = calibration.decode_ad7770(value)
        row(name, int(reference_raw[name][index]), int(value))

    title("RBD calibrado — mesmo registro")
    for field in rbd_schema["fields"]:
        if field.get("convert") != "yes":
            continue
        name = field["name"]
        value = ours[our_index][field_index[name]]
        if field.get("origin") == "ad7770":
            value = calibration.decode_ad7770(value)
        calibrated = value * field["slope"] + field["offset"]
        unit = field.get("converted_unit", "")
        row("{} [{}]".format(name, unit), float(reference_calibrated[name][index]), calibrated, tolerance=0.0)

    signal = array("d", [float(x) for x in reference_calibrated["golay"]])
    husec = array("Q", [int(x) for x in reference_raw["husec"]])
    our_husec, our_amplitude = demodulation.demodulate(signal, husec)
    window = min(args.record, len(our_amplitude) - 1) if len(our_amplitude) else 0

    if len(our_amplitude):
        title("Demodulação 20 Hz — janela #{}".format(window))
        row("husec (centro)", int(reference.rbd.Deconv["husec"][window]), int(our_husec[window]))
        row("amplitude [mV]", float(reference.rbd.Deconv["amplitude"][window]),
            float(our_amplitude[window]), tolerance=0.0)
        print()
        print(paint("   a diferença aqui é o off-by-one da recursão de Goertzel no", DIM))
        print(paint("   windowed_dft.c do CRAAM, ~0,003%. Ver Docs/upstream/README.md.", DIM))

    aux_index = {f["name"]: i for i, f in enumerate(aux_schema["fields"])}
    our_aux = list(records.iter_records(aux_path, aux_schema))
    reference_aux = reference.aux.Data
    aux_record = min(args.record, len(our_aux) - 1)

    title("AUX apontamento — registro #{}".format(aux_record))
    for field in aux_schema["fields"]:
        name = field["name"]
        if name not in reference_aux.dtype.names:
            continue
        reference_value = reference_aux[name][aux_record]
        our_value = our_aux[aux_record][aux_index[name]]
        if isinstance(our_value, int):
            row(name, int(reference_value), int(our_value))
        else:
            row(name, float(reference_value), float(our_value), tolerance=0.0)

    print()
    print(" Campos que só o nosso produz (o HATS.py não os tem):")
    print(" " + "-" * 76)
    generic = records.Record({n: our_aux[aux_record][i] for n, i in aux_index.items()})
    corrected = pointing.corrected_values(generic)
    for name, value in corrected.items():
        print(" {:<24} {:>20.9f}   {}".format(name, value, paint("corrigido", GREEN)))
    state = pointing.state(generic)
    print(" {:<24} {:>20}   {}".format("record_valid", str(state["record_valid"]),
                                       paint("registro defasado" if not state["record_valid"] else "", YELLOW)))
    opmode = getattr(generic, "opmode", None)
    print(" {:<24} {:>20}".format("opmode_name", constants.OPMODE_NAMES.get(opmode, "unknown")))
    print()


if __name__ == "__main__":
    main()
