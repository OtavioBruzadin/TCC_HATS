"""
Mede o desempenho dos backends e, se disponível, do pipeline original do CRAAM.

    python3 tools/benchmark.py                      # gera uma hora sintética e mede
    python3 tools/benchmark.py --rbd caminho.rbd    # mede sobre um arquivo real
    python3 tools/benchmark.py --with-craam         # inclui o HATS.py de referência

O modo --with-craam precisa do ambiente de referência; monte-o com
`make setup-reference` ou `tools/setup_reference.sh`.
"""

import argparse
import resource
import shutil
import struct
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def synthesise_hour(destination, seconds=3600, sampling=1000):
    """
    Gera um .rbd de uma hora com um sinal picado a 20 Hz.

    O conteúdo é periódico, mas o volume, o formato e o caminho de código são os
    reais — que é o que interessa para medir tempo e memória.
    """
    import math

    packer = struct.Struct("<IIHQiiiii")
    total = seconds * sampling
    print("  gerando {} registros ({:.0f} MB) ...".format(total, total * 38 / 1e6), flush=True)

    with destination.open("wb") as handle:
        for second in range(seconds):
            chunk = bytearray()
            for tick in range(sampling):
                index = second * sampling + tick
                volts = 100.0 * math.sin(2 * math.pi * 20.0 * tick / sampling)
                chunk += packer.pack(
                    index, 1773770400 + second, tick,
                    18 * 36000000 + index * 10,
                    int(round(volts / 0.001154)) & 0x00FFFFFF,
                    2000, 1045255, 1001714, 1694898)
            handle.write(chunk)
    return total


def synthesise_aux(destination, seconds=3600):
    """
    Gera o .aux correspondente, a cerca de um registro por segundo.

    O HATS.py de referência lê os dois arquivos ao instanciar, então ele precisa
    existir para o modo --with-craam funcionar. O pacote não depende disto.
    """
    packer = struct.Struct("<Qddddddddii")
    destination.parent.mkdir(parents=True, exist_ok=True)
    count = int(seconds / 1.05)
    with destination.open("wb") as handle:
        for index in range(count):
            handle.write(packer.pack(
                18 * 36000000 + index * 10500,
                2461117.25 + index * 1.2e-5,
                1.0665 + index * 2.9245e-4,
                54.62 - index * 0.0002, 326.58 - index * 0.0007,
                23.8268, -1.0859, 0.0375385, 0.0164809, 11, 0))
    return count


def peak_memory_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)


def time_it(function, repeats):
    best = None
    result = None
    for _ in range(repeats):
        start = time.perf_counter()
        result = function()
        elapsed = time.perf_counter() - start
        best = elapsed if best is None else min(best, elapsed)
    return best, result


def measure_package(rbd_path, xml_dir, backend, repeats):
    from hats import backends, rbd as rbd_module, schema as schema_module

    name, analyser, _exporter = backends.resolve(backend)
    loaded = schema_module.load(xml_dir, "rbd")
    options = {"demodulate": True, "window_size": 128, "steps": 32,
               "target_frequency": 20.0, "sampling_frequency": 1000.0,
               "bin_mode": "reference", "record_limit": None}
    before = peak_memory_mb()
    elapsed, result = time_it(lambda: analyser(rbd_path, loaded, dict(options)), repeats)
    return {
        "label": "pacote hats, backend {}".format(name),
        "seconds": elapsed,
        "memory_mb": max(peak_memory_mb() - before, peak_memory_mb()),
        "windows": result["demodulation"]["windows"],
        "amplitude_mean": result["demodulation"]["amplitude"]["mean"],
    }


def measure_craam(rbd_path, xml_dir, upstream_dir, fft_program, repeats):
    """Roda o HATS.py original. Exige numpy, scipy, pandas e astropy."""
    import os

    date_str = rbd_path.name[5:15]
    hour = rbd_path.name.split("T", 1)[1][:4]

    os.environ["HATSXMLPATH"] = str(xml_dir)
    os.environ["HATS_DATA_InputPath"] = str(rbd_path.parent)
    os.environ["HATS_FFTProgram"] = str(fft_program)
    sys.path.insert(0, str(upstream_dir))
    import HATS

    previous = Path.cwd()
    with tempfile.TemporaryDirectory() as scratch:
        os.chdir(scratch)
        try:
            before = peak_memory_mb()
            elapsed, result = time_it(lambda: HATS.hats("{} {}".format(date_str, hour)), repeats)
        finally:
            os.chdir(previous)

    return {
        "label": "HATS.py do CRAAM {}".format(HATS.__version__),
        "seconds": elapsed,
        "memory_mb": max(peak_memory_mb() - before, peak_memory_mb()),
        "windows": int(result.rbd.Deconv.shape[0]),
        "amplitude_mean": float(result.rbd.Deconv["amplitude"].mean()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rbd", default=None, help="Arquivo .rbd a medir. Sem isto, gera uma hora sintética.")
    parser.add_argument("--xml-dir", default=str(PROJECT_ROOT / "XMLTables"))
    parser.add_argument("--upstream-dir", default=str(PROJECT_ROOT / "Docs" / "upstream"))
    parser.add_argument("--fft-program", default=None)
    parser.add_argument("--repeats", type=int, default=3, help="Execuções por medida; vale a melhor.")
    parser.add_argument("--with-craam", action="store_true", help="Também mede o pipeline original.")
    parser.add_argument("--seconds", type=int, default=3600, help="Duração do arquivo sintético.")
    args = parser.parse_args()

    xml_dir = Path(args.xml_dir)
    scratch = None
    if args.rbd:
        rbd_path = Path(args.rbd).resolve()
    else:
        scratch = Path(tempfile.mkdtemp(prefix="hats-bench-"))
        rbd_path = scratch / "hats-2026-03-17T1800.rbd"
        synthesise_hour(rbd_path, seconds=args.seconds)
        synthesise_aux(scratch / "aux" / "hats-2026-03-17T1800.aux", seconds=args.seconds)

    try:
        print()
        print("arquivo : {}".format(rbd_path.name))
        print("tamanho : {:.0f} MB".format(rbd_path.stat().st_size / 1e6))
        print("medidas : melhor de {}".format(args.repeats))
        print()

        rows = []
        from hats import backends
        rows.append(measure_package(rbd_path, xml_dir, "stdlib", args.repeats))
        if backends.numpy_available():
            rows.append(measure_package(rbd_path, xml_dir, "numpy", args.repeats))
        else:
            print("  (numpy não instalado: só o backend stdlib foi medido)")

        if args.with_craam:
            fft_program = Path(args.fft_program) if args.fft_program else Path(args.upstream_dir) / "HATS_fft"
            aux_path = rbd_path.parent / "aux" / rbd_path.name.replace(".rbd", ".aux")
            if not fft_program.exists():
                print("  (HATS_fft não encontrado em {}; rode make setup-reference)".format(fft_program))
            elif not aux_path.exists():
                print("  (o HATS.py lê o .aux junto com o .rbd, e {} não existe;".format(aux_path.name))
                print("   rode sem --rbd para medir sobre dados sintéticos completos)")
            else:
                rows.append(measure_craam(rbd_path, xml_dir, Path(args.upstream_dir),
                                          fft_program, args.repeats))

        width = max(len(row["label"]) for row in rows)
        print(" {:<{w}} {:>10} {:>12} {:>10} {:>18}".format(
            "implementação", "segundos", "memória MB", "janelas", "amplitude média", w=width))
        print(" " + "-" * (width + 54))
        for row in rows:
            print(" {:<{w}} {:>10.2f} {:>12.0f} {:>10} {:>18.6f}".format(
                row["label"], row["seconds"], row["memory_mb"], row["windows"],
                row["amplitude_mean"], w=width))

        baseline = next((r for r in rows if "CRAAM" in r["label"]), None)
        if baseline:
            print()
            for row in rows:
                if row is baseline:
                    continue
                ratio = baseline["seconds"] / row["seconds"]
                verb = "mais rápido" if ratio >= 1 else "mais lento"
                print("  {} é {:.1f}x {} que o CRAAM".format(
                    row["label"], ratio if ratio >= 1 else 1 / ratio, verb))
        print()
    finally:
        if scratch:
            shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
