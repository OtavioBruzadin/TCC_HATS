"""Interface de linha de comando."""

import argparse
import os
from pathlib import Path

from hats import __version__, backends, constants, diagnostics, discovery, pipeline, schema as schema_module


def build_parser():
    parser = argparse.ArgumentParser(
        prog="hats_report",
        description="Reimplementação do processamento de dados do HATS. Produz exatamente "
                    "as mesmas tabelas que o HATS.py do CRAAM, byte a byte.")

    layout = parser.add_argument_group("localização dos arquivos")
    layout.add_argument("--project-root", default=".")
    layout.add_argument("--data-dir", default=None,
                        help="Diretório `data` do manual do CRAAM: os .rbd aqui, os .aux e "
                             "os .ws em aux/. Sem isto, usa HATS_DATA_InputPath.")
    layout.add_argument("--output-dir", default="Saida",
                        help="Onde gravar as tabelas. Padrão: Saida/")
    layout.add_argument("--diagnostics-dir", default="Diagnostico",
                        help="Onde gravar os relatórios JSON. Padrão: Diagnostico/")
    layout.add_argument("--xml-dir", default=None,
                        help="Onde estão os XML de formato. Sem isto, usa HATSXMLPATH.")
    layout.add_argument("--day", default=None, help="Processa só este dia, YYYY-MM-DD.")
    layout.add_argument("--hour", default=None,
                        help="Processa só esta hora, no formato HHMM — por exemplo 1800.")

    processing = parser.add_argument_group("processamento")
    processing.add_argument("--record-limit", type=int, default=None,
                            help="Lê só os N primeiros registros de cada binário.")
    processing.add_argument("--no-demod", action="store_true",
                            help="Pula a demodulação; não grava o arquivo -deconv.csv.")
    processing.add_argument("--fft-window", type=int, default=constants.WINDOW_SIZE)
    processing.add_argument("--fft-steps", type=int, default=constants.STEPS)
    processing.add_argument("--fft-target-hz", type=float, default=constants.TARGET_FREQUENCY)
    processing.add_argument("--fft-sampling-hz", type=float, default=constants.SAMPLING_FREQUENCY)
    processing.add_argument("--diagnostico", dest="diagnostico", action="store_true",
                            help="Também grava os relatórios JSON. Eles não fazem parte da "
                                 "saída reproduzida e exigem uma passada a mais sobre o "
                                 "arquivo — cerca de 8 s por hora de dados.")
    processing.add_argument("--backend", choices=["auto", "numpy", "stdlib"], default="auto")

    info = parser.add_argument_group("informação")
    info.add_argument("--init-project", action="store_true", help="Só cria a estrutura de pastas.")
    info.add_argument("--backends", action="store_true", help="Mostra os backends disponíveis.")
    info.add_argument("--version", action="version", version="hats {}".format(__version__))
    return parser


# As mesmas variáveis que o manual do CRAAM manda exportar na instalação. Quem
# configurou a máquina seguindo o manual roda este pipeline sem passar flag
# nenhuma. As flags, quando dadas, têm precedência.
#
# HATS_FFTProgram não se aplica: aqui a demodulação é feita em processo, sem
# lançar binário externo. PYTHONPATH é do interpretador.
ENVIRONMENT = {
    "data_dir": ("HATS_DATA_InputPath", "Data"),
    "xml_dir": ("HATSXMLPATH", "XMLTables"),
}


def resolve_path(project_root, given, key):
    """Flag, senão variável de ambiente, senão o padrão do projeto."""
    variable, fallback = ENVIRONMENT[key]
    chosen = given or os.environ.get(variable) or fallback
    path = Path(chosen).expanduser()
    return path if path.is_absolute() else (project_root / path)


def main(argv=None):
    args = build_parser().parse_args(argv)

    if args.backends:
        print(backends.describe())
        return 0

    backend_name, analyser = backends.resolve(args.backend)
    project_root = Path(args.project_root).resolve()
    paths = discovery.ensure_structure(
        project_root,
        resolve_path(project_root, args.data_dir, "data_dir"),
        args.output_dir,
        resolve_path(project_root, args.xml_dir, "xml_dir"),
        args.diagnostics_dir)

    if args.init_project:
        print("Projeto criado em {}".format(project_root))
        for key in ("data_dir", "output_dir", "diagnostics_dir", "xml_dir"):
            print("  {:<12} {}".format(key, paths[key]))
        return 0

    day_index = discovery.build_day_index(paths["data_dir"])
    if args.day:
        day_index = {key: value for key, value in day_index.items() if key == args.day}
    if args.hour:
        for value in day_index.values():
            value["hours"] = {key: files for key, files in value["hours"].items()
                              if key in (args.hour, "daily")}
        day_index = {day: value for day, value in day_index.items()
                     if any(key != "daily" for key in value["hours"])}
    if not day_index:
        raise SystemExit("Nenhum arquivo encontrado em {} para os filtros dados.".format(
            paths["data_dir"]))

    options = {
        "demodulate": not args.no_demod,
        "window_size": args.fft_window,
        "steps": args.fft_steps,
        "target_frequency": args.fft_target_hz,
        "sampling_frequency": args.fft_sampling_hz,
        "bin_mode": "reference",
        "record_limit": args.record_limit,
    }

    schemas = {
        "rbd": schema_module.load(paths["xml_dir"], "rbd"),
        "aux": schema_module.load(paths["xml_dir"], "aux"),
    }

    print("hats {}  |  backend: {}".format(__version__, backend_name))
    print("  dados: {}".format(paths["data_dir"]))
    total = 0
    for day_key, day_info in sorted(day_index.items()):
        for hour_key, files in sorted(day_info["hours"].items()):
            if hour_key == "daily":
                continue
            print("  {} {} ...".format(day_key, hour_key), flush=True)
            stem = "{}T{}".format(day_key, hour_key)
            written, deconv, offset = pipeline.process_hour(
                paths["output_dir"], stem,
                files.get("rbd"), schemas["rbd"], files.get("aux"), schemas["aux"],
                options, analyser)
            for path in written:
                print("    {}".format(path.name))
            total += len(written)

            if args.diagnostico:
                if files.get("rbd"):
                    diagnostics.write_json(
                        diagnostics.rbd_diagnostics(files["rbd"], schemas["rbd"],
                                                    options, offset, deconv),
                        paths["diagnostics_dir"] / "{}-rbd.json".format(stem))
                if files.get("aux"):
                    diagnostics.write_json(
                        diagnostics.aux_diagnostics(files["aux"], schemas["aux"], options),
                        paths["diagnostics_dir"] / "{}-aux.json".format(stem))

        if args.diagnostico:
            weather_file = day_info["hours"].get("daily", {}).get("ws")
            if weather_file:
                diagnostics.write_json(
                    diagnostics.ws_diagnostics(weather_file),
                    paths["diagnostics_dir"] / "{}-ws.json".format(day_key))

    print("{} tabela(s) em {}".format(total, paths["output_dir"]))
    if args.diagnostico:
        print("diagnóstico em {}".format(paths["diagnostics_dir"]))
    return 0
