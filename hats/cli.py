"""Interface de linha de comando."""

import argparse
from pathlib import Path

from hats import __version__, backends, constants, discovery, exporters, reports, schema as schema_module


def build_parser():
    parser = argparse.ArgumentParser(
        prog="hats_report",
        description="Lê, valida e demodula os dados do telescópio solar HATS.")

    layout = parser.add_argument_group("localização dos arquivos")
    layout.add_argument("--project-root", default=".")
    layout.add_argument("--data-dir", default="Data")
    layout.add_argument("--reports-dir", default="Reports")
    layout.add_argument("--xml-dir", default="XMLTables")
    layout.add_argument("--day", default=None, help="Processa só este dia, no formato YYYY-MM-DD.")

    output = parser.add_argument_group("saídas")
    output.add_argument("--export-csv", action="store_true",
                        help="Exporta CSV do apontamento, da estação e da amplitude demodulada.")
    output.add_argument("--export-rbd-csv", action="store_true",
                        help="Também exporta o CSV do sinal bruto de 1 kHz: ~3,6 M linhas "
                             "e ~767 MB por hora de dados.")
    output.add_argument("--csv-limit", type=int, default=None, help="Máximo de linhas por CSV.")
    output.add_argument("--sample-count", type=int, default=5,
                        help="Registros mostrados de cada ponta do arquivo no JSON.")

    processing = parser.add_argument_group("processamento")
    processing.add_argument("--record-limit", type=int, default=None,
                            help="Lê só os N primeiros registros de cada binário.")
    processing.add_argument("--no-demod", action="store_true", help="Pula a demodulação de 20 Hz.")
    processing.add_argument("--fft-window", type=int, default=constants.WINDOW_SIZE)
    processing.add_argument("--fft-steps", type=int, default=constants.STEPS)
    processing.add_argument("--fft-target-hz", type=float, default=constants.TARGET_FREQUENCY)
    processing.add_argument("--fft-sampling-hz", type=float, default=constants.SAMPLING_FREQUENCY)
    processing.add_argument("--fft-bin-mode", choices=["reference", "exact"], default="reference",
                            help="'reference' reproduz o floor() do HATS_fft.c; 'exact' usa o "
                                 "bin fracionário e elimina o scalloping dependente de fase.")
    processing.add_argument("--backend", choices=["auto", "numpy", "stdlib"], default="auto")

    info = parser.add_argument_group("informação")
    info.add_argument("--init-project", action="store_true", help="Só cria a estrutura de pastas.")
    info.add_argument("--backends", action="store_true", help="Mostra os backends disponíveis e sai.")
    info.add_argument("--version", action="version", version="hats {}".format(__version__))
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)

    if args.backends:
        print(backends.describe())
        return 0

    backend_name, analyser, rbd_exporter = backends.resolve(args.backend)
    project_root = Path(args.project_root).resolve()
    paths = discovery.ensure_structure(project_root, args.data_dir, args.reports_dir, args.xml_dir)

    if args.init_project:
        print("Projeto criado em {}".format(project_root))
        for key in ("data_dir", "reports_dir", "xml_dir"):
            print("  {:<12} {}".format(key, paths[key]))
        return 0

    day_index = discovery.build_day_index(paths["data_dir"])
    if args.day:
        day_index = {key: value for key, value in day_index.items() if key == args.day}
    if not day_index:
        raise SystemExit("Nenhuma pasta de dia encontrada em {}.".format(paths["data_dir"]))

    options = {
        "demodulate": not args.no_demod,
        "window_size": args.fft_window,
        "steps": args.fft_steps,
        "target_frequency": args.fft_target_hz,
        "sampling_frequency": args.fft_sampling_hz,
        "bin_mode": args.fft_bin_mode,
        "record_limit": args.record_limit,
    }
    settings = {
        "options": options,
        "analyser": analyser,
        "rbd_exporter": rbd_exporter,
        "sample_count": args.sample_count,
        "export_csv": args.export_csv,
        "export_rbd_csv": args.export_rbd_csv,
        "csv_limit": args.csv_limit,
    }
    schemas = {
        "rbd": schema_module.load(paths["xml_dir"], "rbd"),
        "aux": schema_module.load(paths["xml_dir"], "aux"),
    }

    print("hats {}  |  backend: {}".format(__version__, backend_name))
    processed = []
    for day_key, day_info in day_index.items():
        reports.process_day(day_key, day_info, paths, schemas, settings)
        processed.append(day_key)

    exporters.write_json({
        "hats_version": __version__,
        "backend": backend_name,
        "project_root": str(project_root),
        "data_dir": str(paths["data_dir"]),
        "reports_dir": str(paths["reports_dir"]),
        "xml_dir": str(paths["xml_dir"]),
        "rbd_schema_mode": schemas["rbd"].get("mode"),
        "rbd_schema_source": schemas["rbd"].get("source"),
        "aux_schema_mode": schemas["aux"].get("mode"),
        "aux_schema_source": schemas["aux"].get("source"),
        "processing_options": options,
        "aux_unit_corrections": {
            name: {"declared_in_xml": declared, "actual": actual,
                   "corrected_field": corrected_name, "factor": factor}
            for name, (declared, actual, factor, corrected_name)
            in schema_module.AUX_UNIT_FIXES.items()
        },
        "days_processed": processed,
        # Referências, não cópias: cada relatório já está no seu próprio arquivo.
        "day_reports": {day: "{}__day_report.json".format(day) for day in processed},
    }, paths["reports_dir"] / "summary.json")

    print("{} dia(s) processado(s).".format(len(processed)))
    print("Resumo em {}".format(paths["reports_dir"] / "summary.json"))
    return 0
