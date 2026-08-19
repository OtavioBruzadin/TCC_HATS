"""
Compara as saídas do pacote atual com as dos protótipos em prototypes/.

Roda cada versão sobre os mesmos dados e confronta os CSV byte a byte e os
relatórios JSON chave a chave.

    python3 tools/compare_versions.py
    python3 tools/compare_versions.py --day-dir Data/2026-03-17
    python3 tools/compare_versions.py --versions v4 v5

Diferenças esperadas, e por quê:

  v3   não demodula, não corrige as unidades do AUX e não detecta os registros
       defasados. As divergências são o motivo pelo qual ele foi substituído.
  v4   equivalente ao pacote no caminho stdlib; espera-se diferença nenhuma nos
       CSV e só nos textos de duas notas nos JSON.
  v5   idem, mais o backend numpy.
"""

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# nome -> (script, argumentos extras)
PROTOTYPES = {
    "v3": ("hats_report_v3.py", ["--from-data-dir"]),
    "v4": ("hats_report_v4.py", ["--from-data-dir"]),
    "v5": ("hats_report_v5.py", ["--from-data-dir"]),
}


def run_package(project_root, reports_dir, export_rbd_csv, backend):
    command = [sys.executable, str(PROJECT_ROOT / "hats_report.py"),
               "--project-root", str(project_root), "--reports-dir", str(reports_dir),
               "--export-csv", "--backend", backend]
    if export_rbd_csv:
        command.append("--export-rbd-csv")
    return subprocess.run(command, capture_output=True, text=True)


def run_prototype(name, project_root, reports_dir, export_rbd_csv):
    script, extra = PROTOTYPES[name]
    command = [sys.executable, script, "--project-root", str(project_root),
               "--reports-dir", str(reports_dir), "--export-csv"] + extra
    if export_rbd_csv and name in ("v4", "v5"):
        command.append("--export-rbd-csv")
    return subprocess.run(command, capture_output=True, text=True,
                          cwd=str(PROJECT_ROOT / "prototypes"))


def compare_csv(left, right):
    """Devolve linhas de relatório comparando as pastas de CSV."""
    lines = []
    left_files = {p.name for p in left.glob("*.csv")} if left.exists() else set()
    right_files = {p.name for p in right.glob("*.csv")} if right.exists() else set()

    for name in sorted(left_files | right_files):
        if name not in left_files:
            lines.append(("  {:<34} só no protótipo".format(name), False))
            continue
        if name not in right_files:
            lines.append(("  {:<34} só no pacote".format(name), False))
            continue
        same = (left / name).read_bytes() == (right / name).read_bytes()
        if same:
            lines.append(("  {:<34} byte-idêntico".format(name), True))
        else:
            detail = numeric_difference(left / name, right / name)
            lines.append(("  {:<34} difere{}".format(name, detail), False))
    return lines


def numeric_difference(left, right):
    """Se os CSV têm as mesmas colunas numéricas, quantifica a maior diferença."""
    import csv

    with left.open(encoding="utf-8") as one, right.open(encoding="utf-8") as other:
        rows_left = list(csv.DictReader(one))
        rows_right = list(csv.DictReader(other))

    if len(rows_left) != len(rows_right):
        return "  ({} linhas contra {})".format(len(rows_left), len(rows_right))

    shared = [c for c in rows_left[0] if c in rows_right[0]] if rows_left else []
    worst = 0.0
    worst_column = None
    text_differences = set()
    for a, b in zip(rows_left, rows_right):
        for column in shared:
            if a[column] == b[column]:
                continue
            try:
                delta = abs(float(a[column]) - float(b[column]))
                if delta > worst:
                    worst, worst_column = delta, column
            except ValueError:
                text_differences.add(column)

    parts = []
    if worst_column:
        parts.append("máx {:.2e} em {}".format(worst, worst_column))
    if text_differences:
        parts.append("texto em {}".format(", ".join(sorted(text_differences))))
    missing = [c for c in rows_left[0] if c not in rows_right[0]] if rows_left else []
    extra = [c for c in rows_right[0] if c not in rows_left[0]] if rows_right else []
    if missing:
        parts.append("colunas só no protótipo: {}".format(", ".join(missing)))
    if extra:
        parts.append("colunas só no pacote: {}".format(", ".join(extra)))
    return "  ({})".format("; ".join(parts)) if parts else ""


def compare_json(left, right):
    lines = []
    for name in sorted({p.name for p in left.glob("*.json")} & {p.name for p in right.glob("*.json")}):
        one = json.loads((left / name).read_text(encoding="utf-8"))
        other = json.loads((right / name).read_text(encoding="utf-8"))
        differing = sorted(k for k in set(one) | set(other) if one.get(k) != other.get(k))
        if differing:
            lines.append(("  {:<38} chaves diferentes: {}".format(name, ", ".join(differing)), False))
        else:
            lines.append(("  {:<38} idêntico".format(name), True))
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--versions", nargs="+", default=["v4", "v5"],
                        choices=sorted(PROTOTYPES), help="Protótipos a comparar.")
    parser.add_argument("--backend", choices=["auto", "numpy", "stdlib"], default="stdlib",
                        help="Backend do pacote. stdlib é o comparável direto com os protótipos.")
    parser.add_argument("--export-rbd-csv", action="store_true")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    workspace = Path(tempfile.mkdtemp(prefix="hats-versions-"))
    failures = 0

    try:
        package_reports = workspace / "package"
        result = run_package(project_root, package_reports, args.export_rbd_csv, args.backend)
        if result.returncode != 0:
            print("O pacote falhou:\n{}".format(result.stderr))
            return 1
        print("pacote hats (backend {}): ok".format(args.backend))

        for version in args.versions:
            print()
            print("=" * 72)
            print(" pacote  vs  {}".format(version))
            print("=" * 72)
            reports = workspace / version
            result = run_prototype(version, project_root, reports, args.export_rbd_csv)
            if result.returncode != 0:
                print("  {} falhou ao rodar:\n{}".format(version, result.stderr.strip()[:400]))
                failures += 1
                continue

            print(" CSV")
            for line, ok in compare_csv(reports / "csv", package_reports / "csv"):
                print(line)
                if not ok and version in ("v4", "v5"):
                    failures += 1
            print(" JSON")
            for line, ok in compare_json(reports / "json", package_reports / "json"):
                print(line)

        print()
        if failures:
            print("{} divergência(s) inesperada(s) em relação ao v4/v5.".format(failures))
        else:
            print("Nenhuma divergência inesperada.")
        return 0
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
