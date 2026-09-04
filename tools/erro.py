"""
Mede o erro entre duas pastas de saída, coluna a coluna.

    python3 tools/erro.py SaidaCRAAM Saida

O `diff` responde "igual ou diferente". Isto responde "quanto", que é o que se
cita num texto: erro quadrático médio, erro absoluto e relativo máximos, viés, e
quantos valores efetivamente diferem.

Com as duas saídas byte a byte iguais todas as métricas dão exatamente zero — o
que é uma afirmação mais forte, e mais verificável, do que a ausência de saída do
`diff`.

As tabelas são percorridas em fluxo, sem carregar nada inteiro: o `-rbd_cal.csv`
de uma hora tem 3,6 milhões de linhas e 225 MB.

Colunas de texto, como o carimbo de tempo, entram só na contagem de divergências;
não faz sentido subtrair carimbos.
"""

import argparse
import csv
import math
import sys
from pathlib import Path


class ColumnError(object):
    """Acumula as métricas de uma coluna em uma passada."""

    __slots__ = ("name", "count", "differing", "sum_squares", "sum_signed",
                 "max_absolute", "max_relative", "numeric", "worst_row")

    def __init__(self, name):
        self.name = name
        self.count = 0
        self.differing = 0
        self.sum_squares = 0.0
        self.sum_signed = 0.0
        self.max_absolute = 0.0
        self.max_relative = 0.0
        self.numeric = True
        self.worst_row = None

    def add(self, reference_text, ours_text, row_number):
        self.count += 1
        if reference_text != ours_text:
            self.differing += 1
        try:
            reference = float(reference_text)
            ours = float(ours_text)
        except ValueError:
            self.numeric = False
            return
        difference = ours - reference
        self.sum_signed += difference
        self.sum_squares += difference * difference
        magnitude = abs(difference)
        if magnitude > self.max_absolute:
            self.max_absolute = magnitude
            self.worst_row = row_number
        if reference:
            relative = magnitude / abs(reference)
            if relative > self.max_relative:
                self.max_relative = relative

    def summary(self):
        if not self.count:
            return None
        report = {"coluna": self.name, "linhas": self.count, "diferentes": self.differing}
        if self.numeric:
            report.update({
                "rmse": math.sqrt(self.sum_squares / self.count),
                "vies": self.sum_signed / self.count,
                "erro_abs_max": self.max_absolute,
                "erro_rel_max": self.max_relative,
                "linha_do_pior": self.worst_row,
            })
        return report


def compare_table(reference_path, ours_path):
    """Percorre as duas tabelas em paralelo e devolve as métricas por coluna."""
    with reference_path.open(encoding="utf-8") as one, ours_path.open(encoding="utf-8") as other:
        reference_rows = csv.reader(one)
        our_rows = csv.reader(other)

        reference_header = next(reference_rows, None)
        our_header = next(our_rows, None)
        if reference_header != our_header:
            return {"erro": "cabeçalhos diferentes",
                    "referencia": reference_header, "nosso": our_header}

        columns = [ColumnError(name) for name in reference_header]
        row_number = 0
        extra_reference = extra_ours = 0

        while True:
            left = next(reference_rows, None)
            right = next(our_rows, None)
            if left is None and right is None:
                break
            if left is None:
                extra_ours += 1 + sum(1 for _ in our_rows)
                break
            if right is None:
                extra_reference += 1 + sum(1 for _ in reference_rows)
                break
            row_number += 1
            for column, one_value, other_value in zip(columns, left, right):
                column.add(one_value, other_value, row_number)

    return {"colunas": [c.summary() for c in columns if c.summary()],
            "linhas_so_na_referencia": extra_reference,
            "linhas_so_no_nosso": extra_ours}


def format_table(name, result):
    lines = ["", "  {}".format(name), "  " + "-" * 92]
    if "erro" in result:
        lines.append("    {}".format(result["erro"]))
        lines.append("    referência: {}".format(result["referencia"]))
        lines.append("    nosso     : {}".format(result["nosso"]))
        return lines

    if result["linhas_so_na_referencia"] or result["linhas_so_no_nosso"]:
        lines.append("    ATENÇÃO: {} linhas só na referência, {} só no nosso".format(
            result["linhas_so_na_referencia"], result["linhas_so_no_nosso"]))

    lines.append("    {:<18} {:>10} {:>12} {:>13} {:>13} {:>13}".format(
        "coluna", "linhas", "diferentes", "RMSE", "erro abs máx", "erro rel máx"))
    for column in result["colunas"]:
        if "rmse" in column:
            lines.append("    {:<18} {:>10} {:>12} {:>13.6e} {:>13.6e} {:>13.6e}".format(
                column["coluna"], column["linhas"], column["diferentes"],
                column["rmse"], column["erro_abs_max"], column["erro_rel_max"]))
        else:
            lines.append("    {:<18} {:>10} {:>12} {:>13} {:>13} {:>13}".format(
                column["coluna"], column["linhas"], column["diferentes"],
                "(texto)", "—", "—"))
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("referencia", help="Pasta com a saída do CRAAM.")
    parser.add_argument("nosso", help="Pasta com a nossa saída.")
    args = parser.parse_args()

    reference_dir = Path(args.referencia)
    ours_dir = Path(args.nosso)
    for folder in (reference_dir, ours_dir):
        if not folder.is_dir():
            raise SystemExit("Pasta não encontrada: {}".format(folder))

    reference_files = {p.name for p in reference_dir.glob("*.csv")}
    our_files = {p.name for p in ours_dir.glob("*.csv")}

    print()
    print("  referência : {}".format(reference_dir))
    print("  nosso      : {}".format(ours_dir))

    for name in sorted(reference_files - our_files):
        print("  FALTANDO no nosso: {}".format(name))
    for name in sorted(our_files - reference_files):
        print("  só no nosso      : {}".format(name))

    perfect = True
    total_rows = 0
    for name in sorted(reference_files & our_files):
        result = compare_table(reference_dir / name, ours_dir / name)
        for line in format_table(name, result):
            print(line)
        if "erro" in result:
            perfect = False
            continue
        total_rows += max((c["linhas"] for c in result["colunas"]), default=0)
        if (result["linhas_so_na_referencia"] or result["linhas_so_no_nosso"]
                or any(c["diferentes"] for c in result["colunas"])):
            perfect = False

    print()
    print("  " + "=" * 92)
    if perfect:
        print("  {} linhas comparadas. Erro exatamente zero em todas as colunas.".format(total_rows))
    else:
        print("  Há divergências. Veja as colunas com 'diferentes' maior que zero.")
    print()
    return 0 if perfect else 1


if __name__ == "__main__":
    sys.exit(main())
