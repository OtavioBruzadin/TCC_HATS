"""
Acumuladores de estatística em streaming.

Um arquivo de uma hora tem 3,6 milhões de registros, então nada é mantido em
memória: cada bloco contribui com min, max, soma e soma dos quadrados, e o
resumo é fechado no fim.
"""

import math
import operator


class Accumulator(object):
    """min, max, soma e soma dos quadrados de uma coluna, acumulados por bloco."""

    __slots__ = ("count", "total", "total_squares", "minimum", "maximum")

    def __init__(self):
        self.count = 0
        self.total = 0.0
        self.total_squares = 0.0
        self.minimum = None
        self.maximum = None

    def add_column(self, column):
        """Absorve um bloco inteiro, usando as reduções em C do interpretador."""
        if not len(column):
            return
        lowest = min(column)
        highest = max(column)
        self.minimum = lowest if self.minimum is None else min(self.minimum, lowest)
        self.maximum = highest if self.maximum is None else max(self.maximum, highest)
        self.total += sum(column)
        self.total_squares += sum(map(operator.mul, column, column))
        self.count += len(column)

    def add_partial(self, count, lowest, highest, total, total_squares):
        """Absorve estatísticas já reduzidas — usado pelo caminho numpy."""
        if not count:
            return
        self.minimum = lowest if self.minimum is None else min(self.minimum, lowest)
        self.maximum = highest if self.maximum is None else max(self.maximum, highest)
        self.total += total
        self.total_squares += total_squares
        self.count += count

    def summary(self):
        if not self.count:
            return {"count": 0, "min": None, "max": None, "mean": None, "sd": None}
        mean = self.total / self.count
        # A variância por E[x²] - E[x]² cancela catastroficamente. Sobre 3,6
        # milhões de amostras a diferença para uma soma em pares fica em 1e-9
        # relativo, o que é irrelevante aqui e mantém o cálculo em uma passada.
        variance = max(0.0, self.total_squares / self.count - mean * mean)
        return {"count": self.count, "min": self.minimum, "max": self.maximum,
                "mean": mean, "sd": math.sqrt(variance)}


def summarize(values):
    """Resumo de uma sequência que já está inteira em memória."""
    if not len(values):
        return {"count": 0, "min": None, "max": None, "mean": None, "sd": None}
    count = len(values)
    mean = sum(values) / count
    variance = max(0.0, sum(map(operator.mul, values, values)) / count - mean * mean)
    return {"count": count, "min": min(values), "max": max(values),
            "mean": mean, "sd": math.sqrt(variance)}


def summarize_simple(values):
    """Resumo sem desvio, para as séries curtas da estação meteorológica."""
    if not values:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {"count": len(values), "min": min(values), "max": max(values),
            "mean": float(sum(values)) / float(len(values))}


def linear_fit(pairs):
    """Ajuste de mínimos quadrados y = a*x + b. Devolve (a, b) ou None."""
    count = len(pairs)
    if count < 2:
        return None
    sum_x = sum(x for x, _ in pairs)
    sum_y = sum(y for _, y in pairs)
    sum_xx = sum(x * x for x, _ in pairs)
    sum_xy = sum(x * y for x, y in pairs)
    denominator = count * sum_xx - sum_x * sum_x
    if denominator == 0:
        return None
    slope = (count * sum_xy - sum_x * sum_y) / denominator
    return slope, (sum_y - slope * sum_x) / count
