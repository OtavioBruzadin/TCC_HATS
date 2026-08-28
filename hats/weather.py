"""
Leitura dos arquivos .ws da estação meteorológica.

ASCII, uma linha a cada cinco segundos, no formato

    2026-03-17T00:00:00,0R2,Ta=18.4C,Ua=9.0P,Pa=757.5H

Os arquivos brutos contêm linhas com um byte 0x7f colado antes do carimbo de
tempo, cerca de uma por dia. O carimbo é limpo e validado em vez de repassado
como texto, e as linhas irrecuperáveis são descartadas e contadas.
"""

from datetime import datetime

from hats import statistics

CONTROL_CHARACTERS = {code: None for code in list(range(0, 32)) + [127]}


def parse_measurement(token, unit_suffix):
    """
    Lê um campo no formato 'Xx=12.3U'.

    A unidade é localizada, não assumida com um caractere: o campo de pressão
    termina em 'HPa' em alguns firmwares e em 'H' em outros.
    """
    body = token.split("=", 1)[1]
    cut = body.find(unit_suffix)
    if cut < 0:
        cut = len(body) - 1
    return float(body[:cut])


def parse_line(line):
    """Uma linha -> dicionário, ou None se não for aproveitável."""
    parts = line.strip().split(",")
    if len(parts) != 5:
        return None
    try:
        stamp = parts[0].translate(CONTROL_CHARACTERS).strip()
        parsed = datetime.fromisoformat(stamp)
        return {
            "time": parsed.isoformat(),
            "station_code": parts[1].translate(CONTROL_CHARACTERS).strip(),
            "temperature_c": parse_measurement(parts[2], "C"),
            "humidity": parse_measurement(parts[3], "P"),
            "pressure_hpa": parse_measurement(parts[4], "H"),
            "repaired": stamp != parts[0].strip(),
        }
    except (ValueError, IndexError):
        return None


def read(path):
    """Devolve (linhas, rejeitadas, carimbos recuperados)."""
    rows = []
    rejected = 0
    repaired = 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = parse_line(line)
            if row is None:
                rejected += 1
                continue
            if row.pop("repaired"):
                repaired += 1
            rows.append(row)
    return rows, rejected, repaired


def analyse(path, sample_count):
    """Relatório do dia da estação meteorológica."""
    from collections import Counter

    rows, rejected, repaired = read(path)
    sampled = (rows[:sample_count] + rows[-sample_count:]) if len(rows) > sample_count else rows

    return {
        "total_valid_rows": len(rows),
        "rejected_lines": rejected,
        "repaired_timestamps": repaired,
        "first_time": rows[0]["time"] if rows else None,
        "last_time": rows[-1]["time"] if rows else None,
        "sampled_rows": sampled,
        "station_code_counts": dict(Counter(row["station_code"] for row in rows)),
        "stats": {
            "temperature_c": statistics.summarize_simple([r["temperature_c"] for r in rows]),
            "humidity": statistics.summarize_simple([r["humidity"] for r in rows]),
            "pressure_hpa": statistics.summarize_simple([r["pressure_hpa"] for r in rows]),
        },
    }
