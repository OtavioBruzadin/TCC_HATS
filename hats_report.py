#!/usr/bin/env python3
"""
Entrada do processamento de dados do HATS.

    python3 hats_report.py --export-csv
    python3 hats_report.py --day 2026-03-17 --export-csv
    python3 hats_report.py --backends

A implementação está no pacote `hats/`; este arquivo só repassa os argumentos.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from hats.cli import main

if __name__ == "__main__":
    sys.exit(main())
