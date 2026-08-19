# Atalhos do projeto. Rode `make` sem argumentos para ver a lista.
#
# Os alvos que falam com o pipeline original do CRAAM precisam do ambiente de
# referência: rode `make setup-reference` uma vez.

PYTHON  ?= python3
REFVENV ?= .refvenv
REFPY    = $(REFVENV)/bin/python
DAY     ?= 2026-03-17
HOUR    ?= 1800
DAYDIR  ?= Data/$(DAY)

.DEFAULT_GOAL := help
.PHONY: help test run run-full clean bench bench-craam compare-versions compare-craam side-by-side setup-reference

help:
	@echo ""
	@echo "  Uso: make <alvo>"
	@echo ""
	@echo "  Rodar"
	@echo "    make run                 processa o Data/ e gera os relatórios"
	@echo "    make run-full            idem, incluindo o CSV do sinal bruto de 1 kHz"
	@echo "    make clean               apaga o Reports/"
	@echo ""
	@echo "  Testar"
	@echo "    make test                suíte completa (não precisa de dados nem de numpy)"
	@echo ""
	@echo "  Desempenho"
	@echo "    make bench               stdlib contra numpy, sobre uma hora sintética"
	@echo "    make bench-craam         idem, incluindo o HATS.py do CRAAM"
	@echo ""
	@echo "  Comparar resultados"
	@echo "    make compare-versions    pacote contra os protótipos v4 e v5"
	@echo "    make compare-craam       pacote contra o HATS.py do CRAAM, campo a campo"
	@echo "    make side-by-side        uma linha de cada, lado a lado no terminal"
	@echo ""
	@echo "  Preparar"
	@echo "    make setup-reference     monta o ambiente do CRAAM (venv + HATS_fft)"
	@echo ""
	@echo "  Variáveis: DAY=$(DAY)  HOUR=$(HOUR)  PYTHON=$(PYTHON)"
	@echo ""

test:
	$(PYTHON) -m unittest discover -s tests -t . -v

run:
	$(PYTHON) hats_report.py --export-csv

run-full:
	$(PYTHON) hats_report.py --export-csv --export-rbd-csv

clean:
	rm -rf Reports

bench:
	$(PYTHON) tools/benchmark.py

bench-craam: check-reference
	$(REFPY) tools/benchmark.py --with-craam

compare-versions:
	$(PYTHON) tools/compare_versions.py

compare-craam: check-reference
	$(REFPY) tools/compare_with_reference.py --day-dir $(DAYDIR) --date $(DAY) --hour $(HOUR)

side-by-side: check-reference
	$(REFPY) tools/side_by_side.py --day-dir $(DAYDIR) --date $(DAY) --hour $(HOUR)

setup-reference:
	tools/setup_reference.sh

.PHONY: check-reference
check-reference:
	@test -x $(REFPY) || { \
	  echo ""; \
	  echo "  Ambiente de referência não encontrado em $(REFVENV)."; \
	  echo "  Rode:  make setup-reference"; \
	  echo ""; exit 1; }
