# Atalhos do projeto. Rode `make` sem argumentos para ver a lista.
#
# Os alvos que falam com o pipeline original do CRAAM precisam do ambiente de
# referência: rode `make setup-reference` uma vez.

PYTHON  ?= python3
REFVENV ?= .refvenv
REFPY    = $(REFVENV)/bin/python
DAY      ?= 2026-03-17
DECIMALS ?= 6
HOUR    ?= 1800
DAYDIR  ?= Data/$(DAY)

.DEFAULT_GOAL := help
.PHONY: help test run run-full clean bench bench-craam compare-versions compare-craam side-by-side craam-shell run-nosso run-craam run-ambos run-alinhado csv-craam csv-nosso diff setup-reference

help:
	@echo ""
	@echo "  Uso: make <alvo>"
	@echo ""
	@echo "  Rodar"
	@echo "    make run                 processa o Data/ e gera os relatórios"
	@echo "    make run-full            idem, incluindo o CSV do sinal bruto de 1 kHz"
	@echo "    make clean               apaga o Reports/"
	@echo ""
	@echo "  Rodar os dois pipelines separados, lado a lado"
	@echo "    make run-nosso           só o nosso   -> Reports/nosso/"
	@echo "    make run-craam           só o do CRAAM -> Reports/craam/"
	@echo "    make run-ambos           os dois, e lista as duas pastas"
	@echo "    make run-alinhado        o nosso replicando o descarte do HATS.py,"
	@echo "                             para as janelas caírem na mesma grade dele"
	@echo ""
	@echo "  Gerar as duas tabelas para comparar com diff"
	@echo "    make csv-craam           -> Reports/diff/craam.csv"
	@echo "    make csv-nosso           -> Reports/diff/nosso.csv"
	@echo "    make diff                gera as duas e mostra o diff"
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
	@echo "  Rodar o código original do CRAAM"
	@echo "    make craam-shell         sessão Python com o HATS.py deles carregado"
	@echo ""
	@echo "  Preparar"
	@echo "    make setup-reference     monta o ambiente do CRAAM (venv + HATS_fft)"
	@echo ""
	@echo "  Variáveis: DAY=$(DAY)  HOUR=$(HOUR)  DECIMALS=$(DECIMALS)  PYTHON=$(PYTHON)"
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

craam-shell: check-reference
	tools/craam_shell.sh $(DAY) $(HOUR)

run-nosso:
	@rm -rf Reports/nosso
	$(PYTHON) hats_report.py --reports-dir Reports/nosso --export-csv --export-rbd-csv
	@echo ""
	@echo "  Reports/nosso/"
	@find Reports/nosso -type f | sort | sed 's|^|    |'

run-craam: check-reference
	@rm -rf Reports/craam
	OUTDIR=$(CURDIR)/Reports/craam tools/craam_csv.sh $(DAY) $(HOUR)

csv-craam: check-reference
	@$(REFPY) tools/deconv_csv.py --source craam --day $(DAY) --hour $(HOUR) \
	    --decimals $(DECIMALS) --out Reports/diff/craam.csv

csv-nosso:
	@$(PYTHON) tools/deconv_csv.py --source nosso --day $(DAY) --hour $(HOUR) \
	    --decimals $(DECIMALS) --out Reports/diff/nosso.csv

diff: csv-craam csv-nosso
	@echo ""
	@echo "  diff Reports/diff/craam.csv Reports/diff/nosso.csv"
	@echo "  ----------------------------------------------------------------"
	@diff Reports/diff/craam.csv Reports/diff/nosso.csv && echo "  (sem diferenças)" || true

run-alinhado:
	@rm -rf Reports/alinhado
	$(PYTHON) hats_report.py --reports-dir Reports/alinhado --export-csv --drop-before-hour
	@echo ""
	@echo "  Reports/alinhado/   comparável linha a linha com Reports/craam/"

run-ambos: run-nosso run-craam
	@echo ""
	@echo "  ================ os dois pipelines, saídas separadas ================"
	@echo "  Reports/nosso/   este projeto"
	@echo "  Reports/craam/   HATS.py do CRAAM"

setup-reference:
	tools/setup_reference.sh

.PHONY: check-reference
check-reference:
	@test -x $(REFPY) || { \
	  echo ""; \
	  echo "  Ambiente de referência não encontrado em $(REFVENV)."; \
	  echo "  Rode:  make setup-reference"; \
	  echo ""; exit 1; }
