# Atalhos do projeto. Rode `make` sem argumentos para ver a lista.
#
# Os alvos que executam o código do CRAAM precisam do ambiente de referência:
# rode `make setup-reference` uma vez.

PYTHON  ?= python3
REFVENV ?= .refvenv
REFPY    = $(REFVENV)/bin/python
DAY     ?= 2026-03-17
HOUR    ?= 1800
ZIP     ?= $(HOME)/Downloads/HATS_software.zip

.DEFAULT_GOAL := help
.PHONY: help test run run-craam diff clean bench bench-craam craam-shell verify-binario setup-reference check-reference

help:
	@echo ""
	@echo "  Uso: make <alvo>"
	@echo ""
	@echo "    make run                 roda o NOSSO      -> Saida/ + Diagnostico/"
	@echo "    make run-craam           roda o do CRAAM   -> SaidaCRAAM/"
	@echo "    make diff                roda os dois e compara as duas pastas"
	@echo ""
	@echo "    make test                suíte de testes"
	@echo "    make bench               desempenho dos backends"
	@echo "    make bench-craam         idem, incluindo o HATS.py do CRAAM"
	@echo "    make craam-shell         sessão Python com o HATS.py deles carregado"
	@echo "    make verify-binario      confronta com o binário ELF que o CRAAM distribui"
	@echo "    make clean               apaga Saida/ e SaidaCRAAM/"
	@echo ""
	@echo "    make setup-reference     monta o ambiente do CRAAM (venv + HATS_fft)"
	@echo ""
	@echo "  Variáveis: DAY=$(DAY)  HOUR=$(HOUR)  PYTHON=$(PYTHON)"
	@echo ""

run:
	$(PYTHON) hats_report.py --day $(DAY)

run-craam: check-reference
	@rm -rf SaidaCRAAM
	@OUTDIR=$(CURDIR)/SaidaCRAAM tools/craam_csv.sh $(DAY) $(HOUR)

diff: check-reference
	@rm -rf Saida SaidaCRAAM
	@$(MAKE) --no-print-directory run >/dev/null
	@OUTDIR=$(CURDIR)/SaidaCRAAM tools/craam_csv.sh $(DAY) $(HOUR) >/dev/null
	@echo ""
	@echo "  diff -r SaidaCRAAM Saida"
	@echo "  ----------------------------------------------------------------"
	@if diff -r SaidaCRAAM Saida; then echo "  (sem diferenças — idênticas byte a byte)"; fi

test:
	$(PYTHON) -m unittest discover -s tests -t . -v

bench:
	$(PYTHON) tools/benchmark.py

bench-craam: check-reference
	$(REFPY) tools/benchmark.py --with-craam

verify-binario: check-reference
	tools/verify_binary.sh $(ZIP)

craam-shell: check-reference
	tools/craam_shell.sh $(DAY) $(HOUR)

clean:
	rm -rf Saida SaidaCRAAM Diagnostico

setup-reference:
	tools/setup_reference.sh

check-reference:
	@test -x $(REFPY) || { \
	  echo ""; \
	  echo "  Ambiente de referência não encontrado em $(REFVENV)."; \
	  echo "  Rode:  make setup-reference"; \
	  echo ""; exit 1; }
