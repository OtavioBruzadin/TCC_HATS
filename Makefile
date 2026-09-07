# Atalhos do projeto. Rode `make` sem argumentos para ver a lista.
#
# Os alvos que executam o código do CRAAM precisam do ambiente de referência:
# rode `make setup-reference` uma vez.

# Interpretador. O pacote roda com a biblioteca padrão, mas fica cerca de 16x
# mais rápido no processamento quando numpy está disponível — por isso, quando o
# python3 do sistema não tem numpy e existe um ambiente com ele, o segundo é
# preferido. Passe PYTHON= para escolher à mão.
PYTHON  ?= $(shell python3 -c "import numpy" >/dev/null 2>&1 && echo python3 \
             || { test -x .refvenv/bin/python && echo .refvenv/bin/python; } \
             || echo python3)
REFVENV ?= .refvenv
REFPY    = $(REFVENV)/bin/python
DAY     ?= 2026-03-17
HOUR    ?= 1800
AMOSTRA ?= Amostra

# Onde estão os dados. A ordem de precedência é: DATA= na linha de comando,
# depois local.mk, depois a variável HATS_DATA_InputPath do manual do CRAAM.
#
# Não há queda para a amostra. Um padrão silencioso que usasse os 1000 registros
# de exemplo faria uma comparação passar sem provar nada — foi assim que duas
# divergências reais ficaram escondidas por uma sessão inteira. Para a amostra
# existem alvos próprios, com -demo no nome.
-include local.mk
DATA    ?= $(HATS_DATA_InputPath)
ZIP     ?= $(HOME)/Downloads/HATS_software.zip

.DEFAULT_GOAL := help
.PHONY: help test run run-diag run-dia run-demo diff-demo erro erro-demo run-craam diff clean config-data check-data bench bench-craam craam-shell verify-binario setup-reference check-reference

help:
	@echo ""
	@echo "  Uso: make <alvo>"
	@echo ""
	@echo "    make run                 roda o NOSSO, uma hora  -> Saida/"
	@echo "    make run-diag            idem, com os relatórios -> Diagnostico/"
	@echo "    make run-dia             roda o NOSSO, o dia todo -> Saida/"
	@echo "    make run-craam           roda o do CRAAM         -> SaidaCRAAM/"
	@echo "    make diff                roda os dois e compara as duas pastas"
	@echo "    make erro                mede o erro coluna a coluna: RMSE, máximos, viés"
	@echo ""
	@echo "  Sobre a amostra de 1000 registros, sem precisar dos dados reais"
	@echo "    make run-demo            roda o NOSSO sobre Amostra/"
	@echo "    make diff-demo           compara os dois sobre Amostra/"
	@echo "    make erro-demo           mede o erro sobre Amostra/"
	@echo ""
	@echo "    make test                suíte de testes"
	@echo "    make bench               desempenho dos backends"
	@echo "    make bench-craam         idem, incluindo o HATS.py do CRAAM"
	@echo "    make craam-shell         sessão Python com o HATS.py deles carregado"
	@echo "    make verify-binario      confronta com o binário ELF que o CRAAM distribui"
	@echo "    make clean               apaga Saida/ e SaidaCRAAM/"
	@echo ""
	@echo "    make setup-reference     monta o ambiente do CRAAM (venv + HATS_fft)"
	@echo "    make config-data DATA=... grava o caminho dos dados em local.mk"
	@echo ""
	@echo "  Variáveis: DAY=$(DAY)  HOUR=$(HOUR)  PYTHON=$(PYTHON)"
	@echo "             DATA=$(if $(DATA),$(DATA),<não configurado — veja make config-data>)"
	@echo ""

# run e run-craam processam a MESMA hora, para que o diff compare pastas de
# mesmo conteúdo. Para o dia inteiro, use make run-dia.
run: check-data
	@rm -rf Saida
	$(PYTHON) hats_report.py --data-dir "$(DATA)" --day $(DAY) --hour $(HOUR)

run-diag: check-data
	@rm -rf Saida Diagnostico
	$(PYTHON) hats_report.py --data-dir "$(DATA)" --day $(DAY) --hour $(HOUR) --diagnostico

run-dia: check-data
	@rm -rf Saida
	$(PYTHON) hats_report.py --data-dir "$(DATA)" --day $(DAY)

run-demo:
	@$(MAKE) --no-print-directory run DATA=$(AMOSTRA)

# Roda os dois e mede o erro. O diff diz "igual ou diferente"; isto diz quanto.
erro: check-reference check-data
	@$(MAKE) --no-print-directory run >/dev/null
	@$(MAKE) --no-print-directory run-craam >/dev/null
	@$(PYTHON) tools/erro.py SaidaCRAAM Saida

erro-demo:
	@$(MAKE) --no-print-directory erro DATA=$(AMOSTRA)

diff-demo:
	@$(MAKE) --no-print-directory diff DATA=$(AMOSTRA)

# O caminho é absolutizado no shell, não com $(abspath): a função do make trata
# o valor como lista separada por espaço, e um diretório com espaço no nome
# viraria dois caminhos.
run-craam: check-reference check-data
	@rm -rf SaidaCRAAM
	@D="$(DATA)"; case "$$D" in /*) ;; *) D="$(CURDIR)/$$D" ;; esac; \
	 HATS_DATA_InputPath="$$D" OUTDIR="$(CURDIR)/SaidaCRAAM" \
	     tools/craam_csv.sh $(DAY) $(HOUR)

diff: check-reference check-data
	@$(MAKE) --no-print-directory run >/dev/null
	@$(MAKE) --no-print-directory run-craam >/dev/null
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

config-data:
	@test -n "$(DATA)" || { echo "Use: make config-data DATA=/caminho/para/os/dados"; exit 1; }
	@test -d "$(DATA)" || { echo "Diretório não encontrado: $(DATA)"; exit 1; }
	@D="$(DATA)"; case "$$D" in /*) ;; *) D="$(CURDIR)/$$D" ;; esac; \
	 printf 'DATA = %s\n' "$$D" > local.mk; \
	 echo "Gravado em local.mk:"; echo "  DATA = $$D"

check-data:
	@test -n "$(DATA)" || { \
	  echo ""; \
	  echo "  Não sei onde estão os dados."; \
	  echo ""; \
	  echo "  Configure uma vez:"; \
	  echo "    make config-data DATA=/caminho/para/os/dados"; \
	  echo ""; \
	  echo "  Ou exporte a variável que o manual do CRAAM usa:"; \
	  echo "    export HATS_DATA_InputPath=/caminho/para/os/dados"; \
	  echo ""; \
	  echo "  Para experimentar sem os dados reais:  make run-demo"; \
	  echo ""; exit 1; }
	@test -d "$(DATA)" || { echo "Diretório de dados não encontrado: $(DATA)"; exit 1; }

setup-reference:
	tools/setup_reference.sh

check-reference:
	@test -x $(REFPY) || { \
	  echo ""; \
	  echo "  Ambiente de referência não encontrado em $(REFVENV)."; \
	  echo "  Rode:  make setup-reference"; \
	  echo ""; exit 1; }
