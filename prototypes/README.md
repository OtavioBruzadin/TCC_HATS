# Protótipos

Versões anteriores, mantidas como registro da evolução do projeto. **Nenhuma delas é
usada em produção** — o código ativo é o pacote `hats/`, com entrada em
`hats_report.py`.

| arquivo | o que era | por que foi substituído |
|---|---|---|
| `hats_report_v2.py` | primeira leitura dos binários | ordem de campos errada: desempacotava `temp_env` antes de `temp_hics`, ao contrário do XML |
| `hats_report_v3.py` | esquema vindo do XML, exportação CSV/JSON | sem demodulação; unidades do AUX erradas; registros defasados não detectados; sem estatística de arquivo inteiro |
| `hats_report_v4.py` | correções de unidade, filtro de defasados, Goertzel, otimizações stdlib | virou o pacote `hats/` |
| `hats_report_v5.py` | backend numpy opcional sobre o v4 | virou `hats/backends.py` e as funções `*_numpy` do pacote |

A sequência de descobertas que levou de um ao outro está documentada no README
principal, nas seções "Correções aplicadas" e "Validação".

Para rodar um protótipo, é preciso executá-lo de dentro desta pasta ou ajustar o
`sys.path` — o `v5` importa o `v4` por nome.
