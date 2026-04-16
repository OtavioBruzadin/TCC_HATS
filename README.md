## Como rodar? 
```python hats_report_v2.py --from-data-dir --export-csv```



# TCC_HATS

Projeto para processamento e análise de arquivos gerados pelo instrumento HATS.

---

## Estrutura do Projeto

A pasta `Data/` **não é versionada no repositório** e deve ser criada manualmente seguindo a estrutura abaixo:

```TCC_HATS/
├── .venv/
├── Data/
│   ├── 2026-03-17/
│   │   ├── hats-2026-03-17.ws
│   │   ├── hats-2026-03-17T1100.au
│   │   ├── hats-2026-03-17T1300.rbd
│   │   └── …
│   ├── 2026-03-18/
│   │   ├── hats-2026-03-18.ws
│   │   ├── hats-2026-03-18T1100.au
│   │   ├── hats-2026-03-18T1300.rbd
│   │   └── …
│   └── …
├── src/
├── scripts/
└── README.md
```


---

## ▶Como usar

1. Crie a pasta `Data/` na raiz do projeto
2. Adicione os arquivos organizados por data
3. Execute o script de análise normalmente

---

## Observação

Os dados do HATS não são incluídos no repositório devido ao tamanho dos arquivos.  
Você deve obter esses arquivos separadamente e colocá-los manualmente na pasta `Data/`.


## Link para Drive contendo as pastas Data e Relatorio de exemplo
https://drive.google.com/drive/folders/1_aWg-CdfVP4UcG06CKtlhWi68MCRzkFz?usp=sharing