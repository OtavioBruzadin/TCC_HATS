#!/usr/bin/env python3
"""
Converte o dicionário de dados em HTML, para virar PDF ou .docx sem anexo feio.

Não é uma implementação de Markdown: cobre exatamente o que o
`dicionario_de_dados.py` emite — títulos, parágrafos, tabelas de pipe, blocos de
código cercados, listas numeradas, `código` e **negrito** em linha. Qualquer coisa
além disso sai como parágrafo, que é o comportamento menos surpreendente.

Existe porque a máquina não tem pandoc e instalar um conversor para gerar um anexo
seria desproporcional: o `textutil` do macOS faz HTML -> .docx e o Chrome faz
HTML -> PDF, e os dois partem daqui.

    tools/markdown_para_html.py Descricao/HATS-dicionario-de-dados.md > saida.html
"""

import html
import re
import sys
from pathlib import Path

ESTILO = """
@page { size: A4; margin: 18mm 16mm 18mm 16mm; }
html { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
body {
  font-family: -apple-system, "Helvetica Neue", Helvetica, Arial, sans-serif;
  font-size: 10.5pt; line-height: 1.5; color: #1a1a1a;
  max-width: 190mm; margin: 0 auto; padding: 0 2mm;
}
h1 {
  font-size: 22pt; font-weight: 600; letter-spacing: -0.01em;
  margin: 0 0 2mm; padding-bottom: 3mm; border-bottom: 2px solid #1a1a1a;
}
h2 {
  font-size: 14pt; font-weight: 600; margin: 9mm 0 3mm;
  padding-bottom: 1.5mm; border-bottom: 1px solid #c8c8c8;
  page-break-after: avoid;
}
h3 { font-size: 11.5pt; font-weight: 600; margin: 6mm 0 2mm; page-break-after: avoid; }
h4 {
  font-size: 10.5pt; font-weight: 600; margin: 5mm 0 1.5mm;
  color: #333; page-break-after: avoid;
}
h4::before { content: "\\2022\\00a0\\00a0"; color: #999; }
p { margin: 0 0 3mm; text-align: justify; hyphens: auto; }
ol, ul { margin: 0 0 3mm; padding-left: 7mm; }
li { margin-bottom: 1.5mm; text-align: justify; }
table {
  border-collapse: collapse; width: 100%; margin: 0 0 4mm;
  font-size: 8.5pt; page-break-inside: avoid;
}
th, td {
  border: 1px solid #d0d0d0; padding: 1.4mm 2mm;
  text-align: left; vertical-align: top;
}
th { background: #f0f0f0; font-weight: 600; }
tr:nth-child(even) td { background: #fafafa; }
code {
  font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 0.88em;
  background: #f2f2f2; padding: 0.3mm 1mm; border-radius: 2px;
}
pre {
  font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 7.4pt;
  line-height: 1.35; background: #f7f7f7; border: 1px solid #e0e0e0;
  border-left: 3px solid #b0b0b0; padding: 2.5mm 3mm; margin: 0 0 4mm;
  overflow-x: auto; white-space: pre; page-break-inside: avoid;
}
pre code { background: none; padding: 0; font-size: 1em; }
"""


def em_linha(texto: str) -> str:
    """Escapa o texto e aplica `código` e **negrito**, nessa ordem."""
    escapado = html.escape(texto)
    escapado = re.sub(r"`([^`]+)`", r"<code>\1</code>", escapado)
    escapado = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escapado)
    return escapado


def celulas(linha: str) -> list:
    """As células de uma linha de tabela, sem os pipes das pontas."""
    return [parte.strip() for parte in linha.strip().strip("|").split("|")]


def e_separador(linha: str) -> bool:
    return bool(re.fullmatch(r"\|[\s:|-]+\|", linha.strip()))


def converte(markdown: str, titulo: str = None) -> str:
    saida = []
    tabela = []
    paragrafo = []
    lista = []
    codigo = []
    em_codigo = False

    def fecha_paragrafo():
        if paragrafo:
            saida.append("<p>{}</p>".format(em_linha(" ".join(paragrafo))))
            paragrafo.clear()

    def fecha_lista():
        if lista:
            saida.append("<ol>")
            saida.extend("<li>{}</li>".format(em_linha(item)) for item in lista)
            saida.append("</ol>")
            lista.clear()

    def fecha_tabela():
        if not tabela:
            return
        cabecalho, corpo = tabela[0], tabela[1:]
        saida.append("<table>")
        saida.append("<thead><tr>{}</tr></thead>".format(
            "".join("<th>{}</th>".format(em_linha(c)) for c in cabecalho)))
        if corpo:
            saida.append("<tbody>")
            for linha in corpo:
                saida.append("<tr>{}</tr>".format(
                    "".join("<td>{}</td>".format(em_linha(c)) for c in linha)))
            saida.append("</tbody>")
        saida.append("</table>")
        tabela.clear()

    def fecha_tudo():
        fecha_paragrafo()
        fecha_lista()
        fecha_tabela()

    for linha in markdown.splitlines():
        if linha.startswith("```"):
            if em_codigo:
                saida.append("<pre><code>{}</code></pre>".format(
                    html.escape("\n".join(codigo))))
                codigo.clear()
            else:
                fecha_tudo()
            em_codigo = not em_codigo
            continue

        if em_codigo:
            codigo.append(linha)
            continue

        if not linha.strip():
            fecha_tudo()
            continue

        cabecalho = re.match(r"^(#{1,4})\s+(.*)$", linha)
        if cabecalho:
            fecha_tudo()
            nivel = len(cabecalho.group(1))
            saida.append("<h{0}>{1}</h{0}>".format(nivel, em_linha(cabecalho.group(2))))
            continue

        if linha.lstrip().startswith("|"):
            fecha_paragrafo()
            fecha_lista()
            if not e_separador(linha):
                tabela.append(celulas(linha))
            continue

        item = re.match(r"^\d+\.\s+(.*)$", linha)
        if item:
            fecha_paragrafo()
            fecha_tabela()
            lista.append(item.group(1))
            continue

        if lista:
            # continuação de um item, que o gerador quebra em várias linhas
            lista[-1] += " " + linha.strip()
            continue

        fecha_tabela()
        paragrafo.append(linha.strip())

    if em_codigo and codigo:
        saida.append("<pre><code>{}</code></pre>".format(
            html.escape("\n".join(codigo))))
    fecha_tudo()

    primeiro_titulo = re.search(r"^#\s+(.*)$", markdown, re.M)
    return ("<!doctype html>\n<html lang=\"en\">\n<head>\n"
            "<meta charset=\"utf-8\">\n"
            "<title>{}</title>\n<style>{}</style>\n</head>\n<body>\n{}\n"
            "</body>\n</html>\n").format(
                html.escape(titulo or (primeiro_titulo.group(1)
                                       if primeiro_titulo else "documento")),
                ESTILO, "\n".join(saida))


def main(argv=None):
    argumentos = (argv if argv is not None else sys.argv[1:])
    if not argumentos:
        raise SystemExit("uso: markdown_para_html.py arquivo.md [--titulo T] "
                         "[--saida arquivo.html]")

    entrada = Path(argumentos[0])
    titulo = None
    saida = None
    resto = argumentos[1:]
    for posicao, item in enumerate(resto):
        if item == "--titulo" and posicao + 1 < len(resto):
            titulo = resto[posicao + 1]
        if item == "--saida" and posicao + 1 < len(resto):
            saida = Path(resto[posicao + 1])

    documento = converte(entrada.read_text(encoding="utf-8"), titulo)
    if saida:
        saida.parent.mkdir(parents=True, exist_ok=True)
        saida.write_text(documento, encoding="utf-8")
        print("gravado em {} ({} bytes)".format(saida, len(documento.encode("utf-8"))))
    else:
        sys.stdout.write(documento)
    return 0


if __name__ == "__main__":
    sys.exit(main())
