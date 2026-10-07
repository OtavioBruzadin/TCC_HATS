"""
A grade temporal uniforme do level 1, e a interpolação sobre ela.

O Guigue pediu, textualmente: "como os dados serão oferecidos em 0,1 e 1 segundos
de resolução, e as variáveis têm diferentes resoluções temporais, o melhor é criar
uma sequência temporal uniforme de 0,1 ou 1 segundo e fazer uma interpolação dos
dados sobre essa sequência".

Este módulo é só isso: a grade e as duas interpolações. Fica separado do
`hats_export.py` porque não tem nada a ver com formato de arquivo — é conta — e
porque assim pode ser testado contra funções de resposta conhecida, sem passar por
escrita de FITS. Não importa nada do plugin, e nada do plugin precisa importar
além destas quatro funções.

Duas decisões de representação
------------------------------
**A grade é ancorada em 0 UT**, e não no primeiro registro do arquivo: os pontos
são múltiplos exatos do passo contados desde a meia-noite. Com isso a grade de uma
hora encaixa na da hora seguinte sem emenda e sem registro repetido, e dois
arquivos do mesmo dia têm os mesmos instantes. Ancorar no primeiro registro faria
cada hora começar num deslocamento diferente, porque o primeiro husec de cada
arquivo é arbitrário.

**Não há extrapolação.** A grade só cobre o intervalo onde TODAS as séries de
origem têm nó, porque fora dele o valor interpolado seria invenção. Na prática isso
corta alguns pontos nas pontas: a série demodulada começa no centro da primeira
janela de 128 amostras e o apontamento costuma começar depois do detector.
"""

from __future__ import annotations

from typing import Any, List, Sequence


def verifica_nos(nome: str, nos_x: Sequence[int]) -> None:
    """
    Os nós têm de ser dois ou mais e estritamente crescentes.

    A ingestão garante as duas coisas — é o passo 4 do §5.3 —, mas estes dados
    chegam do banco e uma consulta fora de ordem produziria uma interpolação sem
    significado em silêncio, porque a conta não falha, só erra.
    """
    if len(nos_x) < 2:
        raise ValueError(
            "a interpolacao de {} precisa de ao menos dois nos, e veio {}".format(
                nome, len(nos_x)))
    for posicao in range(1, len(nos_x)):
        if nos_x[posicao] <= nos_x[posicao - 1]:
            raise ValueError(
                "nos de {} fora de ordem na posicao {}: {} vem depois de {}".format(
                    nome, posicao, nos_x[posicao], nos_x[posicao - 1]))


def grade(primeiro: int, ultimo: int, passo: int) -> List[int]:
    """
    Os múltiplos de `passo` desde 0 UT que caem dentro de [primeiro, ultimo].

    Tudo em husec, que é inteiro: a grade não acumula erro de ponto flutuante, e o
    passo de 0,1 s são exatamente 1000 husec.
    """
    if passo <= 0:
        raise ValueError("o passo da grade tem de ser positivo, veio {}".format(passo))
    if ultimo < primeiro:
        raise ValueError(
            "intervalo vazio: o ultimo husec {} vem antes do primeiro {}".format(
                ultimo, primeiro))

    # ceil(primeiro / passo) * passo, em inteiros
    inicio = -(-primeiro // passo) * passo
    if inicio > ultimo:
        return []
    return list(range(inicio, ultimo + 1, passo))


def interpola_linear(nos_x: Sequence[int], nos_y: Sequence[float],
                     alvos: Sequence[int]) -> List[float]:
    """
    Interpolação linear dos nós sobre os alvos, numa passada só.

    Os alvos têm de estar em ordem crescente e dentro do intervalo dos nós — a
    grade é construída assim. A varredura é O(nós + alvos) porque o cursor dos nós
    nunca volta: importa, porque os nós do detector são 3,6 milhões por hora.
    """
    total = len(nos_x)
    if total != len(nos_y):
        raise ValueError("ha {} nos em x e {} em y".format(total, len(nos_y)))
    if total < 2:
        raise ValueError("a interpolacao precisa de ao menos dois nos")

    saida: List[float] = []
    cursor = 0
    for alvo in alvos:
        if alvo < nos_x[0] or alvo > nos_x[-1]:
            raise ValueError(
                "o alvo {} esta fora do intervalo dos nos [{}, {}]: interpolar aqui "
                "seria extrapolar".format(alvo, nos_x[0], nos_x[-1]))
        while cursor < total - 2 and nos_x[cursor + 1] < alvo:
            cursor += 1
        esquerda, direita = nos_x[cursor], nos_x[cursor + 1]
        valor_esquerda, valor_direita = float(nos_y[cursor]), float(nos_y[cursor + 1])
        if direita == esquerda:
            saida.append(valor_esquerda)
        else:
            peso = (alvo - esquerda) / (direita - esquerda)
            saida.append(valor_esquerda + (valor_direita - valor_esquerda) * peso)
    return saida


def interpola_vizinho(nos_x: Sequence[int], nos_y: Sequence[Any],
                      alvos: Sequence[int]) -> List[Any]:
    """
    Vizinho mais próximo, para as variáveis categóricas.

    `object` e `opmode` são identificadores: 11 não é "mais" que 7, e a média de 7
    com 8 não existe. Interpolar linearmente produziria um valor que o software de
    controle nunca escreveu. Empate vai para o nó anterior, por ser determinístico.
    """
    total = len(nos_x)
    if total != len(nos_y):
        raise ValueError("ha {} nos em x e {} em y".format(total, len(nos_y)))
    if total < 2:
        raise ValueError("a interpolacao precisa de ao menos dois nos")

    saida: List[Any] = []
    cursor = 0
    for alvo in alvos:
        if alvo < nos_x[0] or alvo > nos_x[-1]:
            raise ValueError(
                "o alvo {} esta fora do intervalo dos nos [{}, {}]".format(
                    alvo, nos_x[0], nos_x[-1]))
        while cursor < total - 2 and nos_x[cursor + 1] < alvo:
            cursor += 1
        esquerda, direita = nos_x[cursor], nos_x[cursor + 1]
        saida.append(nos_y[cursor] if (alvo - esquerda) <= (direita - alvo)
                     else nos_y[cursor + 1])
    return saida
