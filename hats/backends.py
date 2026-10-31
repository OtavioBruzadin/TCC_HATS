"""
Escolha entre o caminho acelerado por numpy e o da biblioteca padrão.

O pacote funciona sem dependência nenhuma. Quando numpy está instalado, as
mesmas operações rodam vetorizadas.

A série demodulada é idêntica nos dois caminhos: contagens, husec e cada janela
do `deconv` batem exatamente, o que `TestDemodulation.test_backends_agree_bit_for_bit`
exige com igualdade estrita. É essa igualdade que permite trocar de backend sem
abrir mão do bit a bit com a referência do CRAAM.

A estatística por canal é a única saída que difere, em cerca de 1e-9 relativo,
porque os dois caminhos somam em ordens diferentes. Ela é diagnóstico aditivo,
fora do que a referência produz, e o motivo da escolha está em `statistics.py`.

Os números de desempenho ficam no README, para não envelhecerem em dois lugares.
"""

from hats import rbd


def numpy_available():
    try:
        import numpy  # noqa: F401
    except ImportError:
        return False
    return True


def resolve(requested="auto"):
    """
    Devolve (nome, analisador).

    'auto' prefere numpy quando ele existe; 'numpy' falha se não existir;
    'stdlib' força a biblioteca padrão.
    """
    if requested == "stdlib":
        return "stdlib", rbd.analyse_stdlib
    if requested == "numpy":
        if not numpy_available():
            raise SystemExit("Backend 'numpy' pedido, mas numpy não está instalado.")
        return "numpy", rbd.analyse_numpy
    if numpy_available():
        return "numpy", rbd.analyse_numpy
    return "stdlib", rbd.analyse_stdlib


def describe():
    """Texto para o --backends."""
    lines = ["stdlib : sempre disponível",
             "numpy  : {}".format("disponível" if numpy_available() else "não instalado"),
             "auto   -> {}".format(resolve("auto")[0])]
    return "\n".join(lines)
