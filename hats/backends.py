"""
Escolha entre o caminho acelerado por numpy e o da biblioteca padrão.

O pacote funciona sem dependência nenhuma. Quando numpy está instalado, as
mesmas operações rodam vetorizadas — os resultados são equivalentes, verificados
sobre uma hora de dados: contagens e husec idênticos, amplitudes com erro
relativo de 9e-16, estatísticas de 6,6e-09, que é ordem de soma.

Medido numa hora de dados (3,6 milhões de registros):

    HATS.py do CRAAM (numpy + binário C)    1,08 s     828 MB
    backend stdlib                          4,6  s      34 MB
    backend numpy                           0,14 s     128 MB
"""

from hats import exporters, rbd


def numpy_available():
    try:
        import numpy  # noqa: F401
    except ImportError:
        return False
    return True


def resolve(requested="auto"):
    """
    Devolve (nome, analisador, exportador do rbd).

    'auto' prefere numpy quando ele existe; 'numpy' falha se não existir;
    'stdlib' força a biblioteca padrão.
    """
    if requested == "stdlib":
        return "stdlib", rbd.analyse_stdlib, exporters.export_rbd_csv
    if requested == "numpy":
        if not numpy_available():
            raise SystemExit("Backend 'numpy' pedido, mas numpy não está instalado.")
        return "numpy", rbd.analyse_numpy, exporters.export_rbd_csv_numpy
    if numpy_available():
        return "numpy", rbd.analyse_numpy, exporters.export_rbd_csv_numpy
    return "stdlib", rbd.analyse_stdlib, exporters.export_rbd_csv


def describe():
    """Texto para o --backends."""
    lines = ["stdlib : sempre disponível",
             "numpy  : {}".format("disponível" if numpy_available() else "não instalado"),
             "auto   -> {}".format(resolve("auto")[0])]
    return "\n".join(lines)
