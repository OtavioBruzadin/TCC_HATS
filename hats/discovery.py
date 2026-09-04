"""Criação da estrutura de pastas e descoberta dos arquivos de dados no disco."""

from collections import defaultdict
from pathlib import Path

from hats import timebase

DATA_README = """Layout esperado, o mesmo que o manual do CRAAM descreve:

data/
  hats-2026-03-17T1800.rbd
  hats-2026-03-17T1900.rbd
  aux/
    hats-2026-03-17T1800.aux
    hats-2026-03-17T1900.aux
    hats-2026-03-17.ws

Os .rbd ficam todos juntos aqui; o dia vem do nome do arquivo, não de pasta.
Aponte HATS_DATA_InputPath ou --data-dir para este diretório.
"""

XML_README = """Arquivos de formato do CRAAM:

XMLTables/
  HATSDataFormat.xml      (ou um symlink para a versão em uso)
  HATSDataFormat-120.xml  (>= 2021-10-25, registros de 38 bytes)
  HATSDataFormat-110.xml  (<  2021-10-25, registros de 26 bytes)
  HATSAuxFormat.xml

Sem eles, o pacote usa o esquema fixo interno de hats/schema.py.

O HATSAuxFormat.xml declara unidades erradas para right_ascension (horas, não
graus) e para ra_rate/dec_rate (arcsec/s, não graus/s). O leitor corrige; veja
AUX_UNIT_FIXES em hats/schema.py.
"""


def ensure_structure(project_root, data_dir="Data", output_dir="Saida",
                     xml_dir="XMLTables", diagnostics_dir="Diagnostico"):
    """`data_dir` e `xml_dir` podem vir como Path já resolvido ou como nome relativo."""
    """
    Cria as pastas do projeto se não existirem e devolve os caminhos.

    A pasta de saída guarda só as tabelas da referência, para que a comparação
    com ela seja um `diff -r` limpo entre dois diretórios. Os diagnósticos vão
    para outra pasta justamente por isso.
    """
    def under(value):
        path = Path(value)
        return path if path.is_absolute() else (project_root / path)

    paths = {
        "project_root": project_root,
        "data_dir": under(data_dir),
        "output_dir": under(output_dir),
        "diagnostics_dir": under(diagnostics_dir),
        "xml_dir": under(xml_dir),
    }
    for key, path in paths.items():
        if key != "project_root":
            path.mkdir(parents=True, exist_ok=True)

    readme = paths["data_dir"] / "README.txt"
    if not readme.exists():
        readme.write_text(DATA_README, encoding="utf-8")
    xml_readme = paths["xml_dir"] / "README.txt"
    if not xml_readme.exists():
        xml_readme.write_text(XML_README, encoding="utf-8")
    return paths


def build_day_index(data_dir):
    """
    Mapeia o diretório de dados, agrupando por dia e por hora.

    A estrutura é a que o manual do CRAAM descreve, e é a que o instrumento
    entrega:

        root
          |__data          <- os .rbd, todos juntos
          |    |__aux      <- os .aux e os .ws
          |__log

    O dia vem do nome do arquivo, não de pasta nenhuma: a convenção é
    `hats-YYYY-MM-DDTHH00.rbd`, com os minutos sempre em 00, e um par
    RBD/AUX novo a cada hora. Não há nível por dia — ele seria redundante.

    `data_dir` é o `data/` do diagrama, ou seja o mesmo caminho que a variável
    HATS_DATA_InputPath recebe.
    """
    aux_dir = data_dir / "aux"
    index = defaultdict(lambda: {"day_dir": data_dir, "aux_dir": aux_dir,
                                 "hours": defaultdict(dict)})

    for path in sorted(data_dir.glob("*.rbd")):
        day = timebase.date_from_filename(path)
        if day:
            index[day]["hours"][timebase.hour_from_filename(path) or "unknown"]["rbd"] = path

    if aux_dir.exists():
        for path in sorted(aux_dir.glob("*.aux")):
            day = timebase.date_from_filename(path)
            if day:
                index[day]["hours"][timebase.hour_from_filename(path) or "unknown"]["aux"] = path
        for path in sorted(aux_dir.glob("*.ws")):
            day = timebase.date_from_filename(path)
            if day:
                index[day]["hours"]["daily"]["ws"] = path

    return {day: {"day_dir": value["day_dir"],
                  "aux_dir": value["aux_dir"],
                  "hours": dict(sorted(value["hours"].items()))}
            for day, value in sorted(index.items())}
