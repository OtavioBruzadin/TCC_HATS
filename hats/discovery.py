"""Criação da estrutura de pastas e descoberta dos arquivos de dados no disco."""

from collections import defaultdict

from hats import timebase

DATA_README = """Layout esperado:

Data/
  2026-03-17/
    hats-2026-03-17T1800.rbd
    aux/
      hats-2026-03-17T1800.aux
      hats-2026-03-17.ws
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


def ensure_structure(project_root, data_dir="Data", reports_dir="Reports", xml_dir="XMLTables"):
    """Cria as pastas do projeto se não existirem e devolve os caminhos."""
    paths = {
        "project_root": project_root,
        "data_dir": project_root / data_dir,
        "reports_dir": project_root / reports_dir,
        "json_dir": project_root / reports_dir / "json",
        "csv_dir": project_root / reports_dir / "csv",
        "xml_dir": project_root / xml_dir,
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
    Mapeia a pasta de dados: um dia por diretório, os arquivos agrupados por hora.

    Os .aux e .ws ficam num subdiretório `aux/`, seguindo a convenção do CRAAM.
    O .ws é diário, então entra sob a chave 'daily'.
    """
    index = {}
    for day_dir in sorted(data_dir.iterdir()):
        if not day_dir.is_dir():
            continue

        aux_dir = day_dir / "aux"
        hours = defaultdict(dict)
        for path in sorted(day_dir.glob("*.rbd")):
            hours[timebase.hour_from_filename(path) or "unknown"]["rbd"] = path
        if aux_dir.exists():
            for path in sorted(aux_dir.glob("*.aux")):
                hours[timebase.hour_from_filename(path) or "unknown"]["aux"] = path
            for path in sorted(aux_dir.glob("*.ws")):
                hours["daily"]["ws"] = path

        index[day_dir.name] = {
            "day_dir": day_dir,
            "aux_dir": aux_dir,
            "hours": dict(sorted(hours.items(), key=lambda item: item[0])),
        }
    return index
