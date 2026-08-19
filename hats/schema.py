"""
Formato dos registros binários, lido dos XML de descrição do CRAAM.

O layout dos arquivos não está no código: vem de `HATSDataFormat.xml` (sinal do
detector) e `HATSAuxFormat.xml` (apontamento), que trazem nome, tipo, unidade,
origem no hardware e os coeficientes de calibração de cada campo.

Correções de unidade
--------------------
O `HATSAuxFormat.xml` declara unidades erradas para três campos. Verificado
contra efemérides solares em 2026-03-17, 18 e 19: a ascensão reta multiplicada
por 15 bate com a do Sol em 0,01 grau, e as taxas batem com o movimento aparente
dele em 1%. O `HATS.py` do CRAAM e a wiki repetem a mesma descrição errada.

Os campos originais são preservados como estão no arquivo; as versões corrigidas
entram como colunas adicionais.
"""

import struct
import xml.etree.ElementTree as ElementTree

TYPE_MAP = {
    "xs:int": ("i", 4),
    "xs:unsignedInt": ("I", 4),
    "xs:unsignedShort": ("H", 2),
    "xs:unsignedLong": ("Q", 8),
    "xs:double": ("d", 8),
}

NUMPY_FORMAT_MAP = {
    "i": "<i4",
    "I": "<u4",
    "H": "<u2",
    "Q": "<u8",
    "d": "<f8",
}

# campo -> (unidade declarada no XML, unidade real, fator para a declarada, nome da coluna corrigida)
AUX_UNIT_FIXES = {
    "right_ascension": ("degrees", "hours", 15.0, "right_ascension_deg"),
    "ra_rate": ("degrees/s", "arcsec/s", 1.0 / 3600.0, "ra_rate_deg_s"),
    "dec_rate": ("degrees/s", "arcsec/s", 1.0 / 3600.0, "dec_rate_deg_s"),
}

FALLBACK_RBD_SCHEMA = {
    "name": "fallback_rbd",
    "record_size": 38,
    "struct_format": "<IIHQiiiii",
    "fields": [
        {"name": "sample", "type": "xs:unsignedInt", "unit": "none", "size": 4, "fmt": "I", "dim": 1, "origin": "sam4e", "convert": "no", "offset": 0.0, "slope": 1.0, "converted_unit": "none"},
        {"name": "sec", "type": "xs:unsignedInt", "unit": "unix_time", "size": 4, "fmt": "I", "dim": 1, "origin": "sam4e", "convert": "no", "offset": 0.0, "slope": 1.0, "converted_unit": "unix_time"},
        {"name": "ms", "type": "xs:unsignedShort", "unit": "ms_of_second", "size": 2, "fmt": "H", "dim": 1, "origin": "sam4e", "convert": "no", "offset": 0.0, "slope": 1.0, "converted_unit": "ms_of_second"},
        {"name": "husec", "type": "xs:unsignedLong", "unit": "husec_0UTC", "size": 8, "fmt": "Q", "dim": 1, "origin": "sam4e", "convert": "no", "offset": 0.0, "slope": 1.0, "converted_unit": "husec_0UTC"},
        {"name": "golay", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": 0.0, "slope": 0.001154, "converted_unit": "mV"},
        {"name": "chopper", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": -2.106, "slope": 0.00233, "converted_unit": "mV"},
        {"name": "temp_hics", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": 0.013, "slope": 0.0000299, "converted_unit": "C"},
        {"name": "temp_env", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": 0.013, "slope": 0.0000299, "converted_unit": "C"},
        {"name": "temp_golay", "type": "xs:int", "unit": "ADCu", "size": 4, "fmt": "i", "dim": 1, "origin": "ad7770", "convert": "yes", "offset": 0.013, "slope": 0.0000299, "converted_unit": "C"},
    ],
}

FALLBACK_AUX_SCHEMA = {
    "name": "fallback_aux",
    "record_size": 80,
    "struct_format": "<Qddddddddii",
    "fields": [
        {"name": "husec", "type": "xs:unsignedLong", "unit": "husec_0UTC", "size": 8, "fmt": "Q", "dim": 1},
        {"name": "jd", "type": "xs:double", "unit": "day", "size": 8, "fmt": "d", "dim": 1},
        {"name": "sid", "type": "xs:double", "unit": "hour", "size": 8, "fmt": "d", "dim": 1},
        {"name": "elevation", "type": "xs:double", "unit": "degrees", "size": 8, "fmt": "d", "dim": 1},
        {"name": "azimuth", "type": "xs:double", "unit": "degrees", "size": 8, "fmt": "d", "dim": 1},
        {"name": "right_ascension", "type": "xs:double", "unit": "hours", "size": 8, "fmt": "d", "dim": 1},
        {"name": "declination", "type": "xs:double", "unit": "degrees", "size": 8, "fmt": "d", "dim": 1},
        {"name": "ra_rate", "type": "xs:double", "unit": "arcsec/s", "size": 8, "fmt": "d", "dim": 1},
        {"name": "dec_rate", "type": "xs:double", "unit": "arcsec/s", "size": 8, "fmt": "d", "dim": 1},
        {"name": "object", "type": "xs:int", "unit": "id", "size": 4, "fmt": "i", "dim": 1},
        {"name": "opmode", "type": "xs:int", "unit": "id", "size": 4, "fmt": "i", "dim": 1},
    ],
}


def annotate_aux_units(fields):
    """Marca nos campos do AUX quais unidades do XML estão erradas e como corrigir."""
    for field in fields:
        fix = AUX_UNIT_FIXES.get(field["name"])
        if not fix:
            continue
        declared, actual, factor, corrected_name = fix
        field["declared_unit"] = field.get("unit")
        field["unit"] = actual
        field["corrected_field"] = corrected_name
        field["corrected_unit"] = declared
        field["correction_factor"] = factor
    return fields


def _finalise(fields, source_name, source_path):
    struct_format = "<" + "".join(field["fmt"] * field["dim"] for field in fields)
    return {
        "name": source_name,
        "source": str(source_path),
        "mode": "xml",
        "fields": fields,
        "struct_format": struct_format,
        "record_size": struct.calcsize(struct_format),
    }


def parse_from_xml_tree(xml_path, kind):
    """Leitura normal: cada filho da raiz descreve um campo."""
    root = ElementTree.parse(str(xml_path)).getroot()
    fields = []

    for item in list(root):
        texts = [(child.text or "").strip() for child in list(item)]
        if len(texts) < 4:
            continue
        xml_type = texts[2]
        if xml_type not in TYPE_MAP:
            continue

        fmt, size = TYPE_MAP[xml_type]
        field = {"name": texts[0], "dim": int(texts[1]), "type": xml_type,
                 "unit": texts[3], "fmt": fmt, "size": size}

        if kind == "rbd":
            field["origin"] = texts[4] if len(texts) > 4 else "sam4e"
            field["convert"] = texts[5] if len(texts) > 5 else "no"
            field["offset"] = float(texts[6]) if len(texts) > 6 and texts[6] else 0.0
            field["slope"] = float(texts[7]) if len(texts) > 7 and texts[7] else 1.0
            field["converted_unit"] = texts[8] if len(texts) > 8 else field["unit"]

        fields.append(field)

    if not fields:
        raise ValueError("Nenhum campo encontrado em {}".format(xml_path))
    if kind == "aux":
        fields = annotate_aux_units(fields)
    return _finalise(fields, xml_path.name, xml_path)


def _tokenize(text):
    clean = text.replace("﻿", " ").replace("<", " <").replace(">", "> ")
    tokens = []
    current = ""
    inside_tag = False
    for character in clean:
        if character == "<":
            inside_tag = True
            if current.strip():
                tokens.extend(current.strip().split())
            current = ""
        elif character == ">":
            inside_tag = False
            current = ""
        elif not inside_tag:
            current += character
    if current.strip():
        tokens.extend(current.strip().split())
    return [token for token in tokens if token.strip()]


def parse_from_flat_tokens(xml_path, kind):
    """
    Plano B para XML malformado que o parser de árvore recusa.

    Ignora a estrutura e lê os valores em sequência, na ordem em que aparecem.
    """
    tokens = _tokenize(xml_path.read_text(encoding="utf-8", errors="ignore"))
    fields = []
    index = 0

    while index < len(tokens):
        if index + 3 >= len(tokens):
            break
        name = tokens[index]
        try:
            dimension = int(tokens[index + 1])
        except ValueError:
            index += 1
            continue
        xml_type = tokens[index + 2]
        if xml_type not in TYPE_MAP:
            index += 1
            continue

        fmt, size = TYPE_MAP[xml_type]
        field = {"name": name, "dim": dimension, "type": xml_type,
                 "unit": tokens[index + 3], "fmt": fmt, "size": size}

        if kind == "rbd":
            if index + 8 >= len(tokens):
                break
            field["origin"] = tokens[index + 4]
            field["convert"] = tokens[index + 5]
            field["offset"] = float(tokens[index + 6])
            field["slope"] = float(tokens[index + 7])
            field["converted_unit"] = tokens[index + 8]
            index += 9
        else:
            index += 4
        fields.append(field)

    if not fields:
        raise ValueError("Nenhum campo encontrado em {}".format(xml_path))
    if kind == "aux":
        fields = annotate_aux_units(fields)
    return _finalise(fields, xml_path.name, xml_path)


def resolve_pointer(xml_path):
    """Segue um arquivo que contenha apenas o nome de outro XML."""
    if not xml_path.exists():
        return xml_path
    text = xml_path.read_text(encoding="utf-8", errors="ignore").strip().replace("﻿", "").strip()
    if text.endswith(".xml") and len(text.split()) == 1:
        candidate = xml_path.parent / text
        if candidate.exists():
            return candidate
    return xml_path


def load(xml_dir, kind):
    """
    Carrega o esquema de `kind` ('rbd' ou 'aux'), com fallback interno.

    O XML é escolhido pelo nome. Os atributos InitialDate/FinalDate existem nos
    arquivos do CRAAM mas não são usados para selecionar a versão: dados antigos
    (formato 110, registros de 26 bytes) falham alto na verificação de tamanho em
    records.py, em vez de serem lidos errado em silêncio.
    """
    if kind == "rbd":
        candidates = [xml_dir / "HATSDataFormat.xml",
                      xml_dir / "HATSDataFormat-120.xml",
                      xml_dir / "HATSDataFormat-110.xml"]
        fallback = dict(FALLBACK_RBD_SCHEMA)
    else:
        candidates = [xml_dir / "HATSAuxFormat.xml"]
        fallback = dict(FALLBACK_AUX_SCHEMA)

    for candidate in candidates:
        candidate = resolve_pointer(candidate)
        if not candidate.exists():
            continue
        try:
            return parse_from_xml_tree(candidate, kind)
        except Exception:
            try:
                return parse_from_flat_tokens(candidate, kind)
            except Exception:
                pass

    fallback["mode"] = "fallback_fixed_without_xml"
    fallback["source"] = "esquema_fixo_interno"
    fallback["fields"] = [dict(field) for field in fallback["fields"]]
    if kind == "aux":
        fallback["fields"] = annotate_aux_units(fallback["fields"])
    return fallback


def numpy_dtype(schema):
    """
    dtype estruturado equivalente ao struct_format do esquema.

    numpy monta dtypes de lista sem alinhamento, igual ao struct com prefixo '<'.
    A conferência de itemsize é explícita porque um desencontro aqui deslocaria
    todos os campos em silêncio.
    """
    import numpy as np

    fields = []
    for field in schema["fields"]:
        numpy_format = NUMPY_FORMAT_MAP.get(field["fmt"])
        if numpy_format is None:
            raise ValueError("Sem mapeamento numpy para o formato struct '{}'".format(field["fmt"]))
        dimension = field.get("dim", 1)
        if dimension == 1:
            fields.append((field["name"], numpy_format))
        else:
            fields.append((field["name"], numpy_format, dimension))

    dtype = np.dtype(fields)
    if dtype.itemsize != schema["record_size"]:
        raise ValueError("dtype numpy tem itemsize {} mas o esquema diz {}".format(
            dtype.itemsize, schema["record_size"]))
    return dtype


def field_index(schema):
    """Nome do campo -> posição na tupla desempacotada."""
    names = []
    for field in schema["fields"]:
        names.extend([field["name"]] * field.get("dim", 1))
    return {name: position for position, name in enumerate(names)}


def converted_fields(schema):
    """Campos que passam por decodificação do ADC, calibração, ou ambas."""
    return [f for f in schema["fields"]
            if f.get("convert") == "yes" or f.get("origin") == "ad7770"]
