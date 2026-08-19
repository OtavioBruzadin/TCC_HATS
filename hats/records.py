"""Iteração sobre os arquivos binários de registros de tamanho fixo."""

import struct

from hats import constants


class Record(object):
    """Registro avulso com os campos como atributos, para inspeção pontual."""

    def __init__(self, values):
        for name, value in values.items():
            setattr(self, name, value)


def total_records(path, schema, limit=None):
    """Quantos registros o arquivo tem, verificando que o tamanho fecha."""
    file_size = path.stat().st_size
    record_size = schema["record_size"]
    if file_size % record_size != 0:
        raise ValueError(
            "{} tem {} bytes, que não é múltiplo do registro de {} bytes. "
            "Provavelmente o XML de formato não corresponde a estes dados.".format(
                path.name, file_size, record_size))
    count = file_size // record_size
    return min(count, limit) if limit is not None else count


def iter_columns(path, schema, limit=None, chunk_records=constants.STDLIB_CHUNK_RECORDS):
    """
    Percorre o arquivo em blocos, entregando cada bloco já transposto em colunas.

    Um laço por registro sobre cinco canais custa cerca de quarenta operações
    interpretadas por registro, o que domina o tempo a 3,6 milhões de registros
    por hora. `zip(*Struct.iter_unpack(...))` transpõe o bloco em C e leva quase
    todo esse trabalho para fora do interpretador.

    Produz tuplas (colunas, quantidade), onde `colunas` está na ordem dos campos
    do esquema.
    """
    record_size = schema["record_size"]
    unpacker = struct.Struct(schema["struct_format"])
    remaining = total_records(path, schema, limit)

    with path.open("rb") as handle:
        while remaining > 0:
            blob = handle.read(record_size * min(chunk_records, remaining))
            if not blob:
                return
            usable = len(blob) - (len(blob) % record_size)
            if usable != len(blob):
                blob = blob[:usable]
            columns = list(zip(*unpacker.iter_unpack(blob)))
            if not columns:
                return
            count = len(columns[0])
            remaining -= count
            yield columns, count


def iter_records(path, schema, limit=None, chunk_records=constants.STDLIB_CHUNK_RECORDS):
    """Percorre registro a registro, entregando a tupla desempacotada."""
    record_size = schema["record_size"]
    unpacker = struct.Struct(schema["struct_format"])
    remaining = total_records(path, schema, limit)

    with path.open("rb") as handle:
        while remaining > 0:
            blob = handle.read(record_size * min(chunk_records, remaining))
            if not blob:
                return
            usable = len(blob) - (len(blob) % record_size)
            for offset in range(0, usable, record_size):
                if remaining <= 0:
                    return
                yield unpacker.unpack_from(blob, offset)
                remaining -= 1


def sample_records(path, schema, sample_count):
    """
    Lê os primeiros e os últimos `sample_count` registros.

    Serve para o relatório mostrar como os dados realmente se parecem nas pontas
    do arquivo, sem precisar percorrê-lo.
    """
    count = total_records(path, schema)
    if count == 0:
        return [], 0

    wanted = sorted(set(list(range(min(sample_count, count)))
                        + list(range(max(0, count - sample_count), count))))
    unpacker = struct.Struct(schema["struct_format"])
    names = []
    for field in schema["fields"]:
        names.extend([field["name"]] * field.get("dim", 1))

    records = []
    with path.open("rb") as handle:
        for index in wanted:
            handle.seek(index * schema["record_size"])
            chunk = handle.read(schema["record_size"])
            if len(chunk) != schema["record_size"]:
                raise ValueError("Registro incompleto em {} no índice {}".format(path.name, index))
            values = unpacker.unpack(chunk)
            records.append((index, Record(dict(zip(names, values)))))
    return records, count


def iter_numpy_blocks(path, schema, limit=None, chunk_records=constants.NUMPY_CHUNK_RECORDS):
    """Percorre o arquivo em blocos como arrays estruturados do numpy."""
    import numpy as np

    from hats import schema as schema_module

    dtype = schema_module.numpy_dtype(schema)
    remaining = total_records(path, schema, limit)

    with path.open("rb") as handle:
        while remaining > 0:
            block = np.fromfile(handle, dtype=dtype, count=min(chunk_records, remaining))
            if block.size == 0:
                return
            remaining -= block.size
            yield block
