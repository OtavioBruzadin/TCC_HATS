"""
ValidationError, com fallback.

Dentro do container do MackSun os contratos importam esta exceção de
`core.contracts.errors`. Fora dele esse pacote não existe, e sem um substituto
nenhum dos contratos poderia ser importado nem testado.

A tentativa de importar o original vem primeiro, então no ambiente de produção é
a classe do core que vale — o que importa, porque o pipeline provavelmente trata
essa exceção por tipo.
"""

try:                                                    # pragma: no cover
    from core.contracts.errors import ValidationError
except ImportError:
    class ValidationError(ValueError):
        """Substituto local, com a mesma semântica de erro de validação."""
