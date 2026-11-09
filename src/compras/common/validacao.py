"""Validações puras em Python (sem Spark).

Servem de referência para as expressões Spark em ``compras.common.exprs`` e são usadas
na ingestão (fan-out de CNPJs). Os testes garantem que as duas implementações concordam.
"""

from __future__ import annotations

import re

_NAO_DIGITO = re.compile(r"\D")
_PESOS_CNPJ_1 = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_PESOS_CNPJ_2 = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)


def somente_digitos(valor: str | None) -> str:
    return _NAO_DIGITO.sub("", valor or "")


def _dv(digitos: str, pesos: tuple[int, ...]) -> int:
    resto = sum(int(d) * p for d, p in zip(digitos, pesos, strict=True)) % 11
    return 0 if resto < 2 else 11 - resto


def cnpj_valido(valor: str | None) -> bool:
    """CNPJ com 14 dígitos e dígitos verificadores corretos (rejeita sequências repetidas)."""
    d = somente_digitos(valor)
    if len(d) != 14 or d == d[0] * 14:
        return False
    return _dv(d[:12], _PESOS_CNPJ_1) == int(d[12]) and _dv(d[:13], _PESOS_CNPJ_2) == int(d[13])


def mascarar_cpf(valor: str | None) -> str | None:
    """Mascara CPF no padrão de publicação do governo: ``***.456.789-**``.

    Se o valor já vier mascarado pela fonte (contém ``*``), só normaliza o formato.
    """
    if valor is None or not valor.strip():
        return None
    if "*" in valor:
        meio = somente_digitos(valor)
        return f"***.{meio[:3]}.{meio[3:6]}-**" if len(meio) == 6 else None
    d = somente_digitos(valor)
    if len(d) != 11:
        return None
    return f"***.{d[3:6]}.{d[6:9]}-**"


def normalizar_ni(valor: str | None) -> tuple[str | None, str | None]:
    """Normaliza o identificador de fornecedor (NI).

    Retorna ``(ni, tipo_pessoa)``: CNPJ só com dígitos e tipo ``PJ``; CPF mascarado e
    tipo ``PF``; ``(None, None)`` se vazio. Qualquer outro valor é fornecedor estrangeiro
    ou identificador de preenchimento: tipo ``PE``, com os dígitos trocados por ``#``.
    """
    if valor is None or not valor.strip():
        return None, None
    if "*" in valor:
        return mascarar_cpf(valor), "PF"
    d = somente_digitos(valor)
    if len(d) == 14:
        return d, "PJ"
    if len(d) == 11:
        return mascarar_cpf(d), "PF"
    return re.sub(r"[0-9]", "#", valor.strip()), "PE"


def normalizar_uasg(valor: str | int | None) -> str | None:
    """Código UASG com 6 dígitos, zeros à esquerda e sem espaços."""
    if valor is None:
        return None
    d = somente_digitos(str(valor))
    return d.zfill(6) if d else None
