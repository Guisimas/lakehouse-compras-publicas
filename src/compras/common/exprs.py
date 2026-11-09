"""Expressões Spark (sem UDF) para limpeza e validação.

Todas são seguras com ``spark.sql.ansi.enabled=true`` (padrão no serverless): nenhuma
conversão falha em valor inválido, o resultado vira ``null`` e a regra de qualidade
correspondente reprova o registro. As versões em Python puro, usadas como referência nos
testes, ficam em ``compras.common.validacao``.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

_PESOS_1 = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_PESOS_2 = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)

_RE_DECIMAL = r"^-?[0-9]+(\.[0-9]+)?$"
_RE_CIENTIFICO = r"^-?[0-9]+(\.[0-9]+)?[eE][-+]?[0-9]+$"
_RE_DECIMAL_BR = r"^-?[0-9]{1,3}(\.[0-9]{3})*(,[0-9]+)?$|^-?[0-9]+,[0-9]+$"


def texto(c: Column) -> Column:
    """String aparada; vazio vira null."""
    s = F.trim(c.cast("string"))
    return F.when(s == "", None).otherwise(s)


def digitos(c: Column) -> Column:
    return F.regexp_replace(c.cast("string"), r"[^0-9]", "")


def _dv(d: Column, pesos: tuple[int, ...]) -> Column:
    arr_pesos = F.array(*[F.lit(p) for p in pesos])
    soma = F.aggregate(
        F.sequence(F.lit(1), F.lit(len(pesos))),
        F.lit(0),
        lambda acc, i: acc + d.substr(i, F.lit(1)).cast("int") * F.element_at(arr_pesos, i),
    )
    resto = soma % 11
    return F.when(resto < 2, F.lit(0)).otherwise(F.lit(11) - resto)


def cnpj_valido(c: Column) -> Column:
    d = digitos(c)
    ok_formato = (F.length(d) == 14) & ~d.rlike(r"^(\d)\1{13}$")
    return F.when(
        ok_formato,
        (_dv(d, _PESOS_1) == d.substr(13, 1).cast("int"))
        & (_dv(d, _PESOS_2) == d.substr(14, 1).cast("int")),
    ).otherwise(F.lit(False))


def mascarar_cpf(c: Column) -> Column:
    s = texto(c)
    d = digitos(s)
    ja_mascarado = s.contains("*")
    return (
        F.when(
            ja_mascarado & (F.length(d) == 6),
            F.concat(F.lit("***."), d.substr(1, 3), F.lit("."), d.substr(4, 3), F.lit("-**")),
        )
        .when(
            ~ja_mascarado & (F.length(d) == 11),
            F.concat(F.lit("***."), d.substr(4, 3), F.lit("."), d.substr(7, 3), F.lit("-**")),
        )
        .otherwise(F.lit(None).cast("string"))
    )


def tipo_pessoa(c: Column) -> Column:
    s = texto(c)
    d = digitos(s)
    return (
        F.when(s.isNull(), None)
        .when(s.contains("*"), F.lit("PF"))
        .when(F.length(d) == 14, F.lit("PJ"))
        .when(F.length(d) == 11, F.lit("PF"))
        # Nem CNPJ nem CPF: fornecedor estrangeiro (ex.: "US094788608"; o PNCP chama de
        # "pessoa estrangeira") ou identificador de preenchimento ("000000000").
        .otherwise(F.lit("PE"))
    )


def ni(c: Column) -> Column:
    """Identificador do fornecedor: CNPJ só com dígitos ou CPF mascarado."""
    s = texto(c)
    tp = tipo_pessoa(s)
    return (
        F.when(tp == "PJ", digitos(s))
        .when(tp == "PF", mascarar_cpf(s))
        # PE: dígitos mascarados, porque um CPF malformado (sem zeros à esquerda) também
        # cai aqui e não pode vazar.
        .when(s.isNotNull(), F.regexp_replace(s, r"[0-9]", "#"))
    )


def uasg(c: Column) -> Column:
    d = digitos(texto(c))
    return F.when(F.length(d) > 0, F.lpad(d, 6, "0"))


def timestamp(c: Column) -> Column:
    s = texto(c)
    return F.coalesce(
        F.try_to_timestamp(F.substring(s, 1, 19), F.lit("yyyy-MM-dd'T'HH:mm:ss")),
        F.try_to_timestamp(F.substring(s, 1, 19), F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(F.substring(s, 1, 10), F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(F.substring(s, 1, 10), F.lit("dd/MM/yyyy")),
        F.try_to_timestamp(F.substring(s, 1, 8), F.lit("yyyyMMdd")),
    )


def data(c: Column) -> Column:
    return timestamp(c).cast("date")


def numero(c: Column, tipo: str = "decimal(18,2)") -> Column:
    """Converte texto numérico (JSON, científico ou formato BR ``1.234,56``) sem falhar."""
    s = F.regexp_replace(texto(c), r"\s", "")
    br = F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")
    texto_decimal = F.when(s.rlike(_RE_DECIMAL), s).when(s.rlike(_RE_DECIMAL_BR), br)
    # Limite de magnitude (13 dígitos inteiros) evita overflow — que no ANSI derruba o
    # job — em decimal(18,2) e decimal(18,4). Valores acima disso viram null e reprovam.
    digitos_inteiros = F.length(F.regexp_extract(texto_decimal, r"^-?([0-9]*)", 1))
    via_decimal = F.when(
        texto_decimal.isNotNull() & (digitos_inteiros <= 13),
        texto_decimal.cast("decimal(38,10)"),
    )
    d = F.when(s.rlike(_RE_CIENTIFICO), s.cast("double"))
    via_cientifico = F.when(F.abs(d) < F.lit(1e13), d.cast("decimal(38,10)"))
    return F.coalesce(via_decimal, via_cientifico).cast(tipo)


def inteiro(c: Column) -> Column:
    return F.when(
        F.abs(numero(c, "decimal(38,10)")) < 2**31, numero(c, "decimal(38,10)").cast("int")
    )


def booleano(c: Column) -> Column:
    s = F.lower(texto(c))
    return (
        F.when(s.isin("true", "1", "s", "sim", "t", "y", "yes"), F.lit(True))
        .when(s.isin("false", "0", "n", "nao", "não", "f", "no"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def converter(c: Column, tipo: str) -> Column:
    """Aplica a conversão do tipo canônico declarado em ``compras.silver.schemas``."""
    if tipo in ("string", "texto"):
        return texto(c)
    if tipo == "uasg":
        return uasg(c)
    if tipo == "ni":
        return ni(c)
    if tipo == "date":
        return data(c)
    if tipo == "timestamp":
        return timestamp(c)
    if tipo == "int":
        return inteiro(c)
    if tipo == "boolean":
        return booleano(c)
    if tipo.startswith("decimal"):
        return numero(c, tipo)
    raise ValueError(f"Tipo desconhecido: {tipo}")


def tipo_spark(tipo: str) -> str:
    return {
        "texto": "string",
        "uasg": "string",
        "ni": "string",
    }.get(tipo, tipo)


def hash_colunas(*cols: str) -> Column:
    """Hash estável (sha2-256) de colunas, com separador e marcador de null."""
    return F.sha2(
        F.concat_ws("␟", *[F.coalesce(F.col(c).cast("string"), F.lit("␀")) for c in cols]),
        256,
    )


def sk_estavel(*cols: Column) -> Column:
    """Surrogate key estável (bigint) a partir da chave natural; nunca colide com -1."""
    h = F.xxhash64(*cols)
    return F.when(h == -1, F.lit(-2).cast("long")).otherwise(h)
