"""Regras de qualidade linha a linha da passagem Bronze → Silver (DQ-02 a DQ-08).

Cada regra devolve uma coluna booleana "viola?" e um motivo legível. As violações de um
registro viram um único array: uma linha de quarentena por registro, com todas as regras
listadas.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from compras.common import exprs
from compras.silver.schemas import EntidadeSilver

RAW = "_raw_"  # prefixo das colunas com o texto original, antes da conversão


@dataclass(frozen=True)
class RegraLinha:
    id: str
    motivo: str
    viola: Callable[[DataFrame], Column]


def _vazio(c: Column) -> Column:
    return c.isNull()


def _invalido(df: DataFrame, nome: str) -> Column:
    """Tinha texto na origem, mas a conversão de tipo falhou."""
    raw = F.col(RAW + nome)
    return F.trim(raw).isNotNull() & (F.trim(raw) != "") & F.col(nome).isNull()


def regras_da_entidade(ent: EntidadeSilver) -> list[RegraLinha]:
    regras: list[RegraLinha] = []
    tipos = {c.nome: c.tipo for c in ent.colunas}

    for nome in ent.obrigatorias:
        regras.append(RegraLinha("DQ-02", f"{nome} nulo", lambda _df, n=nome: _vazio(F.col(n))))

    if ent.tem_fornecedor:
        regras.append(
            RegraLinha(
                "DQ-03",
                "ni_fornecedor com CNPJ inválido (DV errado ou sequência repetida)",
                # Identificador que não é CNPJ nem CPF vira tipo PE (estrangeiro) e passa:
                # a compra é real e entra na fato com sk_fornecedor = -1.
                lambda _df: (
                    (F.col("tipo_pessoa") == "PJ") & ~exprs.cnpj_valido(F.col("ni_fornecedor"))
                ),
            )
        )

    for nome in ent.valores:
        regras.append(
            RegraLinha(
                "DQ-04",
                f"{nome} negativo ou não numérico",
                lambda df, n=nome: (F.col(n) < 0) | _invalido(df, n),
            )
        )
    for nome, tipo in tipos.items():
        if (
            (tipo.startswith("decimal") or tipo == "int")
            and nome not in ent.valores
            and (nome != "quantidade")
        ):
            regras.append(
                RegraLinha("DQ-04", f"{nome} não numérico", lambda df, n=nome: _invalido(df, n))
            )

    if ent.nome == "itens_compra":
        regras.append(
            RegraLinha(
                "DQ-05",
                "quantidade nula, zero, negativa ou não numérica",
                lambda _df: F.col("quantidade").isNull() | (F.col("quantidade") <= 0),
            )
        )

    for nome, tipo in tipos.items():
        if tipo in ("date", "timestamp"):
            regras.append(
                RegraLinha(
                    "DQ-06", f"{nome} com data inválida", lambda df, n=nome: _invalido(df, n)
                )
            )
    for nome in ent.datas_nao_futuras:
        regras.append(
            RegraLinha(
                "DQ-06",
                f"{nome} no futuro",
                lambda _df, n=nome: F.col(n).cast("date") > F.current_date(),
            )
        )

    if ent.nome == "contratos":
        regras.append(
            RegraLinha(
                "DQ-07",
                "data_vigencia_fim anterior a data_vigencia_inicio",
                lambda _df: F.col("data_vigencia_fim") < F.col("data_vigencia_inicio"),
            )
        )
        regras.append(
            RegraLinha(
                "DQ-08",
                "contrato sem órgão nem unidade gestora",
                lambda _df: F.col("codigo_orgao").isNull() & F.col("codigo_uasg").isNull(),
            )
        )
    return regras


def aplicar_regras(df: DataFrame, regras: list[RegraLinha]) -> DataFrame:
    """Adiciona ``_violacoes: array<struct<regra, motivo>>`` (vazio = registro válido)."""
    if not regras:
        return df.withColumn(
            "_violacoes", F.array().cast("array<struct<regra:string,motivo:string>>")
        )
    itens = [
        F.when(
            F.coalesce(r.viola(df), F.lit(False)),
            F.struct(F.lit(r.id).alias("regra"), F.lit(r.motivo).alias("motivo")),
        )
        for r in regras
    ]
    return df.withColumn("_violacoes", F.filter(F.array(*itens), lambda x: x.isNotNull()))
