"""Reconciliação Silver ↔ Gold (DQ-13, DQ-14, DQ-15), executada sobre o staging da fato
antes da publicação. Reprovação = ``FalhaCritica`` e nenhuma partição é substituída."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from compras.quality.results import Resultado


def checar_sks(fato: DataFrame, nome: str, sks: list[str], janela: str) -> Resultado:
    """DQ-13: nenhuma surrogate key obrigatória nula (ausência de dimensão é -1, nunca null)."""
    cond = None
    for sk in sks:
        c = F.col(sk).isNull()
        cond = c if cond is None else cond | c
    total = fato.count()
    nulas = fato.where(cond).count() if sks else 0
    return Resultado("DQ-13", nome, janela, "gold", total, nulas, f"sks {sks}")


def checar_reconciliacao(
    base: DataFrame,
    rejeitadas: DataFrame,
    fato: DataFrame,
    nome: str,
    valor_base: str,
    valor_fato: str,
    tolerancia: float,
) -> list[Resultado]:
    """DQ-14 (soma por partição) e DQ-15 (contagem Silver = Gold + rejeitadas)."""
    b = base.groupBy("ano_mes").agg(F.count("*").alias("n_base"), F.sum(valor_base).alias("v_base"))
    r = rejeitadas.groupBy("ano_mes").agg(
        F.count("*").alias("n_rej"), F.sum(valor_base).alias("v_rej")
    )
    g = fato.groupBy("ano_mes").agg(F.count("*").alias("n_gold"), F.sum(valor_fato).alias("v_gold"))
    comp = (
        b.join(r, "ano_mes", "full_outer")
        .join(g, "ano_mes", "full_outer")
        .fillna(0, subset=["n_base", "n_rej", "n_gold"])
        .collect()
    )
    resultados = []
    for linha in comp:
        janela = str(linha.ano_mes)
        n_base, n_rej, n_gold = int(linha.n_base), int(linha.n_rej), int(linha.n_gold)
        resultados.append(
            Resultado(
                "DQ-15",
                nome,
                janela,
                "silver->gold",
                n_base,
                abs(n_base - (n_gold + n_rej)),
                f"silver={n_base} gold={n_gold} rejeitadas={n_rej}",
            )
        )
        v_base = Decimal(linha.v_base or 0) - Decimal(linha.v_rej or 0)
        v_gold = Decimal(linha.v_gold or 0)
        dif = abs(v_base - v_gold)
        resultados.append(
            Resultado(
                "DQ-14",
                nome,
                janela,
                "silver->gold",
                n_gold,
                n_gold if dif > Decimal(str(tolerancia)) else 0,
                f"silver={v_base:.2f} gold={v_gold:.2f} diferença={dif:.2f}",
            )
        )
    return resultados
