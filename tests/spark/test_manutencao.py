"""Manutenção: retenção das varreduras na Bronze de contratos."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

pytestmark = pytest.mark.spark


def test_reter_varreduras_mantem_as_mais_recentes(spark, ambiente):
    from compras.maintenance.optimize import VARREDURAS_RETIDAS, otimizar

    tabela = ambiente.bronze("contratos")
    varreduras = [date(2026, 9, 1) + timedelta(days=7 * i) for i in range(7)]
    linhas = [("varredura", d, f"c{i}-{j}") for i, d in enumerate(varreduras) for j in range(2)]
    linhas.append(("dia", date(2026, 8, 1), "fora-da-varredura"))
    spark.createDataFrame(
        linhas, "tipo_janela string, data_referencia date, id string"
    ).write.format("delta").saveAsTable(tabela)

    otimizar(spark, ambiente, vacuum=False, intervalo_dias=0)

    restantes = {
        (r.tipo_janela, r.data_referencia)
        for r in spark.table(tabela).select("tipo_janela", "data_referencia").collect()
    }
    esperadas = {("varredura", d) for d in varreduras[-VARREDURAS_RETIDAS:]}
    assert restantes == esperadas | {("dia", date(2026, 8, 1))}
    assert spark.table(tabela).count() == 2 * VARREDURAS_RETIDAS + 1

    # Rodar de novo não apaga mais nada
    otimizar(spark, ambiente, vacuum=False, intervalo_dias=0)
    assert spark.table(tabela).count() == 2 * VARREDURAS_RETIDAS + 1
