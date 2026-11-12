"""Textos das regras de linha (a avaliação em si é coberta pela suíte Spark)."""

import pytest

pytest.importorskip("pyspark")

from compras.silver.rules import regras_da_entidade
from compras.silver.schemas import CONTRATOS


def test_motivo_dq04_formata_so_o_numero():
    motivos = {r.motivo for r in regras_da_entidade(CONTRATOS, 50_000_000_000) if r.id == "DQ-04"}
    assert "valor_global negativo, não numérico ou acima de R$ 50.000.000.000" in motivos
