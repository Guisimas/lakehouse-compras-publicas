"""Janelas de data de cada endpoint (semântica conferida na API real em 2026-11-06)."""

from __future__ import annotations

from datetime import date

from compras.ingestion.endpoints import endpoint

D = date(2026, 11, 4)


def test_pncp_contratacoes_usa_fim_exclusivo():
    fanouts = endpoint("pncp_contratacoes").params(D, None)
    assert len(fanouts) == 14
    for _, p in fanouts:
        assert p["dataPublicacaoPncpInicial"] == D
        assert p["dataPublicacaoPncpFinal"] == date(2026, 11, 5)


def test_pncp_itens_usa_fim_exclusivo():
    [(_, p)] = endpoint("pncp_itens").params(D, None)
    assert p["dataInclusaoPncpInicial"] == D
    assert p["dataInclusaoPncpFinal"] == date(2026, 11, 5)


def test_legado_licitacoes_usa_fim_inclusivo():
    [(_, p)] = endpoint("legado_licitacoes").params(D, None)
    assert p["data_publicacao_inicial"] == p["data_publicacao_final"] == D
