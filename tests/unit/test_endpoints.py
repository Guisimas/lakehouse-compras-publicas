"""Janelas de data de cada endpoint (semântica conferida na API real em 2026-11-06)."""

from __future__ import annotations

from datetime import date

from compras.ingestion.endpoints import PNCP_API, endpoint, fanout_compra_pncp

D = date(2026, 11, 4)


def test_pncp_contratacoes_usa_fim_exclusivo():
    fanouts = endpoint("pncp_contratacoes").params(D, None)
    assert len(fanouts) == 4
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


def test_fanout_compra_pncp_a_partir_do_id_da_contratacao():
    fanout, params = fanout_compra_pncp("10637926000146-1-000290/2026")
    assert fanout == "10637926000146-2026-290"
    assert params == {"cnpj": "10637926000146", "ano": "2026", "sequencial": 290}
    assert fanout_compra_pncp("lixo") is None


class _Ctx:
    def compras_pncp_sem_catalogo(self, _janela):
        return ["10637926000146-1-000290/2026", "invalido"]


def test_pncp_itens_catalogo_monta_url_do_pncp():
    spec = endpoint("pncp_itens_catalogo")
    [(fanout, params)] = spec.params(D, _Ctx())
    path, query = spec.requisicao(params)
    assert fanout == "10637926000146-2026-290"
    assert path == f"{PNCP_API}/v1/orgaos/10637926000146/compras/2026/290/itens"
    assert query == {}
    assert spec.opcional and spec.vazio_em_404


def test_requisicao_sem_campos_no_caminho_mantem_a_query():
    spec = endpoint("pncp_itens")
    [(_, params)] = spec.params(D, None)
    assert spec.requisicao(params) == (spec.path, params)
