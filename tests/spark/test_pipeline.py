"""Bronze → Silver → Gold sobre as fixtures versionadas, em Delta local.

Rodar: ``uv sync --extra spark && uv run pytest -m spark`` (precisa de Java 17).
"""

from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal

import pytest

from fixtures import api
from fixtures.montagem import escrever_landing

pytestmark = pytest.mark.spark
F = pytest.importorskip("pyspark.sql.functions")

D1 = date(2026, 10, 1)
D2 = date(2026, 10, 2)


def rodar(spark, settings, run_id: str, data_carga: date = D1, completo: bool = False):
    """Executa bronze, silver e gold como o job faria (sem ingestão/otimização)."""
    from compras.bronze.controle import sincronizar_controle
    from compras.bronze.load import carregar_janela, chaves_carregadas
    from compras.common.tabelas import criar_estrutura
    from compras.gold.dim_fornecedor import construir_dim_fornecedor
    from compras.gold.dimensoes import (
        construir_dim_data,
        construir_dim_item,
        construir_dim_modalidade,
        construir_dim_orgao,
    )
    from compras.gold.fato_compras import construir_fato_compras
    from compras.gold.fato_contratos import construir_fato_contratos
    from compras.ingestion.control import Controle
    from compras.ingestion.landing import Landing
    from compras.ingestion.storage import LocalStorage
    from compras.quality.results import RegistradorDQ
    from compras.silver.transform import processar_entidade
    from compras.tasks.main import ORDEM_SILVER

    criar_estrutura(spark, settings)
    storage = LocalStorage()
    landing = Landing(storage, settings.landing)
    sincronizar_controle(spark, settings, Controle(storage, settings.landing))
    for ent in ORDEM_SILVER:
        carregadas = chaves_carregadas(spark, settings, ent)
        # recarrega tudo o que está no landing (reprocessamento) para testar idempotência
        for chave in landing.chaves(ent):
            if completo or chave not in carregadas:
                carregar_janela(spark, settings, storage, ent, chave, run_id)
    dq = RegistradorDQ(spark, settings, run_id)
    for ent in ORDEM_SILVER:
        processar_entidade(spark, settings, ent, run_id, dq)
    construir_dim_data(spark, settings, date(2025, 1, 1), date(2027, 12, 31))
    construir_dim_modalidade(spark, settings)
    construir_dim_orgao(spark, settings)
    construir_dim_item(spark, settings)
    construir_dim_fornecedor(spark, settings, data_carga)
    construir_fato_compras(spark, settings, run_id, dq, completo=completo)
    construir_fato_contratos(spark, settings, run_id, dq, completo=completo)
    dq.gravar()
    return dq


def _fotografia(spark, settings):
    """Contagens e somas de todas as camadas (para comparar reexecuções)."""
    foto = {}
    for ent in ("licitacoes", "itens_compra", "contratos", "fornecedores", "orgaos", "catalogo"):
        foto[f"bronze_{ent}"] = spark.table(settings.bronze(ent)).count()
        foto[f"silver_{ent}"] = spark.table(settings.silver(ent)).count()
        foto[f"quarentena_{ent}"] = spark.table(settings.quarentena(ent)).count()
    fc = spark.table(settings.gold("fato_compras"))
    foto["fato_compras"] = fc.count()
    foto["fato_compras_valor"] = fc.agg(F.sum("valor_total_item")).first()[0]
    fct = spark.table(settings.gold("fato_contratos"))
    foto["fato_contratos"] = fct.count()
    foto["fato_contratos_valor"] = fct.agg(F.sum("valor_global")).first()[0]
    foto["dim_fornecedor"] = spark.table(settings.gold("dim_fornecedor")).count()
    return foto


def test_ponta_a_ponta(spark, ambiente):
    escrever_landing(ambiente, api.JANELA_1)
    rodar(spark, ambiente, "run-1")
    s = ambiente

    # ---------------- Bronze: dado como recebido + colunas de controle
    bronze = spark.table(s.bronze("itens_compra"))
    assert bronze.count() == 8  # 7 PNCP (com a duplicata) + 1 Legado
    for col in (
        "data_ingestao",
        "arquivo_origem",
        "fonte_dados",
        "data_referencia",
        "run_id",
        "hash_registro",
        "payload",
        "idCompraItem",
    ):
        assert col in bronze.columns

    # ---------------- Silver + quarentena
    assert spark.table(s.silver("licitacoes")).count() == 3
    q_lic = {
        r.chave_registro: tuple(r.regra_violada)
        for r in spark.table(s.quarentena("licitacoes")).collect()
    }
    assert set(q_lic.values()) == {("DQ-02",), ("DQ-04",)}
    assert q_lic["20000508000993026"] == ("DQ-04",)  # acima do teto de R$ 50 bi

    itens = {r.id_compra_item: r for r in spark.table(s.silver("itens_compra")).collect()}
    assert set(itens) == {
        f"{api.COMPRA_PREGAO}-1",
        f"{api.COMPRA_DISPENSA}-1",
        f"{api.COMPRA_LEGADO}-1",
        f"{api.COMPRA_PREGAO}-4",
        "99999999999999999-1",
    }
    item1 = itens[f"{api.COMPRA_PREGAO}-1"]
    assert item1.valor_total_homologado == Decimal("900.00")  # dedup manteve a versão nova
    assert item1.ni_fornecedor == api.CNPJ_OK and item1.tipo_pessoa == "PJ"
    cpf = itens[f"{api.COMPRA_DISPENSA}-1"]
    assert cpf.ni_fornecedor == "***.456.789-**" and cpf.tipo_pessoa == "PF"
    estr = itens[f"{api.COMPRA_PREGAO}-4"]
    assert estr.tipo_pessoa == "PE" and estr.ni_fornecedor == "US#########"
    legado = itens[f"{api.COMPRA_LEGADO}-1"]
    assert legado.valor_total_estimado == Decimal("2000.00")  # 10 × 200 completado
    assert legado.regime_legal == "8666" and legado.codigo_uasg == "153080"

    q_itens = {
        r.chave_registro: (r.regra_violada, r.camada_origem)
        for r in spark.table(s.quarentena("itens_compra")).collect()
    }
    assert q_itens[f"{api.COMPRA_PREGAO}-2"] == (["DQ-03"], "bronze->silver")
    assert q_itens[f"{api.COMPRA_PREGAO}-3"] == (["DQ-05"], "bronze->silver")
    assert q_itens["99999999999999999-1"] == (["DQ-11"], "silver->gold")

    contratos = spark.table(s.silver("contratos"))
    assert contratos.count() == 1
    assert [r.regra_violada for r in spark.table(s.quarentena("contratos")).collect()] == [
        ["DQ-07"]
    ]

    orgaos = {r.codigo_uasg: r for r in spark.table(s.silver("orgaos")).collect()}
    assert orgaos["153080"].nome_orgao == "MINISTERIO DA EDUCACAO"

    # ---------------- Gold
    fato = spark.table(s.gold("fato_compras"))
    assert fato.count() == 4
    # 900 + 500 + 2000 + 100 (estrangeiro)
    assert fato.agg(F.sum("valor_total_item")).first()[0] == Decimal("3500.00")
    linha = fato.where(F.col("id_compra_item") == f"{api.COMPRA_PREGAO}-1").first()
    for sk in ("sk_orgao", "sk_fornecedor", "sk_item", "sk_modalidade"):
        assert linha[sk] != -1, sk
    assert linha.sk_data_publicacao == 20261001 and linha.ano_mes == 202610
    assert linha.valor_eh_estimado is False
    sem_forn = fato.where(F.col("id_compra_item") == f"{api.COMPRA_LEGADO}-1").first()
    assert sem_forn.sk_fornecedor == -1
    estrangeiro = fato.where(F.col("id_compra_item") == f"{api.COMPRA_PREGAO}-4").first()
    assert estrangeiro.sk_fornecedor == -1 and estrangeiro.valor_total_item == Decimal("100.00")

    mod = spark.table(s.gold("dim_modalidade")).where(F.col("sk_modalidade") == linha.sk_modalidade)
    assert (mod.first().modalidade_chave, mod.first().grupo) == ("14133:5", "Pregão")
    dispensa = (
        spark.table(s.gold("dim_modalidade")).where(F.col("modalidade_chave") == "14133:6").first()
    )
    assert dispensa.grupo == "Dispensa" and dispensa.sem_licitacao is True

    fct = spark.table(s.gold("fato_contratos")).first()
    assert fct.valor_global == Decimal("900.00") and fct.prazo_dias == 364
    assert fct.regime_legal == "14133" and fct.sk_fornecedor != -1

    # ---------------- qualidade registrada
    dq = {(r.regra, r.status) for r in spark.table(s.dq_resultados).collect()}
    assert ("DQ-14", "ok") in dq and ("DQ-15", "ok") in dq and ("DQ-13", "ok") in dq
    assert ("DQ-09", "ok") in dq and ("DQ-03", "quarentena") in dq


def test_codigo_do_catalogo_completado_pela_api_do_pncp(spark, ambiente):
    escrever_landing(ambiente, api.JANELA_1 + api.JANELA_1_CATALOGO_PNCP)
    rodar(spark, ambiente, "run-1")
    s = ambiente

    itens = {r.id_compra_item: r for r in spark.table(s.silver("itens_compra")).collect()}
    assert itens[f"{api.COMPRA_PREGAO}-5"].codigo_item_catalogo == "150000"
    # o código da fonte principal não é trocado pelo complemento
    assert itens[f"{api.COMPRA_PREGAO}-1"].codigo_item_catalogo == "150000"
    # o complemento não vira linha de item
    assert len(itens) == 6

    linha = (
        spark.table(s.gold("fato_compras"))
        .where(F.col("id_compra_item") == f"{api.COMPRA_PREGAO}-5")
        .first()
    )
    assert linha.sk_item != -1


def test_reexecucao_e_idempotente(spark, ambiente):
    escrever_landing(ambiente, api.JANELA_1)
    rodar(spark, ambiente, "run-1")
    antes = _fotografia(spark, ambiente)
    # mesma janela de novo, recarregando a Bronze e reconstruindo toda a Gold
    rodar(spark, ambiente, "run-2", completo=True)
    assert _fotografia(spark, ambiente) == antes
    rodar(spark, ambiente, "run-3")
    assert _fotografia(spark, ambiente) == antes


def test_scd2_fornecedor(spark, ambiente):
    s = ambiente
    escrever_landing(s, api.JANELA_1)
    rodar(spark, s, "run-1", data_carga=D1)
    escrever_landing(s, api.JANELA_2_FORNECEDOR)
    rodar(spark, s, "run-2", data_carga=D2)

    versoes = (
        spark.table(s.gold("dim_fornecedor"))
        .where(F.col("ni_fornecedor") == api.CNPJ_OK)
        .orderBy("data_inicio_vigencia")
        .collect()
    )
    assert [v.porte_nome for v in versoes] == ["ME", "EPP"]
    assert versoes[0].data_fim_vigencia == D1 and not versoes[0].registro_atual
    assert versoes[1].data_inicio_vigencia == D2 and versoes[1].registro_atual
    assert versoes[0].sk_fornecedor != versoes[1].sk_fornecedor

    # fato de 2026-10-01 continua ligada à versão vigente na data do evento (ME)
    fato = spark.table(s.gold("fato_compras")).where(
        F.col("id_compra_item") == f"{api.COMPRA_PREGAO}-1"
    )
    assert fato.first().sk_fornecedor == versoes[0].sk_fornecedor

    # repetir a carga do mesmo dia não cria versão nova
    rodar(spark, s, "run-3", data_carga=D2)
    assert (
        spark.table(s.gold("dim_fornecedor")).where(F.col("ni_fornecedor") == api.CNPJ_OK).count()
        == 2
    )


def test_quarentena_resolvida_quando_registro_chega_corrigido(spark, ambiente):
    s = ambiente
    escrever_landing(s, api.JANELA_1)
    rodar(spark, s, "run-1")
    # a compra que faltava chega: o item órfão deixa a quarentena e entra na fato
    nova = api._pncp_compra("99999999999999999", "153080", 5, "Pregão - Eletrônico", 6.0, None)
    escrever_landing(
        s,
        [
            (
                "licitacoes",
                "2026-10-01",
                "pncp_contratacoes",
                "mod05",
                [*api.PNCP_CONTRATACOES_MOD05, nova],
            )
        ],
    )
    rodar(spark, s, "run-2", completo=True)
    q = (
        spark.table(s.quarentena("itens_compra"))
        .where(F.col("chave_registro") == "99999999999999999-1")
        .first()
    )
    assert q.resolvido is True
    assert spark.table(s.gold("fato_compras")).count() == 5


def test_falha_critica_bloqueia_publicacao(spark, ambiente):
    from compras.quality.reconciliation import checar_reconciliacao

    base = spark.createDataFrame(
        [(202610, Decimal("10.00")), (202610, Decimal("5.00"))], "ano_mes int, v decimal(18,2)"
    )
    rej = base.limit(0)
    fato_duplicada = base.union(base)  # join que multiplicou linhas
    res = {r.regra: r for r in checar_reconciliacao(base, rej, fato_duplicada, "f", "v", "v", 0.01)}
    assert res["DQ-15"].status == "falha" and res["DQ-14"].status == "falha"


def test_tasks_do_job(spark, ambiente, monkeypatch):
    """Executa as etapas pelo entrypoint do job (planejar → … → qualidade)."""
    from compras.tasks import main as tasks

    escrever_landing(ambiente, api.JANELA_1)
    args = argparse.Namespace(
        run_id="job-1",
        janelas="2026-10-01",
        data_inicio=D1,
        data_fim=D1,
        dias_reprocessar="",
        entidades=list(tasks.ORDEM_SILVER),
        modo="incremental",
        ingestao_no_job="false",
        forcar="false",
        completo="false",
        vacuum="false",
        intervalo_otimizacao="7",
        falhar_em_aviso="false",
    )
    monkeypatch.setattr(tasks, "hoje_brt", lambda: D1)
    for etapa in ("planejar", "ingestao", "bronze", "silver", "gold", "qualidade", "otimizar"):
        tasks.ETAPAS[etapa](args, ambiente)
    assert spark.table(ambiente.gold("fato_compras")).count() == 4
    wm = spark.table(ambiente.ctl_watermark).where("entidade = 'pipeline'").first()
    assert wm.ultima_janela == D1
    assert spark.table(ambiente.ctl_ingestao_log).count() >= 1
