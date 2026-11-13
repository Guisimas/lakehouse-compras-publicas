"""Entrypoint das tasks do Databricks Workflows (``compras-task <etapa> ...``).

Etapas, na ordem do job diário::

    planejar → ingestao → bronze → silver → gold → qualidade → otimizar

Cada etapa é idempotente. ``planejar`` decide as janelas do run e as publica como task
value (``janelas``); as demais recebem ``--janelas {{tasks.planejar.values.janelas}}``.
O ``run_id`` é o id do run do job, compartilhado por todas as tasks (e mantido nos
retries), o que liga Bronze → Silver → Gold do mesmo run.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta

from compras.common.runtime import hoje_brt, novo_run_id, parse_data, planejar_janelas
from compras.config import ENTIDADES, Settings

log = logging.getLogger("compras.tasks")

ORDEM_SILVER = ("orgaos", "catalogo", "licitacoes", "itens_compra", "contratos", "fornecedores")


def _bool(v: str | bool) -> bool:
    return v if isinstance(v, bool) else str(v).strip().lower() in ("1", "true", "sim", "yes", "s")


def _janelas(texto: str | None) -> list[date]:
    if not texto:
        return []
    return sorted({parse_data(t.strip()) for t in texto.split(",") if t.strip()})


def _set_task_value(chave: str, valor: str) -> None:
    try:
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key=chave, value=valor)
    except Exception as exc:  # fora do Databricks
        log.info("taskValue %s=%s (não publicado: %s)", chave, valor, exc)


# ====================================================================== etapas
def etapa_planejar(args: argparse.Namespace, settings: Settings) -> None:
    from compras.bronze.controle import ler_watermark
    from compras.common.spark import obter_spark
    from compras.common.tabelas import criar_estrutura

    spark = obter_spark()
    criar_estrutura(spark, settings)
    ultima = ler_watermark(spark, settings, "pipeline", "gold")
    reprocessar = [int(d) for d in str(args.dias_reprocessar or "").split(",") if d.strip()]
    janelas = planejar_janelas(
        data_inicio=args.data_inicio,
        data_fim=args.data_fim,
        ultima_janela_ok=ultima,
        dias_reprocessar=reprocessar,
    )
    valor = ",".join(j.isoformat() for j in janelas)
    log.info("Janelas do run: %s (watermark pipeline=%s)", valor, ultima)
    _set_task_value("janelas", valor)
    print(f"janelas={valor}")


def etapa_ingestao(args: argparse.Namespace, settings: Settings) -> None:
    from compras.ingestion.endpoints import eh_snapshot, eh_varredura, refresh_dias
    from compras.ingestion.landing import Landing, janela_de_chave
    from compras.ingestion.runner import executar_ingestao, resumo
    from compras.ingestion.storage import LocalStorage

    janelas = _janelas(args.janelas)
    storage = LocalStorage()  # /Volumes/... é acessível como sistema de arquivos no Databricks
    if not _bool(args.ingestao_no_job):
        # Free Edition sem saída para a internet: a ingestão roda fora (CLI) e grava no
        # Volume. Aqui só conferimos o que chegou; falta de dado vira aviso (DQ-16).
        landing = Landing(storage, settings.landing)
        for ent in args.entidades:
            if eh_snapshot(ent):
                continue
            if eh_varredura(ent):
                chaves = landing.chaves(ent)
                ultima = janela_de_chave(chaves[-1]) if chaves else None
                limite = max(janelas) - timedelta(days=refresh_dias(ent)) if janelas else None
                if limite and (ultima is None or ultima < limite):
                    log.warning("Varredura de %s atrasada: a última é de %s", ent, ultima)
                continue
            faltando = [j for j in janelas if j.isoformat() not in landing.chaves(ent)]
            if faltando:
                log.warning(
                    "Landing sem %s para %s janela(s): %s", ent, len(faltando), faltando[:10]
                )
        return
    resultados = executar_ingestao(
        settings=settings,
        storage=storage,
        entidades=args.entidades,
        janelas=janelas,
        hoje=hoje_brt(),
        run_id=args.run_id,
        modo=args.modo,
        forcar=_bool(args.forcar),
    )
    print(resumo(resultados))
    falhas = [r for r in resultados if r.status != "sucesso"]
    if falhas:  # retry da task retoma da última página gravada
        raise RuntimeError(
            f"{len(falhas)} ingestões falharam: "
            + "; ".join(f"{r.entidade}/{r.janela}: {r.erro}" for r in falhas[:5])
        )


def etapa_bronze(args: argparse.Namespace, settings: Settings) -> None:
    from compras.bronze.controle import sincronizar_controle
    from compras.bronze.load import carregar_janela, chaves_carregadas
    from compras.common.spark import obter_spark
    from compras.ingestion.control import Controle
    from compras.ingestion.endpoints import eh_snapshot
    from compras.ingestion.landing import Landing
    from compras.ingestion.storage import LocalStorage
    from compras.quality.results import RegistradorDQ, Resultado

    spark = obter_spark()
    storage = LocalStorage()
    landing = Landing(storage, settings.landing)
    sincronizar_controle(spark, settings, Controle(storage, settings.landing))
    dq = RegistradorDQ(spark, settings, args.run_id)
    pedidas = {j.isoformat() for j in _janelas(args.janelas)}
    for ent in args.entidades:
        no_landing = set(landing.chaves(ent))
        carregadas = chaves_carregadas(spark, settings, ent)
        pendentes = no_landing - carregadas  # inclui CSV, anuais e snapshots vindos da CLI
        if eh_snapshot(ent):
            chaves = sorted(pendentes)[-1:]  # só o snapshot mais recente ainda não carregado
        else:
            chaves = sorted((pedidas & no_landing) | pendentes)
        for chave in chaves:
            res = carregar_janela(spark, settings, storage, ent, chave, args.run_id)
            if res.colunas_novas:
                dq.registrar(
                    Resultado(
                        "SCHEMA-NOVA-COLUNA",
                        ent,
                        chave,
                        "landing->bronze",
                        res.linhas,
                        len(res.colunas_novas),
                        ", ".join(res.colunas_novas),
                    )
                )
    dq.gravar()


def etapa_silver(args: argparse.Namespace, settings: Settings) -> None:
    from compras.common.spark import obter_spark
    from compras.quality.results import FalhaCritica, RegistradorDQ
    from compras.silver.transform import processar_entidade

    spark = obter_spark()
    dq = RegistradorDQ(spark, settings, args.run_id)
    falhas = []
    for ent in [e for e in ORDEM_SILVER if e in args.entidades]:
        try:
            processar_entidade(spark, settings, ent, args.run_id, dq)
        except FalhaCritica as exc:  # crítica "para a entidade": as outras seguem
            log.error("Entidade %s interrompida: %s", ent, exc)
            falhas.append(f"{ent}: {exc}")
    dq.gravar()
    if falhas:
        raise FalhaCritica(" | ".join(falhas))


def etapa_gold(args: argparse.Namespace, settings: Settings) -> None:
    from compras.common.delta import tabela_existe
    from compras.common.spark import obter_spark
    from compras.gold.dim_fornecedor import construir_dim_fornecedor
    from compras.gold.dimensoes import (
        construir_dim_data,
        construir_dim_item,
        construir_dim_modalidade,
        construir_dim_orgao,
    )
    from compras.gold.fato_compras import construir_fato_compras
    from compras.gold.fato_contratos import construir_fato_contratos
    from compras.quality.results import RegistradorDQ

    spark = obter_spark()
    dq = RegistradorDQ(spark, settings, args.run_id)
    if not tabela_existe(spark, settings.gold("dim_data")):
        construir_dim_data(spark, settings)
    construir_dim_modalidade(spark, settings)
    # Sem snapshot de órgãos/catálogo as dimensões nascem só com o membro -1: itens vão
    # para a quarentena (DQ-10) e sk_item = -1, de forma visível em dq_resultados.
    construir_dim_orgao(spark, settings)
    construir_dim_item(spark, settings)
    construir_dim_fornecedor(spark, settings, hoje_brt())
    completo = _bool(args.completo)
    construir_fato_compras(spark, settings, args.run_id, dq, completo=completo)
    construir_fato_contratos(spark, settings, args.run_id, dq, completo=completo)
    dq.gravar()


def etapa_qualidade(args: argparse.Namespace, settings: Settings) -> None:
    from pyspark.sql import functions as F

    from compras.bronze.controle import gravar_watermark
    from compras.common.delta import tabela_existe
    from compras.common.spark import obter_spark
    from compras.gold import fato_compras, fato_contratos
    from compras.quality.operational import checar_atualidade, checar_volume
    from compras.quality.reconciliation import checar_sks
    from compras.quality.results import RegistradorDQ

    spark = obter_spark()
    dq = RegistradorDQ(spark, settings, args.run_id)
    janelas = _janelas(args.janelas)
    resultados = checar_atualidade(spark, settings) + checar_volume(spark, settings, janelas)
    # DQ-13 pós-publicação, na tabela inteira
    for mod in (fato_compras, fato_contratos):
        if tabela_existe(spark, settings.gold(mod.NOME)):
            resultados.append(
                checar_sks(spark.table(settings.gold(mod.NOME)), mod.NOME, mod.SKS, "*")
            )
    for r in resultados:
        dq.registrar(r)
    dq.falhar_se_critico(resultados)
    if janelas:
        gravar_watermark(spark, settings, "pipeline", "gold", max(janelas), args.run_id)
    avisos = (
        spark.table(settings.dq_resultados)
        .where(F.col("run_id") == args.run_id)
        .where(F.col("status").isin("aviso", "quarentena"))
        .count()
    )
    log.info("Run %s concluído com %s resultados de aviso/quarentena", args.run_id, avisos)
    if avisos and _bool(args.falhar_em_aviso):
        raise RuntimeError(f"{avisos} regras de aviso/quarentena reprovadas neste run")


def etapa_otimizar(args: argparse.Namespace, settings: Settings) -> None:
    from compras.common.spark import obter_spark
    from compras.maintenance.optimize import otimizar

    if not _bool(args.otimizar):
        # Backfill antes do benchmark: as tabelas ficam sem OPTIMIZE para a medição "antes".
        log.info("Otimização desligada neste run (--otimizar false)")
        return
    otimizar(
        obter_spark(),
        settings,
        vacuum=_bool(args.vacuum),
        intervalo_dias=int(args.intervalo_otimizacao),
    )


def etapa_benchmark(args: argparse.Namespace, settings: Settings) -> None:
    from compras.common.spark import obter_spark
    from compras.maintenance.benchmark import executar_benchmark

    executar_benchmark(obter_spark(), settings, args.rotulo)


ETAPAS = {
    "planejar": etapa_planejar,
    "ingestao": etapa_ingestao,
    "bronze": etapa_bronze,
    "silver": etapa_silver,
    "gold": etapa_gold,
    "qualidade": etapa_qualidade,
    "otimizar": etapa_otimizar,
    "benchmark": etapa_benchmark,
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="compras-task")
    p.add_argument("etapa", choices=[*ETAPAS, "tudo"])
    p.add_argument("--catalog")
    p.add_argument("--landing-root")
    p.add_argument("--run-id")
    p.add_argument("--janelas", help="Datas separadas por vírgula (saída de 'planejar')")
    p.add_argument("--data-inicio", type=lambda s: parse_data(s) if s else None)
    p.add_argument("--data-fim", type=lambda s: parse_data(s) if s else None)
    p.add_argument("--dias-reprocessar", default="14,30,60,90")
    p.add_argument("--entidades", default=",".join(ENTIDADES))
    p.add_argument("--modo", choices=("incremental", "historico"), default="incremental")
    p.add_argument("--ingestao-no-job", default="true")
    p.add_argument("--forcar", default="false")
    p.add_argument("--completo", default="false", help="Reconstrói todas as partições da Gold")
    p.add_argument("--vacuum", default="true")
    p.add_argument("--otimizar", default="true", help="false pula a task otimizar")
    p.add_argument(
        "--intervalo-otimizacao",
        default="6",
        help="Dias desde o último OPTIMIZE para otimizar de novo (0 = sempre)",
    )
    p.add_argument("--falhar-em-aviso", default="false")
    p.add_argument("--rotulo", default="antes", help="Benchmark: 'antes' ou 'depois'")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if args.catalog:
        os.environ["COMPRAS_CATALOG"] = args.catalog
    if args.landing_root:
        os.environ["COMPRAS_LANDING_ROOT"] = args.landing_root
    settings = Settings()
    # parâmetros vazios vindos do job ("") viram None
    args.run_id = args.run_id or novo_run_id()
    # "+" também separa: no `bundle run --params` a vírgula já separa os parâmetros
    args.entidades = [e.strip() for e in args.entidades.replace("+", ",").split(",") if e.strip()]
    desconhecidas = set(args.entidades) - set(ENTIDADES)
    if desconhecidas:
        p.error(f"Entidades desconhecidas: {sorted(desconhecidas)}")
    log.info("Etapa %s | run_id=%s | catálogo=%s", args.etapa, args.run_id, settings.catalog)

    if args.etapa == "tudo":  # execução local de ponta a ponta
        etapa_planejar(args, settings)
        if not args.janelas:
            args.janelas = ",".join(
                j.isoformat()
                for j in planejar_janelas(
                    data_inicio=args.data_inicio, data_fim=args.data_fim, ultima_janela_ok=None
                )
            )
        for nome in ("ingestao", "bronze", "silver", "gold", "qualidade", "otimizar"):
            ETAPAS[nome](args, settings)
    else:
        ETAPAS[args.etapa](args, settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
