"""Espelha o controle da ingestão (JSON no landing) nas tabelas Delta de controle."""

from __future__ import annotations

from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from compras.common.runtime import agora_utc
from compras.common.tabelas import CTL_INGESTAO_LOG, CTL_WATERMARK
from compras.config import Settings
from compras.ingestion.control import Controle


def _ts(valor: str | None) -> datetime | None:
    return datetime.fromisoformat(valor) if valor else None


def sincronizar_controle(spark: SparkSession, settings: Settings, controle: Controle) -> int:
    """MERGE dos logs do landing em ``ctl_ingestao_log`` (idempotente). Retorna nº de logs."""
    logs = controle.logs()
    if logs:
        linhas = [
            (
                r.get("log_id") or f"{r['run_id']}|{r['entidade']}|{r['janela']}|{r.get('inicio')}",
                r["run_id"],
                r["entidade"],
                r["janela"],
                r.get("fonte_dados"),
                r.get("modo"),
                _ts(r.get("inicio")),
                _ts(r.get("fim")),
                float(r["duracao_s"]) if r.get("duracao_s") is not None else None,
                int(r.get("endpoints") or 0),
                int(r.get("fanouts") or 0),
                int(r.get("fanouts_pulados") or 0),
                int(r.get("paginas") or 0),
                int(r.get("linhas") or 0),
                r.get("status"),
                r.get("erro"),
            )
            for r in logs
        ]
        novos = spark.createDataFrame(linhas, CTL_INGESTAO_LOG)
        (
            DeltaTable.forName(spark, settings.ctl_ingestao_log)
            .alias("t")
            .merge(
                novos.alias("s"),
                "t.log_id = s.log_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    for wm in controle.watermarks():
        if wm.get("ultima_janela"):
            gravar_watermark(
                spark,
                settings,
                wm["entidade"],
                "landing",
                date.fromisoformat(wm["ultima_janela"]),
                wm["run_id"],
                _ts(wm.get("atualizado_em")),
            )
    return len(logs)


def gravar_watermark(
    spark: SparkSession,
    settings: Settings,
    entidade: str,
    camada: str,
    janela: date,
    run_id: str,
    quando: datetime | None = None,
) -> None:
    """Upsert do watermark; só avança (reprocessar janela antiga não o rebaixa)."""
    novo = spark.createDataFrame(
        [(entidade, camada, janela, "sucesso", quando or agora_utc(), run_id)], CTL_WATERMARK
    )
    (
        DeltaTable.forName(spark, settings.ctl_watermark)
        .alias("t")
        .merge(novo.alias("s"), "t.entidade = s.entidade AND t.camada = s.camada")
        .whenMatchedUpdateAll(condition="s.ultima_janela >= t.ultima_janela")
        .whenNotMatchedInsertAll()
        .execute()
    )


def ler_watermark(
    spark: SparkSession, settings: Settings, entidade: str, camada: str
) -> date | None:
    rows = (
        spark.table(settings.ctl_watermark)
        .where((F.col("entidade") == entidade) & (F.col("camada") == camada))
        .select("ultima_janela")
        .collect()
    )
    return rows[0].ultima_janela if rows else None
