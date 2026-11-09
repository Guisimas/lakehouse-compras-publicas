"""Compactação e limpeza: OPTIMIZE (+ Z-ORDER nas fatos) e VACUUM de 7 dias.

No serverless cada OPTIMIZE/VACUUM consome cota. Cada tabela só é otimizada se o último
OPTIMIZE dela tiver ``intervalo_dias`` ou mais (lido do ``DESCRIBE HISTORY``);
``intervalo_dias=0`` força. O padrão é 6 dias, e não 7, para que o run semanal não pule a
otimização quando começa alguns minutos antes do run da semana anterior.

No mesmo ciclo, a Bronze das entidades com varredura (contratos, ~140 mil linhas por
varredura) guarda só as últimas ``VARREDURAS_RETIDAS``; o VACUUM em seguida libera o espaço.
"""

from __future__ import annotations

import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from compras.common.delta import tabela_existe
from compras.config import ENTIDADES, Settings
from compras.ingestion.endpoints import eh_varredura
from compras.quality.operational import VARREDURAS_MEDIA

log = logging.getLogger(__name__)

ZORDER = {
    "fato_compras": ("sk_orgao", "sk_fornecedor", "sk_item"),
    "fato_contratos": ("sk_orgao", "sk_fornecedor"),
}
RETENCAO_HORAS = 168  # 7 dias
INTERVALO_DIAS = 6
# A atual + as anteriores que o DQ-17 usa na média (ele lê a própria Bronze)
VARREDURAS_RETIDAS = VARREDURAS_MEDIA + 1


def tabelas_para_otimizar(settings: Settings) -> list[tuple[str, tuple[str, ...]]]:
    alvo: list[tuple[str, tuple[str, ...]]] = []
    for e in ENTIDADES:
        alvo.append((settings.bronze(e), ()))
        alvo.append((settings.silver(e), ()))
        alvo.append((settings.quarentena(e), ()))
    for d in ("dim_data", "dim_modalidade", "dim_orgao", "dim_item", "dim_fornecedor"):
        alvo.append((settings.gold(d), ()))
    for f, cols in ZORDER.items():
        alvo.append((settings.gold(f), cols))
    alvo.append((settings.dq_resultados, ()))
    return alvo


def tabelas_staging(settings: Settings) -> list[str]:
    """Staging sobrescrito a cada run: não precisa de OPTIMIZE, mas acumula versões."""
    return [settings.tabela("silver", f"_stg_{e}") for e in ENTIDADES] + [
        settings.gold(f"_stg_{f}") for f in ZORDER
    ]


def otimizado_recentemente(spark: SparkSession, tabela: str, intervalo_dias: int) -> bool:
    if intervalo_dias <= 0:
        return False
    limite = F.expr(f"current_timestamp() - INTERVAL {intervalo_dias} DAYS")
    recentes = (
        spark.sql(f"DESCRIBE HISTORY {tabela}")
        .where(F.col("operation") == "OPTIMIZE")
        .where(F.col("timestamp") >= limite)
        .limit(1)
        .count()
    )
    return recentes > 0


def reter_varreduras(
    spark: SparkSession, tabela: str, retidas: int = VARREDURAS_RETIDAS
) -> list[str]:
    """Apaga da Bronze as varreduras mais antigas que as últimas ``retidas``.

    Só toca linhas com ``tipo_janela = 'varredura'``; a Silver não é afetada, porque lê só as
    linhas do run corrente. O landing continua com tudo, para replay. Devolve as datas apagadas.
    """
    datas = sorted(
        r.data_referencia
        for r in spark.table(tabela)
        .where(F.col("tipo_janela") == "varredura")
        .select("data_referencia")
        .distinct()
        .collect()
    )
    if len(datas) <= retidas:
        return []
    corte = datas[-retidas]
    spark.sql(
        f"DELETE FROM {tabela} "
        f"WHERE tipo_janela = 'varredura' AND data_referencia < DATE'{corte.isoformat()}'"
    )
    apagadas = [d.isoformat() for d in datas[:-retidas]]
    log.info("%s: %s varreduras antigas apagadas (%s)", tabela, len(apagadas), apagadas)
    return apagadas


def otimizar(
    spark: SparkSession,
    settings: Settings,
    vacuum: bool = True,
    intervalo_dias: int = INTERVALO_DIAS,
) -> dict[str, float]:
    tempos: dict[str, float] = {}
    com_varredura = {settings.bronze(e) for e in ENTIDADES if eh_varredura(e)}
    for tabela, zorder in tabelas_para_otimizar(settings):
        if not tabela_existe(spark, tabela):
            continue
        if otimizado_recentemente(spark, tabela, intervalo_dias):
            log.info("%s otimizada há menos de %s dias; pulando", tabela, intervalo_dias)
            continue
        t0 = time.monotonic()
        if tabela in com_varredura:
            reter_varreduras(spark, tabela)
        sql = f"OPTIMIZE {tabela}"
        if zorder:
            sql += f" ZORDER BY ({', '.join(zorder)})"
        spark.sql(sql)
        if vacuum:
            spark.sql(f"VACUUM {tabela} RETAIN {RETENCAO_HORAS} HOURS")
        tempos[tabela] = round(time.monotonic() - t0, 2)
        log.info("Otimizada %s em %.1fs", tabela, tempos[tabela])
    if vacuum and tempos:  # ciclo semanal: limpa também as versões antigas do staging
        for tabela in tabelas_staging(settings):
            if tabela_existe(spark, tabela):
                spark.sql(f"VACUUM {tabela} RETAIN {RETENCAO_HORAS} HOURS")
    return tempos
