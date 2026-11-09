"""Regras operacionais: DQ-16 (atualidade) e DQ-17 (volume)."""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from compras.common.delta import tabela_existe
from compras.config import ENTIDADES_FATOS, Settings
from compras.ingestion.endpoints import eh_varredura, refresh_dias
from compras.quality.results import Resultado

VARREDURAS_MEDIA = 4  # DQ-17 de entidades com varredura: média das 4 anteriores


def checar_atualidade(spark: SparkSession, settings: Settings) -> list[Resultado]:
    """DQ-16: última ingestão com sucesso de cada entidade de fatos há menos de N horas.

    Entidades com varredura semanal toleram o intervalo entre varreduras a mais.
    """
    if not tabela_existe(spark, settings.ctl_ingestao_log):
        return []
    # Idade calculada no próprio Spark: evita misturar timestamp sem fuso (collect) com UTC.
    idade_horas = {
        r.entidade: r.horas
        for r in spark.table(settings.ctl_ingestao_log)
        .where(F.col("status") == "sucesso")
        .groupBy("entidade")
        .agg(
            (
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("fim"))) / 3600
            ).alias("horas")
        )
        .collect()
    }
    resultados = []
    for ent in ENTIDADES_FATOS:
        horas = idade_horas.get(ent)
        limite = settings.freshness_horas
        if eh_varredura(ent):
            limite += refresh_dias(ent) * 24
        atrasado = horas is None or horas > limite
        resultados.append(
            Resultado(
                "DQ-16",
                ent,
                "*",
                "ingestao",
                1,
                int(atrasado),
                f"última carga ok há {horas:.1f} h" if horas is not None else "nunca carregada",
            )
        )
    return resultados


def checar_volume(spark: SparkSession, settings: Settings, janelas: list[date]) -> list[Resultado]:
    """DQ-17: linhas da janela dentro de ±30% da média das 28 janelas anteriores (Bronze)."""
    resultados = []
    for ent in ENTIDADES_FATOS:
        tabela = settings.bronze(ent)
        if not tabela_existe(spark, tabela) or not janelas:
            continue
        if eh_varredura(ent):
            resultados.extend(_volume_varredura(spark, settings, ent, janelas))
            continue
        inicio = min(janelas) - timedelta(days=settings.volume_dias_media)
        contagens = {
            r.data_referencia: r["count"]
            for r in spark.table(tabela)
            .where((F.col("tipo_janela") == "dia") & (F.col("data_referencia") >= F.lit(inicio)))
            .groupBy("data_referencia")
            .count()
            .collect()
        }
        for j in janelas:
            historico = [
                contagens.get(j - timedelta(days=d), 0)
                for d in range(1, settings.volume_dias_media + 1)
            ]
            com_dados = [h for h in historico if h > 0]
            atual = contagens.get(j, 0)
            if len(com_dados) < 7:
                resultados.append(
                    Resultado(
                        "DQ-17",
                        ent,
                        j.isoformat(),
                        "bronze",
                        atual,
                        0,
                        "histórico insuficiente (< 7 janelas)",
                    )
                )
                continue
            media = sum(historico) / len(historico)
            fora = media > 0 and abs(atual - media) / media > settings.volume_tolerancia
            resultados.append(
                Resultado(
                    "DQ-17",
                    ent,
                    j.isoformat(),
                    "bronze",
                    atual,
                    atual if fora else 0,
                    f"linhas={atual} média28d={media:.0f} desvio={(atual - media) / media:+.0%}"
                    if media
                    else "média zero",
                )
            )
    return resultados


def _volume_varredura(
    spark: SparkSession, settings: Settings, ent: str, janelas: list[date]
) -> list[Resultado]:
    """DQ-17 para varreduras: cada uma comparada à média das anteriores.

    Só avalia as varreduras cuja data de referência está entre as janelas do run (nos dias
    sem varredura não há o que medir).
    """
    contagens = {
        r.data_referencia: r["count"]
        for r in spark.table(settings.bronze(ent))
        .where(F.col("tipo_janela") == "varredura")
        .groupBy("data_referencia")
        .count()
        .collect()
    }
    resultados = []
    for j in sorted(set(janelas) & set(contagens)):
        anteriores = sorted(d for d in contagens if d < j)[-VARREDURAS_MEDIA:]
        atual = contagens[j]
        if len(anteriores) < 2:
            resultados.append(
                Resultado(
                    "DQ-17",
                    ent,
                    j.isoformat(),
                    "bronze",
                    atual,
                    0,
                    "histórico insuficiente (< 2 varreduras)",
                )
            )
            continue
        media = sum(contagens[d] for d in anteriores) / len(anteriores)
        fora = media > 0 and abs(atual - media) / media > settings.volume_tolerancia
        resultados.append(
            Resultado(
                "DQ-17",
                ent,
                j.isoformat(),
                "bronze",
                atual,
                atual if fora else 0,
                f"linhas={atual} média{len(anteriores)}varreduras={media:.0f} "
                f"desvio={(atual - media) / media:+.0%}"
                if media
                else "média zero",
            )
        )
    return resultados
