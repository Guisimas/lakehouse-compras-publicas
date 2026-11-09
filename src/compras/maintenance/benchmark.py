"""Benchmark das 5 consultas de negócio e volume por camada.

Uso: ``compras-task benchmark --rotulo antes`` → ``otimizar`` → ``--rotulo depois``.
Cada consulta roda 3 vezes, com cache limpo, e grava a mediana em
``quality.benchmark_consultas``. Arquivos e bytes das tabelas de fato são registrados
junto (DESCRIBE DETAIL); os bytes efetivamente escaneados por consulta ficam no Query
Profile do Databricks, que não é exposto pela API do Spark Connect/serverless.
"""

from __future__ import annotations

import contextlib
import logging
import statistics
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from compras.common.delta import tabela_existe
from compras.common.runtime import agora_utc
from compras.config import ENTIDADES, Settings
from compras.consultas import consultas_negocio, sem_comentarios

log = logging.getLogger(__name__)

SCHEMA_BENCH = StructType(
    [
        StructField("rotulo", StringType()),
        StructField("consulta", StringType()),
        StructField("executado_em", TimestampType()),
        StructField("mediana_s", DoubleType()),
        StructField("tempos_s", ArrayType(DoubleType())),
        StructField("linhas", LongType()),
        StructField("arquivos_tabela", LongType()),
        StructField("bytes_tabela", LongType()),
    ]
)
SCHEMA_VOLUME = StructType(
    [
        StructField("medido_em", TimestampType()),
        StructField("camada", StringType()),
        StructField("tabela", StringType()),
        StructField("arquivos", LongType()),
        StructField("bytes", LongType()),
    ]
)


def _detalhe(spark: SparkSession, tabela: str) -> tuple[int, int]:
    d = spark.sql(f"DESCRIBE DETAIL {tabela}").first()
    return int(d.numFiles or 0), int(d.sizeInBytes or 0)


def medir_volume(spark: SparkSession, settings: Settings) -> None:
    agora = agora_utc()
    linhas = []
    tabelas = [("bronze", settings.bronze(e)) for e in ENTIDADES]
    tabelas += [("silver", settings.silver(e)) for e in ENTIDADES]
    tabelas += [("quarentena", settings.quarentena(e)) for e in ENTIDADES]
    tabelas += [
        ("gold", settings.gold(n))
        for n in (
            "fato_compras",
            "fato_contratos",
            "dim_fornecedor",
            "dim_orgao",
            "dim_item",
            "dim_modalidade",
            "dim_data",
        )
    ]
    for camada, tabela in tabelas:
        if tabela_existe(spark, tabela):
            arquivos, tamanho = _detalhe(spark, tabela)
            linhas.append((agora, camada, tabela, arquivos, tamanho))
    spark.createDataFrame(linhas, SCHEMA_VOLUME).write.format("delta").mode("append").saveAsTable(
        settings.tabela("quality", "volume_tabelas")
    )


def executar_benchmark(
    spark: SparkSession, settings: Settings, rotulo: str, repeticoes: int = 3
) -> list[tuple]:
    spark.sql(f"USE CATALOG {settings.catalog}")
    arquivos_fc, bytes_fc = _detalhe(spark, settings.gold("fato_compras"))
    arquivos_ct, bytes_ct = _detalhe(spark, settings.gold("fato_contratos"))
    linhas = []
    for nome, sql in consultas_negocio().items():
        consulta = sem_comentarios(sql)
        tempos = []
        n = 0
        for _ in range(repeticoes):
            # serverless não tem cache de DataFrame para limpar
            with contextlib.suppress(Exception):
                spark.sql("CLEAR CACHE")
            t0 = time.perf_counter()
            n = len(spark.sql(consulta).collect())
            tempos.append(round(time.perf_counter() - t0, 3))
        usa_contratos = "fato_contratos" in consulta
        linhas.append(
            (
                rotulo,
                nome,
                agora_utc(),
                float(statistics.median(tempos)),
                tempos,
                n,
                arquivos_ct if usa_contratos else arquivos_fc,
                bytes_ct if usa_contratos else bytes_fc,
            )
        )
        log.info("%s [%s]: mediana %.2fs (%s)", nome, rotulo, statistics.median(tempos), tempos)
    spark.createDataFrame(linhas, SCHEMA_BENCH).write.format("delta").mode("append").saveAsTable(
        settings.tabela("quality", "benchmark_consultas")
    )
    medir_volume(spark, settings)
    return linhas
