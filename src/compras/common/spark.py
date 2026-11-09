"""Sessão Spark: no Databricks usa a sessão do runtime; localmente sobe Spark + Delta."""

from __future__ import annotations

from typing import TYPE_CHECKING

from compras.common.runtime import em_databricks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def obter_spark() -> SparkSession:
    from pyspark.sql import SparkSession

    if em_databricks():
        return SparkSession.builder.getOrCreate()
    ativa = SparkSession.getActiveSession()
    if ativa is not None:
        return ativa
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("compras-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        # Serverless roda com ANSI ligado; testamos do mesmo jeito.
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
