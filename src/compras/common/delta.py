"""Helpers de escrita Delta: criação, replaceWhere e propriedades das tabelas."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from compras.common.runtime import em_databricks

log = logging.getLogger(__name__)

PROPRIEDADES_DATABRICKS = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def tabela_existe(spark: SparkSession, nome: str) -> bool:
    # spark.catalog.tableExists não aceita nome de 3 partes no Spark OSS 3.5; resolver o
    # schema da tabela funciona igual no Databricks (UC) e localmente.
    try:
        spark.table(nome).schema  # noqa: B018
        return True
    except AnalysisException:
        return False


def _propriedades(spark: SparkSession, nome: str) -> None:
    if not em_databricks():  # Delta OSS rejeita chaves delta.* desconhecidas
        return
    props = ", ".join(f"'{k}' = '{v}'" for k, v in PROPRIEDADES_DATABRICKS.items())
    spark.sql(f"ALTER TABLE {nome} SET TBLPROPERTIES ({props})")


def garantir_tabela(
    spark: SparkSession,
    nome: str,
    schema: StructType,
    particoes: Sequence[str] = (),
    comentario: str | None = None,
) -> None:
    """Cria a tabela vazia se não existir (MERGE exige o destino criado)."""
    if tabela_existe(spark, nome):
        return
    writer = spark.createDataFrame([], schema).write.format("delta").mode("ignore")
    if particoes:
        writer = writer.partitionBy(*particoes)
    writer.saveAsTable(nome)
    _propriedades(spark, nome)
    if comentario:
        spark.sql(f"COMMENT ON TABLE {nome} IS '{comentario.replace(chr(39), chr(39) * 2)}'")
    log.info("Tabela criada: %s", nome)


def sobrescrever_particao(
    df: DataFrame,
    nome: str,
    predicado: str,
    particoes: Sequence[str] = (),
    merge_schema: bool = False,
) -> None:
    """Sobrescreve só o recorte ``predicado`` (idempotência por janela/partição)."""
    spark = df.sparkSession
    writer = df.write.format("delta")
    if particoes:
        writer = writer.partitionBy(*particoes)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if not tabela_existe(spark, nome):
        writer.mode("overwrite").saveAsTable(nome)
        _propriedades(spark, nome)
        return
    writer.mode("overwrite").option("replaceWhere", predicado).saveAsTable(nome)


def colunas(spark: SparkSession, nome: str) -> list[str]:
    return spark.table(nome).columns if tabela_existe(spark, nome) else []


def sql_lista(valores: Sequence[object]) -> str:
    """Formata uma lista para ``IN (...)`` escapando aspas."""
    itens = []
    for v in valores:
        if isinstance(v, int | float):
            itens.append(str(v))
        else:
            itens.append("'" + str(v).replace("'", "''") + "'")
    return ", ".join(itens) if itens else "NULL"
