"""Schemas das tabelas de controle, qualidade e quarentena, e criação da estrutura no UC."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from compras.common.delta import garantir_tabela
from compras.common.runtime import em_databricks
from compras.config import ENTIDADES_QUARENTENA, Settings

log = logging.getLogger(__name__)

SCHEMAS_UC = ("landing", "bronze", "silver", "gold", "quality", "ctl")

CTL_INGESTAO_LOG = StructType(
    [
        StructField("log_id", StringType()),
        StructField("run_id", StringType()),
        StructField("entidade", StringType()),
        StructField("janela", StringType()),
        StructField("fonte_dados", StringType()),
        StructField("modo", StringType()),
        StructField("inicio", TimestampType()),
        StructField("fim", TimestampType()),
        StructField("duracao_s", DoubleType()),
        StructField("endpoints", LongType()),
        StructField("fanouts", LongType()),
        StructField("fanouts_pulados", LongType()),
        StructField("paginas", LongType()),
        StructField("linhas", LongType()),
        StructField("status", StringType()),
        StructField("erro", StringType()),
    ]
)

CTL_WATERMARK = StructType(
    [
        StructField("entidade", StringType()),
        StructField("camada", StringType()),  # landing (por entidade) | gold (entidade "pipeline")
        StructField("ultima_janela", DateType()),
        StructField("status", StringType()),
        StructField("atualizado_em", TimestampType()),
        StructField("run_id", StringType()),
    ]
)

DQ_RESULTADOS = StructType(
    [
        StructField("run_id", StringType()),
        StructField("regra", StringType()),
        StructField("dimensao", StringType()),
        StructField("severidade", StringType()),
        StructField("entidade", StringType()),
        StructField("janela", StringType()),
        StructField("camada", StringType()),
        StructField("linhas_avaliadas", LongType()),
        StructField("linhas_reprovadas", LongType()),
        StructField("status", StringType()),  # ok | aviso | quarentena | falha
        StructField("detalhe", StringType()),
        StructField("data_execucao", TimestampType()),
    ]
)

QUARENTENA = StructType(
    [
        StructField("chave_registro", StringType()),
        StructField("regra_violada", ArrayType(StringType())),
        StructField("motivo_rejeicao", StringType()),
        StructField("camada_origem", StringType()),
        StructField("data_processamento", TimestampType()),
        StructField("run_id", StringType()),
        StructField("data_referencia", DateType()),
        StructField("endpoint", StringType()),
        StructField("arquivo_origem", StringType()),
        StructField("hash_registro", StringType()),
        StructField("payload", StringType()),
        StructField("resolvido", BooleanType()),
        StructField("data_resolucao", TimestampType()),
    ]
)


def criar_estrutura(spark: SparkSession, settings: Settings) -> None:
    """Idempotente: catálogo, schemas, volume do landing e tabelas de controle/qualidade."""
    cat = settings.catalog
    if em_databricks():
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat}")
        except Exception as exc:  # Free Edition pode negar; use um catálogo existente
            log.warning("Não foi possível criar o catálogo %s (%s); assumindo existente", cat, exc)
    for schema in SCHEMAS_UC:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{schema}")
    if em_databricks():
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cat}.landing.raw")

    garantir_tabela(
        spark,
        settings.ctl_ingestao_log,
        CTL_INGESTAO_LOG,
        comentario="Log de execução da ingestão",
    )
    garantir_tabela(
        spark,
        settings.ctl_watermark,
        CTL_WATERMARK,
        comentario="Última janela processada por entidade e camada",
    )
    garantir_tabela(
        spark,
        settings.dq_resultados,
        DQ_RESULTADOS,
        comentario="Resultado de cada regra de qualidade por run/entidade/janela",
    )
    for entidade in ENTIDADES_QUARENTENA:
        garantir_tabela(
            spark,
            settings.quarentena(entidade),
            QUARENTENA,
            comentario=f"Registros reprovados de {entidade}",
        )
