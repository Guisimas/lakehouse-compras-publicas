"""Helpers da Gold: surrogate keys, SCD1, membro desconhecido e publicação de fatos.

Publicação de fato: a fato é montada numa tabela de staging, as regras
críticas (DQ-13/14/15) rodam contra o staging e só então as partições afetadas são
substituídas na tabela final com ``replaceWhere``. Regra crítica reprovada = nada publicado.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from compras.common import exprs
from compras.common.delta import sobrescrever_particao, sql_lista, tabela_existe
from compras.config import Settings

log = logging.getLogger(__name__)

SK_DESCONHECIDO = -1


def sk_data(c: Column) -> Column:
    """Chave da dim_data no formato yyyymmdd; -1 para data nula."""
    d = c.cast("date")
    return F.coalesce(
        (F.year(d) * 10000 + F.month(d) * 100 + F.dayofmonth(d)).cast("int"),
        F.lit(SK_DESCONHECIDO),
    )


def normalizar_codigo(c: Column) -> Column:
    """Código numérico sem zeros à esquerda ('05' → '5')."""
    s = F.trim(c.cast("string"))
    return F.when(s.isNull() | (s == ""), None).otherwise(F.regexp_replace(s, r"^0+(?=.)", ""))


def chave_modalidade(regime: Column, codigo: Column) -> Column:
    cod = normalizar_codigo(codigo)
    return F.when(cod.isNotNull(), F.concat_ws(":", F.coalesce(regime, F.lit("indefinido")), cod))


def upsert_scd1(
    spark: SparkSession,
    tabela: str,
    df: DataFrame,
    chave: str,
    sk: str,
    atributos: Sequence[str],
) -> None:
    """SCD tipo 1: sobrescreve atributos quando mudam; sk estável pela chave natural."""
    origem = df.withColumn("hash_atributos", exprs.hash_colunas(*atributos))
    if not tabela_existe(spark, tabela):
        origem.write.format("delta").saveAsTable(tabela)
        return
    (
        DeltaTable.forName(spark, tabela)
        .alias("t")
        .merge(origem.alias("s"), f"t.{chave} = s.{chave}")
        .whenMatchedUpdateAll(condition="t.hash_atributos <> s.hash_atributos")
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("SCD1 %s atualizada", tabela)


def schema_anulavel(schema: StructType) -> StructType:
    """Mesmo schema com todos os campos anuláveis (para a linha sk = -1)."""
    return StructType([StructField(f.name, f.dataType, True) for f in schema.fields])


def lookup(
    fato: DataFrame, dim: DataFrame, cond_cols: Sequence[tuple[str, str]], sk: str, alias: str
) -> DataFrame:
    """Left join na dimensão; sem correspondência → sk = -1."""
    d = dim.select(
        *[F.col(dc).alias(f"__{alias}_{dc}") for _, dc in cond_cols],
        F.col(sk).alias(f"__{alias}_sk"),
    )
    cond = None
    for fc, dc in cond_cols:
        c = F.col(fc) == F.col(f"__{alias}_{dc}")
        cond = c if cond is None else cond & c
    out = fato.join(d, cond, "left")
    out = out.withColumn(
        alias, F.coalesce(F.col(f"__{alias}_sk"), F.lit(SK_DESCONHECIDO)).cast("long")
    )
    return out.drop(*[c for c in out.columns if c.startswith(f"__{alias}_")])


def anos_meses(df: DataFrame, coluna: str = "ano_mes") -> list[int]:
    return sorted(r[0] for r in df.select(coluna).distinct().collect() if r[0] is not None)


def publicar_fato(
    spark: SparkSession,
    settings: Settings,
    nome: str,
    staging: DataFrame,
    particoes_afetadas: Sequence[int],
) -> None:
    """Substitui só as partições ``ano_mes`` afetadas (idempotente por partição)."""
    if not particoes_afetadas:
        return
    predicado = f"ano_mes IN ({sql_lista(list(particoes_afetadas))})"
    # mergeSchema: coluna nova na fato entra na tabela existente sem recriá-la.
    sobrescrever_particao(
        staging, settings.gold(nome), predicado, particoes=("ano_mes",), merge_schema=True
    )
    log.info("Fato %s publicada: partições %s", nome, list(particoes_afetadas)[:12])


def gravar_staging(spark: SparkSession, settings: Settings, nome: str, df: DataFrame) -> DataFrame:
    """Materializa a fato montada antes das checagens (sem depender de cache no serverless)."""
    tabela = settings.gold(f"_stg_{nome}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    return spark.table(tabela)
