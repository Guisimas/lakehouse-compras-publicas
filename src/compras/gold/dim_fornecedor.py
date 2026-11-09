"""dim_fornecedor — SCD tipo 2.

Atributos rastreados (``hash_atributos``): razão social, porte, UF, município, situação
(ativo/habilitado) e origem do cadastro. Uma única operação MERGE por carga, com o padrão
de staging "merge_key nulo":

- fornecedor novo → ``merge_key = ni`` não casa → INSERT da versão atual, vigente desde
  1900-01-01 (cobre fatos anteriores à primeira carga);
- hash mudou → a linha com ``merge_key = ni`` casa e FECHA a versão atual
  (``data_fim_vigencia = data_carga - 1``); a linha com ``merge_key = NULL`` não casa e
  INSERE a nova versão, vigente desde ``data_carga``;
- mudança no mesmo dia em que a versão foi aberta → atualiza no lugar (evita vigência
  invertida e sk repetida);
- hash igual → nada acontece (idempotente).

Fornecedores que aparecem em itens/contratos mas ainda não foram consultados no cadastro
entram como membros inferidos (``origem_cadastro = 'inferido'``); quando o cadastro chega,
o hash muda e uma nova versão é aberta.
"""

from __future__ import annotations

import logging
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from compras.common import exprs
from compras.common.delta import tabela_existe
from compras.config import Settings
from compras.gold.comum import SK_DESCONHECIDO, schema_anulavel

log = logging.getLogger(__name__)

INICIO_PADRAO = date(1900, 1, 1)
FIM_ABERTO = date(9999, 12, 31)
ATRIBUTOS_RASTREADOS = (
    "razao_social",
    "porte_nome",
    "uf",
    "municipio",
    "situacao",
    "origem_cadastro",
)
COLUNAS = (
    "ni_fornecedor",
    "tipo_pessoa",
    "razao_social",
    "porte_id",
    "porte_nome",
    "natureza_juridica_nome",
    "uf",
    "municipio",
    "codigo_cnae",
    "nome_cnae",
    "ativo",
    "habilitado_licitar",
    "situacao",
    "origem_cadastro",
)


def _cadastro(spark: SparkSession, settings: Settings) -> DataFrame:
    f = spark.table(settings.silver("fornecedores"))
    situacao = F.concat_ws(
        "/",
        F.when(F.col("ativo"), "ativo").when(~F.col("ativo"), "inativo").otherwise("?"),
        F.when(F.col("habilitado_licitar"), "habilitado")
        .when(~F.col("habilitado_licitar"), "nao_habilitado")
        .otherwise("?"),
    )
    return f.select(
        "ni_fornecedor",
        "tipo_pessoa",
        "razao_social",
        "porte_id",
        "porte_nome",
        "natureza_juridica_nome",
        "uf",
        "municipio",
        "codigo_cnae",
        "nome_cnae",
        "ativo",
        "habilitado_licitar",
        situacao.alias("situacao"),
        F.lit("cadastro").alias("origem_cadastro"),
    )


def _inferidos(spark: SparkSession, settings: Settings) -> DataFrame | None:
    partes = []
    for ent in ("itens_compra", "contratos"):
        if tabela_existe(spark, settings.silver(ent)):
            partes.append(
                spark.table(settings.silver(ent))
                .where(F.col("ni_fornecedor").isNotNull())
                # PE: NI mascarado e sem cadastro; vários estrangeiros colapsariam num só
                .where(F.coalesce(F.col("tipo_pessoa"), F.lit("")) != "PE")
                .select(
                    "ni_fornecedor", "tipo_pessoa", F.col("nome_fornecedor").alias("razao_social")
                )
            )
    if not partes:
        return None
    u = partes[0]
    for p in partes[1:]:
        u = u.unionByName(p)
    return (
        u.groupBy("ni_fornecedor")
        .agg(
            F.first("tipo_pessoa", ignorenulls=True).alias("tipo_pessoa"),
            F.max("razao_social").alias("razao_social"),  # determinístico
        )
        .select(
            "ni_fornecedor",
            "tipo_pessoa",
            "razao_social",
            F.lit(None).cast("int").alias("porte_id"),
            F.lit(None).cast("string").alias("porte_nome"),
            F.lit(None).cast("string").alias("natureza_juridica_nome"),
            F.lit(None).cast("string").alias("uf"),
            F.lit(None).cast("string").alias("municipio"),
            F.lit(None).cast("int").alias("codigo_cnae"),
            F.lit(None).cast("string").alias("nome_cnae"),
            F.lit(None).cast("boolean").alias("ativo"),
            F.lit(None).cast("boolean").alias("habilitado_licitar"),
            F.lit(None).cast("string").alias("situacao"),
            F.lit("inferido").alias("origem_cadastro"),
        )
    )


def _criar(spark: SparkSession, tabela: str, exemplo: DataFrame) -> None:
    vazio = exemplo.limit(0).select(
        F.lit(None).cast("long").alias("sk_fornecedor"),
        *COLUNAS,
        F.lit(None).cast("string").alias("hash_atributos"),
        F.lit(None).cast("date").alias("data_inicio_vigencia"),
        F.lit(None).cast("date").alias("data_fim_vigencia"),
        F.lit(None).cast("boolean").alias("registro_atual"),
    )
    vazio.write.format("delta").saveAsTable(tabela)
    desconhecido = spark.createDataFrame(
        [
            (
                SK_DESCONHECIDO,
                "DESCONHECIDO",
                None,
                "Fornecedor não identificado",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,  # porte_id..situacao
                "sistema",
                None,
                INICIO_PADRAO,
                FIM_ABERTO,
                True,
            )
        ],
        schema_anulavel(vazio.schema),
    )
    desconhecido.write.format("delta").mode("append").saveAsTable(tabela)


def construir_dim_fornecedor(spark: SparkSession, settings: Settings, data_carga: date) -> dict:
    tabela = settings.gold("dim_fornecedor")
    cadastro = (
        _cadastro(spark, settings)
        if tabela_existe(spark, settings.silver("fornecedores"))
        else None
    )
    inferidos = _inferidos(spark, settings)
    if cadastro is None and inferidos is None:
        log.info("Sem fornecedores na Silver; dim_fornecedor não alterada")
        return {}
    if cadastro is not None and inferidos is not None:
        inferidos = inferidos.join(cadastro.select("ni_fornecedor"), "ni_fornecedor", "left_anti")
    exemplo = cadastro if cadastro is not None else inferidos
    if not tabela_existe(spark, tabela):
        _criar(spark, tabela, exemplo)

    atual = spark.table(tabela).where("registro_atual = true")
    ja_existe = spark.table(tabela).select("ni_fornecedor").distinct()

    fontes = []
    if cadastro is not None:
        fontes.append(cadastro)
    if inferidos is not None:
        # inferido só entra se o fornecedor nunca existiu na dimensão
        fontes.append(inferidos.join(ja_existe, "ni_fornecedor", "left_anti"))
    src = fontes[0]
    for f in fontes[1:]:
        src = src.unionByName(f)
    src = src.withColumn("hash_atributos", exprs.hash_colunas(*ATRIBUTOS_RASTREADOS))

    carga = F.lit(data_carga)
    # versões novas de fornecedores que mudaram em dia anterior à carga
    mudaram = (
        src.alias("s")
        .join(atual.alias("t"), "ni_fornecedor")
        .where(
            (F.col("t.hash_atributos") != F.col("s.hash_atributos"))
            & (F.col("t.data_inicio_vigencia") < carga)
        )
        .select("s.*")
    )
    staged = src.select(F.col("ni_fornecedor").alias("merge_key"), "*").unionByName(
        mudaram.select(F.lit(None).cast("string").alias("merge_key"), "*")
    )
    inicio = F.when(F.col("merge_key").isNull(), carga).otherwise(F.lit(INICIO_PADRAO))
    staged = staged.withColumn("inicio_novo", inicio).withColumn(
        "sk_novo", exprs.sk_estavel(F.col("ni_fornecedor"), F.col("inicio_novo"))
    )

    valores_insert = {c: f"s.{c}" for c in COLUNAS}
    valores_insert |= {
        "sk_fornecedor": "s.sk_novo",
        "hash_atributos": "s.hash_atributos",
        "data_inicio_vigencia": "s.inicio_novo",
        "data_fim_vigencia": f"DATE'{FIM_ABERTO}'",
        "registro_atual": "true",
    }
    dia_carga = f"DATE'{data_carga.isoformat()}'"
    (
        DeltaTable.forName(spark, tabela)
        .alias("t")
        .merge(staged.alias("s"), "t.ni_fornecedor = s.merge_key AND t.registro_atual = true")
        # mudança no mesmo dia em que a versão abriu: corrige no lugar
        .whenMatchedUpdate(
            condition=(
                f"t.hash_atributos <> s.hash_atributos AND t.data_inicio_vigencia >= {dia_carga}"
            ),
            set={**{c: f"s.{c}" for c in COLUNAS}, "hash_atributos": "s.hash_atributos"},
        )
        # mudança: fecha a versão vigente
        .whenMatchedUpdate(
            condition="t.hash_atributos <> s.hash_atributos",
            set={
                "data_fim_vigencia": f"date_sub({dia_carga}, 1)",
                "registro_atual": "false",
            },
        )
        .whenNotMatchedInsert(values=valores_insert)
        .execute()
    )
    dim = spark.table(tabela)
    stats = {
        "versoes": dim.count(),
        "fornecedores": dim.select("ni_fornecedor").distinct().count(),
        "com_historico": dim.groupBy("ni_fornecedor").count().where("count > 1").count(),
    }
    log.info("dim_fornecedor: %s", stats)
    return stats
