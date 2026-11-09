"""Dimensões estáticas e SCD tipo 1: dim_data, dim_modalidade, dim_orgao, dim_item."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from compras.common import exprs
from compras.common.delta import tabela_existe
from compras.config import Settings
from compras.gold.comum import (
    SK_DESCONHECIDO,
    chave_modalidade,
    normalizar_codigo,
    schema_anulavel,
    upsert_scd1,
)
from compras.ingestion.endpoints import MODALIDADES_14133

NOMES_MES = [
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
]
# dayofweek do Spark: 1 = domingo
NOMES_DIA = ["domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado"]


# ------------------------------------------------------------------ dim_data
def construir_dim_data(
    spark: SparkSession,
    settings: Settings,
    inicio: date = date(2010, 1, 1),
    fim: date = date(2035, 12, 31),
) -> None:
    """Dimensão gerada (estática). Reescrita inteira: pequena e determinística."""
    datas = spark.sql(
        f"SELECT explode(sequence(DATE'{inicio}', DATE'{fim}', INTERVAL 1 DAY)) AS data"
    )
    nomes_mes = F.array(*[F.lit(n) for n in NOMES_MES])
    nomes_dia = F.array(*[F.lit(n) for n in NOMES_DIA])
    dim = datas.select(
        (F.year("data") * 10000 + F.month("data") * 100 + F.dayofmonth("data"))
        .cast("int")
        .alias("sk_data"),
        "data",
        F.year("data").alias("ano"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.element_at(nomes_mes, F.month("data")).alias("nome_mes"),
        (F.year("data") * 100 + F.month("data")).cast("int").alias("ano_mes"),
        F.dayofmonth("data").alias("dia"),
        F.dayofweek("data").alias("dia_semana"),
        F.element_at(nomes_dia, F.dayofweek("data")).alias("nome_dia_semana"),
        F.dayofweek("data").isin(1, 7).alias("fim_de_semana"),
        F.year("data").alias("ano_fiscal"),  # exercício financeiro federal = ano civil
    )
    desconhecida = spark.createDataFrame(
        [
            (
                SK_DESCONHECIDO,
                None,
                None,
                None,
                None,
                "desconhecido",
                None,
                None,
                None,
                "desconhecido",
                None,
                None,
            )
        ],
        schema_anulavel(dim.schema),
    )
    dim.unionByName(desconhecida).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(settings.gold("dim_data"))


# ------------------------------------------------------------------ dim_modalidade
_GRUPO_14133 = {
    1: "Leilão",
    2: "Diálogo competitivo",
    3: "Concurso",
    4: "Concorrência",
    5: "Concorrência",
    6: "Pregão",
    7: "Pregão",
    8: "Dispensa",
    9: "Inexigibilidade",
    10: "Manifestação de interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão",
    14: "Inaplicabilidade",
}
# Códigos do SIASG (Lei 8.666/RDC). Conferir com os nomes que a própria fonte devolve:
# quando a Silver traz nome_modalidade, ele prevalece sobre este seed.
_MODALIDADES_8666 = {
    1: ("Convite", "Convite"),
    2: ("Tomada de Preços", "Tomada de preços"),
    3: ("Concorrência", "Concorrência"),
    4: ("Concorrência Internacional", "Concorrência"),
    5: ("Pregão", "Pregão"),
    6: ("Dispensa de Licitação", "Dispensa"),
    7: ("Inexigibilidade de Licitação", "Inexigibilidade"),
    20: ("Concurso", "Concurso"),
    22: ("Tomada de Preços por Técnica e Preço", "Tomada de preços"),
    33: ("Concorrência por Técnica e Preço", "Concorrência"),
    44: ("Concorrência Internacional por Técnica e Preço", "Concorrência"),
    99: ("RDC", "RDC"),
}
SEM_DISPUTA = {"Dispensa", "Inexigibilidade", "Inaplicabilidade"}

SCHEMA_MODALIDADE = StructType(
    [
        StructField("modalidade_chave", StringType()),
        StructField("regime_legal", StringType()),
        StructField("codigo", StringType()),
        StructField("nome", StringType()),
        StructField("grupo", StringType()),
        StructField("sem_licitacao", BooleanType()),
        StructField("origem", StringType()),
    ]
)


def _seed_modalidades(spark: SparkSession) -> DataFrame:
    linhas = [
        (f"14133:{c}", "14133", str(c), n, _GRUPO_14133[c], _GRUPO_14133[c] in SEM_DISPUTA, "seed")
        for c, n in MODALIDADES_14133.items()
    ] + [
        (f"8666:{c}", "8666", str(c), n, g, g in SEM_DISPUTA, "seed")
        for c, (n, g) in _MODALIDADES_8666.items()
    ]
    return spark.createDataFrame(linhas, SCHEMA_MODALIDADE)


def modalidades_observadas(spark: SparkSession, settings: Settings) -> DataFrame:
    """Chaves de modalidade que aparecem na Silver, com o nome que a fonte usa."""
    partes = []
    if tabela_existe(spark, settings.silver("licitacoes")):
        partes.append(
            spark.table(settings.silver("licitacoes")).select(
                chave_modalidade(F.col("regime_legal"), F.col("codigo_modalidade")).alias(
                    "modalidade_chave"
                ),
                F.col("regime_legal"),
                normalizar_codigo(F.col("codigo_modalidade")).alias("codigo"),
                F.col("nome_modalidade").alias("nome"),
            )
        )
    if tabela_existe(spark, settings.silver("contratos")):
        partes.append(
            chaves_modalidade_contratos(spark, settings).select(
                "modalidade_chave", "regime_legal", "codigo", F.col("nome_modalidade").alias("nome")
            )
        )
    if not partes:
        return spark.createDataFrame(
            [], "modalidade_chave string, regime_legal string, codigo string, nome string"
        )
    obs = partes[0]
    for p in partes[1:]:
        obs = obs.unionByName(p)
    return (
        obs.where(F.col("modalidade_chave").isNotNull())
        .groupBy("modalidade_chave")
        .agg(
            F.first("regime_legal", ignorenulls=True).alias("regime_legal"),
            F.first("codigo", ignorenulls=True).alias("codigo"),
            F.max("nome").alias("nome"),
        )
    )


def chaves_modalidade_contratos(spark: SparkSession, settings: Settings) -> DataFrame:
    """Modalidade do contrato: a da licitação de origem (por id_compra) quando existe;
    senão ``indefinido:<codigo>``, porque o módulo Contratos não informa o regime legal."""
    c = spark.table(settings.silver("contratos")).select(
        "id_contrato", "id_compra", "codigo_modalidade", "nome_modalidade", "regime_legal"
    )
    if tabela_existe(spark, settings.silver("licitacoes")):
        lic = spark.table(settings.silver("licitacoes")).select(
            F.col("id_compra").alias("_idc"),
            F.col("regime_legal").alias("_reg"),
            F.col("codigo_modalidade").alias("_cod"),
        )
        c = c.join(lic, c.id_compra == lic._idc, "left")
    else:
        c = c.select("*", F.lit(None).alias("_reg"), F.lit(None).alias("_cod"))
    regime = F.coalesce(
        F.col("_reg"),
        F.when(F.col("regime_legal") != "indefinido", F.col("regime_legal")),
        F.lit("indefinido"),
    )
    codigo = F.coalesce(F.col("_cod"), F.col("codigo_modalidade"))
    return c.select(
        "id_contrato",
        chave_modalidade(regime, codigo).alias("modalidade_chave"),
        regime.alias("regime_legal"),
        normalizar_codigo(codigo).alias("codigo"),
        "nome_modalidade",
    )


def construir_dim_modalidade(spark: SparkSession, settings: Settings) -> None:
    seed = _seed_modalidades(spark)
    obs = modalidades_observadas(spark, settings)
    # nome da fonte prevalece; modalidade não prevista no seed entra como "observada"
    junt = seed.alias("s").join(obs.alias("o"), "modalidade_chave", "full_outer")
    grupo = F.coalesce(F.col("s.grupo"), F.lit("Outras"))
    dim = junt.select(
        exprs.sk_estavel(F.col("modalidade_chave")).alias("sk_modalidade"),
        "modalidade_chave",
        F.coalesce("s.regime_legal", "o.regime_legal").alias("regime_legal"),
        F.coalesce("s.codigo", "o.codigo").alias("codigo"),
        F.coalesce("o.nome", "s.nome", F.concat(F.lit("Modalidade "), F.col("o.codigo"))).alias(
            "nome"
        ),
        grupo.alias("grupo"),
        F.coalesce("s.sem_licitacao", F.lit(False)).alias("sem_licitacao"),
        F.when(F.col("s.origem").isNotNull(), "seed").otherwise("observada").alias("origem"),
    )
    desconhecida = spark.createDataFrame(
        [
            (
                SK_DESCONHECIDO,
                "desconhecido",
                None,
                None,
                "Desconhecida",
                "Desconhecida",
                None,
                "sistema",
            )
        ],
        schema_anulavel(dim.schema),
    )
    upsert_scd1(
        spark,
        settings.gold("dim_modalidade"),
        dim.unionByName(desconhecida),
        "modalidade_chave",
        "sk_modalidade",
        ("nome", "grupo", "sem_licitacao", "regime_legal", "codigo", "origem"),
    )


# ------------------------------------------------------------------ dim_orgao
# O cadastro de órgãos da API traz códigos de uma letra.
ESFERAS = {"F": "Federal", "E": "Estadual", "M": "Municipal", "D": "Distrital"}
PODERES = {"E": "Executivo", "L": "Legislativo", "J": "Judiciário"}


def _decodificar(coluna: str, nomes: dict[str, str]):
    """Código de uma letra → nome; valor fora do dicionário passa como veio."""
    expr = F.col(coluna)
    for codigo, nome in nomes.items():
        expr = F.when(F.upper(F.trim(F.col(coluna))) == codigo, F.lit(nome)).otherwise(expr)
    return expr.alias(coluna)


def _silver_ou_vazia(spark: SparkSession, settings: Settings, entidade: str) -> DataFrame:
    from compras.silver.schemas import ENTIDADES_SILVER
    from compras.silver.transform import schema_silver

    tabela = settings.silver(entidade)
    if tabela_existe(spark, tabela):
        return spark.table(tabela)
    return spark.createDataFrame([], schema_silver(ENTIDADES_SILVER[entidade]))


def construir_dim_orgao(spark: SparkSession, settings: Settings) -> None:
    src = _silver_ou_vazia(spark, settings, "orgaos")
    dim = src.select(
        exprs.sk_estavel(F.col("codigo_uasg")).alias("sk_orgao"),
        "codigo_uasg",
        "nome_uasg",
        "uf",
        "municipio",
        "codigo_orgao",
        "nome_orgao",
        "codigo_orgao_superior",
        "nome_orgao_superior",
        _decodificar("esfera", ESFERAS),
        _decodificar("poder", PODERES),
        "tipo_administracao",
        "uso_sisg",
        "ativo",
    )
    desconhecido = spark.createDataFrame(
        [
            (
                SK_DESCONHECIDO,
                "-1",
                "Desconhecido",
                None,
                None,
                None,
                "Desconhecido",
                None,
                "Desconhecido",
                None,
                None,
                None,
                None,
                None,
            )
        ],
        schema_anulavel(dim.schema),
    )
    upsert_scd1(
        spark,
        settings.gold("dim_orgao"),
        dim.unionByName(desconhecido),
        "codigo_uasg",
        "sk_orgao",
        [c for c in dim.columns if c not in ("sk_orgao", "codigo_uasg")],
    )


# ------------------------------------------------------------------ dim_item
def construir_dim_item(spark: SparkSession, settings: Settings) -> None:
    src = _silver_ou_vazia(spark, settings, "catalogo")
    chave = F.concat_ws(":", "tipo_item", "codigo_item")
    dim = src.select(
        exprs.sk_estavel(chave).alias("sk_item"),
        chave.alias("item_chave"),
        "tipo_item",
        F.when(F.col("tipo_item") == "M", "Material")
        .when(F.col("tipo_item") == "S", "Serviço")
        .alias("nome_tipo_item"),
        "codigo_item",
        "descricao",
        "codigo_grupo",
        "nome_grupo",
        "codigo_classe",
        "nome_classe",
        "codigo_subclasse_pdm",
        "nome_subclasse_pdm",
        "ativo",
    )
    desconhecido = spark.createDataFrame(
        [
            (
                SK_DESCONHECIDO,
                "desconhecido",
                None,
                None,
                None,
                "Item não catalogado",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
        ],
        schema_anulavel(dim.schema),
    )
    upsert_scd1(
        spark,
        settings.gold("dim_item"),
        dim.unionByName(desconhecido),
        "item_chave",
        "sk_item",
        [c for c in dim.columns if c not in ("sk_item", "item_chave")],
    )
