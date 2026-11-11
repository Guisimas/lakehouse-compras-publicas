"""Bronze → Silver com quarentena.

Para cada entidade, processa as linhas da Bronze gravadas neste run (``run_id``):

1. DQ-01: confere se as colunas de origem esperadas existem e vêm preenchidas.
2. Mapeia cada endpoint para o schema canônico, converte tipos e normaliza (CNPJ, CPF
   mascarado, UASG com 6 dígitos).
3. Aplica DQ-02..DQ-08; reprovados vão para ``quarentena_<entidade>`` (um registro por
   linha, com todas as regras violadas).
4. Deduplica pela chave natural (registro mais recente; empate resolvido pela prioridade
   da fonte: PNCP > Legado > CSV) e faz MERGE na Silver. Linha igual (mesmo hash) não é
   reescrita, o que torna o reprocessamento idempotente.
5. DQ-09 (unicidade, crítica) e DQ-18 (% rejeitados, aviso).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from compras.common import exprs
from compras.common.delta import garantir_tabela, tabela_existe
from compras.config import Settings
from compras.quality.results import RegistradorDQ, Resultado
from compras.silver.rules import RAW, aplicar_regras, regras_da_entidade
from compras.silver.schemas import (
    ENTIDADES_SILVER,
    ORGAO_ATRIBUTOS,
    EntidadeSilver,
    Fonte,
    resolver,
)

log = logging.getLogger(__name__)

LINHAGEM = (
    "endpoint",
    "fonte_dados",
    "data_referencia",
    "arquivo_origem",
    "hash_registro",
    "payload",
    "data_ingestao",
)

COLUNAS_TECNICAS = StructType(
    [
        StructField("prioridade_origem", IntegerType()),
        StructField("ano_mes", IntegerType()),
        StructField("hash_linha", StringType()),
        StructField("arquivo_origem", StringType()),
        StructField("hash_registro", StringType()),
        StructField("run_id_ingestao_bronze", StringType()),
        StructField("ultima_alteracao_run_id", StringType()),
        StructField("data_processamento", TimestampType()),
    ]
)


@dataclass
class ResultadoSilver:
    entidade: str
    lidas: int = 0
    validas: int = 0
    quarentena: int = 0
    duplicadas: int = 0
    alteradas: int = 0
    resultados: list[Resultado] = field(default_factory=list)


# ------------------------------------------------------------------ schema da Silver
def tipo_spark(tipo: str) -> DataType:
    simples = {
        "date": DateType(),
        "timestamp": TimestampType(),
        "int": IntegerType(),
        "long": LongType(),
        "boolean": BooleanType(),
    }
    if tipo in simples:
        return simples[tipo]
    if tipo.startswith("decimal("):
        p, s = tipo[len("decimal(") : -1].split(",")
        return DecimalType(int(p), int(s))
    raise ValueError(f"Tipo desconhecido: {tipo}")


def schema_silver(ent: EntidadeSilver) -> StructType:
    tipos = {
        "string": StringType(),
        "texto": StringType(),
        "uasg": StringType(),
        "ni": StringType(),
    }
    campos = [StructField(c.nome, tipos.get(c.tipo) or tipo_spark(c.tipo)) for c in ent.colunas]
    if ent.tem_fornecedor:
        campos.append(StructField("tipo_pessoa", StringType()))
    if ent.nome == "orgaos":
        campos.extend(StructField(n, StringType()) for n in ORGAO_ATRIBUTOS if n != "codigo_orgao")
    return StructType(campos + COLUNAS_TECNICAS.fields)


def colunas_negocio(ent: EntidadeSilver) -> list[str]:
    extras = ["tipo_pessoa"] if ent.tem_fornecedor else []
    if ent.nome == "orgaos":
        extras += [n for n in ORGAO_ATRIBUTOS if n != "codigo_orgao"]
    return ent.nomes + extras


# ------------------------------------------------------------------ etapas
def checar_schema_origem(bronze: DataFrame, ent: EntidadeSilver, janela: str) -> list[Resultado]:
    """DQ-01: colunas de origem obrigatórias presentes e preenchidas em algum registro."""
    resultados = []
    por_endpoint = {r.endpoint: r["count"] for r in bronze.groupBy("endpoint").count().collect()}
    for endpoint, n in por_endpoint.items():
        if endpoint == "csv" or n == 0:
            continue
        obrigatorias = ent.obrigatorias_origem.get(endpoint)
        if obrigatorias is None:
            if endpoint not in ent.mapeamentos and not (
                ent.nome == "orgaos" and endpoint == "orgao"
            ):
                resultados.append(
                    Resultado(
                        "DQ-01",
                        ent.nome,
                        janela,
                        "bronze->silver",
                        n,
                        n,
                        f"endpoint {endpoint} sem mapeamento para a Silver",
                    )
                )
            continue
        fonte = Fonte(bronze.columns)
        ausentes = [c for c in obrigatorias if not fonte.tem(c)]
        presentes = [c for c in obrigatorias if fonte.tem(c)]
        vazias = []
        if presentes:
            sub = bronze.where(F.col("endpoint") == endpoint)
            contagem = sub.select(
                *[F.count(F.col(f"`{c}`")).alias(c) for c in presentes]
            ).collect()[0]
            vazias = [c for c in presentes if contagem[c] == 0]
        problemas = ausentes + vazias
        resultados.append(
            Resultado(
                "DQ-01",
                ent.nome,
                janela,
                "bronze->silver",
                n,
                n if problemas else 0,
                f"{endpoint}: colunas ausentes/vazias {problemas}" if problemas else endpoint,
            )
        )
    return resultados


def mapear(bronze: DataFrame, ent: EntidadeSilver) -> DataFrame:
    """Une os endpoints no schema canônico (ainda texto) + linhagem + prioridade."""
    fonte = Fonte(bronze.columns)
    partes = []
    for endpoint, mapa in ent.mapeamentos.items():
        sub = bronze.where(F.col("endpoint") == endpoint)
        selecao = [
            resolver(mapa[c], fonte).cast("string").alias(c)
            if c in mapa
            else F.lit(None).cast("string").alias(c)
            for c in ent.nomes
            if c != "sistema_origem"
        ]
        partes.append(
            sub.select(
                *selecao,
                F.coalesce(F.col("sistema_origem"), F.lit(endpoint)).alias("sistema_origem"),
                F.lit(ent.prioridade.get(endpoint, 9)).alias("prioridade_origem"),
                F.col("run_id_ingestao").alias("run_id_ingestao_bronze"),
                *LINHAGEM,
            )
        )
    out = partes[0]
    for p in partes[1:]:
        out = out.unionByName(p)
    return out


def tipar(df: DataFrame, ent: EntidadeSilver) -> DataFrame:
    """Guarda o texto original em ``_raw_*`` e converte cada coluna para o tipo canônico."""
    for c in ent.colunas:
        df = df.withColumn(RAW + c.nome, F.col(c.nome))
    if ent.tem_fornecedor:
        df = df.withColumn("tipo_pessoa", exprs.tipo_pessoa(F.col("ni_fornecedor")))
    for c in ent.colunas:
        df = df.withColumn(c.nome, exprs.converter(F.col(c.nome), c.tipo))
    if ent.nome == "itens_compra":
        # Completa unitário/total quando a fonte só traz um dos dois.
        df = (
            df.withColumn(
                "valor_total_estimado",
                F.coalesce(
                    "valor_total_estimado",
                    (F.col("quantidade") * F.col("valor_unitario_estimado")).cast("decimal(18,2)"),
                ),
            )
            .withColumn(
                "valor_unitario_estimado",
                F.coalesce(
                    "valor_unitario_estimado",
                    F.when(
                        F.col("quantidade") > 0,
                        (F.col("valor_total_estimado") / F.col("quantidade")).cast("decimal(18,4)"),
                    ),
                ),
            )
            .withColumn(
                "valor_total_homologado",
                F.coalesce(
                    "valor_total_homologado",
                    (F.col("quantidade_homologada") * F.col("valor_unitario_homologado")).cast(
                        "decimal(18,2)"
                    ),
                ),
            )
        )
    if ent.data_evento:
        evt = F.coalesce(F.col(ent.data_evento).cast("date"), F.col("data_referencia"))
        df = df.withColumn("ano_mes", (F.year(evt) * 100 + F.month(evt)).cast("int"))
    else:
        df = df.withColumn("ano_mes", F.lit(None).cast("int"))
    return df


def _chave_registro(ent: EntidadeSilver):
    partes = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in ent.chave]
    chave = F.concat_ws("|", *partes)
    todas_nulas = F.lit(True)
    for c in ent.chave:
        todas_nulas = todas_nulas & F.col(c).isNull()
    return F.when(todas_nulas, F.concat(F.lit("hash:"), F.col("hash_registro"))).otherwise(chave)


def gravar_quarentena(
    spark: SparkSession,
    settings: Settings,
    entidade: str,
    reprovados: DataFrame,
    camada: str,
    run_id: str,
) -> None:
    """MERGE por (chave_registro, hash_registro): repetir o run não duplica linhas."""
    linhas = reprovados.select(
        "chave_registro",
        F.transform("_violacoes", lambda v: v.regra).alias("regra_violada"),
        F.array_join(
            F.transform("_violacoes", lambda v: F.concat(v.regra, F.lit(": "), v.motivo)), "; "
        ).alias("motivo_rejeicao"),
        F.lit(camada).alias("camada_origem"),
        F.current_timestamp().alias("data_processamento"),
        F.lit(run_id).alias("run_id"),
        F.col("data_referencia").cast("date").alias("data_referencia"),
        "endpoint",
        "arquivo_origem",
        "hash_registro",
        "payload",
        F.lit(False).alias("resolvido"),
        F.lit(None).cast("timestamp").alias("data_resolucao"),
    ).dropDuplicates(["chave_registro", "hash_registro"])
    (
        DeltaTable.forName(spark, settings.quarentena(entidade))
        .alias("t")
        .merge(
            linhas.alias("s"),
            "t.chave_registro = s.chave_registro AND t.hash_registro = s.hash_registro "
            "AND t.camada_origem = s.camada_origem",
        )
        .whenMatchedUpdate(
            set={
                "regra_violada": "s.regra_violada",
                "motivo_rejeicao": "s.motivo_rejeicao",
                "data_processamento": "s.data_processamento",
                "run_id": "s.run_id",
                "resolvido": "false",
                "data_resolucao": "CAST(NULL AS TIMESTAMP)",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def resolver_quarentena(
    spark: SparkSession, settings: Settings, entidade: str, chaves_validas: DataFrame, camada: str
) -> None:
    """Registro que chegou corrigido sai da quarentena (``resolvido = true``)."""
    (
        DeltaTable.forName(spark, settings.quarentena(entidade))
        .alias("t")
        .merge(
            chaves_validas.select("chave_registro").distinct().alias("s"),
            f"t.chave_registro = s.chave_registro AND t.resolvido = false "
            f"AND t.camada_origem = '{camada}'",
        )
        .whenMatchedUpdate(set={"resolvido": "true", "data_resolucao": "current_timestamp()"})
        .execute()
    )


def materializar(spark: SparkSession, tabela: str, df: DataFrame) -> DataFrame:
    """Grava o lote avaliado e relê da tabela.

    Corta o plano lógico (as expressões de limpeza e as regras seriam reinseridas a cada
    filtro, levando o otimizador a estourar a memória) e evita recalcular o lote a cada
    contagem. Substitui ``cache()``, que não existe no serverless.
    """
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    return spark.table(tabela)


def deduplicar(df: DataFrame, ent: EntidadeSilver) -> DataFrame:
    w = Window.partitionBy(*ent.chave).orderBy(
        F.col("prioridade_origem").asc(),
        F.col("data_atualizacao").desc_nulls_last(),
        F.col("data_referencia").desc(),
        F.col("data_ingestao").desc(),
        F.col("hash_registro").asc(),  # desempate determinístico
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def merge_silver(
    spark: SparkSession, settings: Settings, ent: EntidadeSilver, lote: DataFrame, run_id: str
) -> int:
    tabela = settings.silver(ent.nome)
    schema = schema_silver(ent)
    particoes = ("ano_mes",) if ent.data_evento else ()
    garantir_tabela(
        spark,
        tabela,
        schema,
        particoes,
        comentario=f"Silver de {ent.nome}: tipada, deduplicada, upsert por chave natural",
    )
    negocio = colunas_negocio(ent)
    final = lote.withColumn("hash_linha", exprs.hash_colunas(*negocio)).select(
        *[
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in schema.fields
            if f.name not in ("ultima_alteracao_run_id", "data_processamento")
        ],
        F.lit(run_id).alias("ultima_alteracao_run_id"),
        F.current_timestamp().alias("data_processamento"),
    )
    cond_chave = " AND ".join(f"t.{c} <=> s.{c}" for c in ent.chave)
    # Atualiza só se mudou e se a nova versão "vence" a atual: fonte de maior prioridade,
    # ou mesma fonte com atualização igual ou mais recente.
    vence = (
        "t.hash_linha <> s.hash_linha AND ("
        "s.prioridade_origem < t.prioridade_origem OR "
        "(s.prioridade_origem = t.prioridade_origem AND "
        "coalesce(s.data_atualizacao, TIMESTAMP'1900-01-01') >= "
        "coalesce(t.data_atualizacao, TIMESTAMP'1900-01-01')))"
    )
    antes = spark.table(tabela).where(F.col("ultima_alteracao_run_id") == run_id).count()
    (
        DeltaTable.forName(spark, tabela)
        .alias("t")
        .merge(final.alias("s"), cond_chave)
        .whenMatchedUpdateAll(condition=vence)
        .whenNotMatchedInsertAll()
        .execute()
    )
    return spark.table(tabela).where(F.col("ultima_alteracao_run_id") == run_id).count() - antes


def checar_unicidade(spark: SparkSession, settings: Settings, ent: EntidadeSilver) -> Resultado:
    tabela = settings.silver(ent.nome)
    df = spark.table(tabela)
    total = df.count()
    distintas = df.select(*ent.chave).distinct().count()
    return Resultado(
        "DQ-09",
        ent.nome,
        "*",
        "silver",
        total,
        total - distintas,
        f"chave {ent.chave}",
    )


# ------------------------------------------------------------------ orquestração
_ID_CONTRATACAO_PNCP = r"^(\d{14})-\d+-(\d+)/(\d{4})$"


def _compra_pncp(id_contratacao: Column) -> Column:
    """``cnpj-1-seq/ano`` → ``cnpj-ano-seq``, o mesmo fan-out que a ingestão grava."""

    def parte(n: int) -> Column:
        return F.regexp_extract(id_contratacao, _ID_CONTRATACAO_PNCP, n)

    return F.when(
        id_contratacao.rlike(_ID_CONTRATACAO_PNCP),
        F.concat_ws("-", parte(1), parte(3), exprs.inteiro(parte(2)).cast("string")),
    )


def _completar_catalogo_pncp(bronze: DataFrame, mapeado: DataFrame) -> DataFrame:
    """Enriquecimento dentro da própria entidade: código do catálogo vindo da API do PNCP.

    O endpoint de itens do dadosabertos manda o código vazio desde mai/2026. O complemento
    ``pncp_itens_catalogo`` chega por compra (fan-out ``cnpj-ano-seq``) e casa com o item pelo
    número do item na compra. O código da fonte principal, quando existe, vence.
    """
    fonte = Fonte(bronze.columns)
    if not fonte.tem("catalogoCodigoItem"):
        return mapeado
    complemento = (
        bronze.where(F.col("endpoint") == "pncp_itens_catalogo")
        .select(
            F.col("fanout").alias("_compra"),
            exprs.inteiro(fonte.c("numeroItem")).alias("_numero"),
            exprs.texto(fonte.c("catalogoCodigoItem")).alias("_codigo"),
        )
        .where(F.col("_codigo").isNotNull())
    )
    ponte = (
        bronze.where(F.col("endpoint") == "pncp_itens")
        .select(
            fonte.c("idCompraItem").alias("id_compra_item"),
            _compra_pncp(fonte.c("idContratacaoPNCP")).alias("_compra"),
            exprs.inteiro(fonte.c("numeroItemPncp", "numeroItemCompra")).alias("_numero"),
        )
        .join(complemento, on=["_compra", "_numero"])
        .groupBy("id_compra_item")
        .agg(F.max("_codigo").alias("_codigo_pncp"))
    )
    return (
        mapeado.join(ponte, on="id_compra_item", how="left")
        .withColumn(
            "codigo_item_catalogo",
            F.when(
                F.col("endpoint") == "pncp_itens",
                F.coalesce(F.col("codigo_item_catalogo"), F.col("_codigo_pncp")),
            ).otherwise(F.col("codigo_item_catalogo")),
        )
        .drop("_codigo_pncp")
    )


def _combinar_orgaos(bronze: DataFrame, mapeado: DataFrame) -> DataFrame:
    """Enriquecimento dentro da própria entidade: atributos do órgão na UASG."""
    fonte = Fonte(bronze.columns)
    orgaos = bronze.where(F.col("endpoint") == "orgao").select(
        *[resolver(v, fonte).cast("string").alias(k) for k, v in ORGAO_ATRIBUTOS.items()],
        F.col("data_referencia").alias("_dr"),
    )
    w = Window.partitionBy("codigo_orgao").orderBy(F.col("_dr").desc())
    orgaos = (
        orgaos.where(F.col("codigo_orgao").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn", "_dr")
    )
    return mapeado.join(orgaos, on="codigo_orgao", how="left")


def processar_entidade(
    spark: SparkSession,
    settings: Settings,
    entidade: str,
    run_id: str,
    dq: RegistradorDQ,
) -> ResultadoSilver:
    ent = ENTIDADES_SILVER[entidade]
    res = ResultadoSilver(entidade)
    tabela_bronze = settings.bronze(entidade)
    if not tabela_existe(spark, tabela_bronze):
        log.warning("Bronze de %s inexistente; nada a fazer", entidade)
        return res
    bronze = spark.table(tabela_bronze).where(F.col("run_id") == run_id)
    janelas = sorted(
        r.data_referencia.isoformat() for r in bronze.select("data_referencia").distinct().collect()
    )
    rotulo_janela = ",".join(janelas[:5]) + ("…" if len(janelas) > 5 else "") or "-"
    res.lidas = bronze.count()
    if res.lidas == 0:
        log.info("Silver %s: nenhuma linha nova na Bronze neste run", entidade)
        return res

    # 1. DQ-01 (crítica para a entidade)
    r01 = checar_schema_origem(bronze, ent, rotulo_janela)
    for r in r01:
        dq.registrar(r)
    res.resultados.extend(r01)
    dq.falhar_se_critico(r01)

    # 2. mapeamento + tipos
    mapeado = mapear(bronze, ent)
    if entidade == "orgaos":
        mapeado = _combinar_orgaos(bronze, mapeado)
    if entidade == "itens_compra":
        mapeado = _completar_catalogo_pncp(bronze, mapeado)
    tipado = tipar(mapeado, ent).withColumn("chave_registro", _chave_registro(ent))

    # 3. regras linha a linha
    regras = regras_da_entidade(ent)
    avaliado = materializar(
        spark,
        settings.tabela("silver", f"_stg_{entidade}"),
        aplicar_regras(tipado, regras).drop(*[c for c in tipado.columns if c.startswith(RAW)]),
    )
    validos = avaliado.where(F.size("_violacoes") == 0)
    reprovados = avaliado.where(F.size("_violacoes") > 0)

    por_regra = {
        r.regra: r["count"]
        for r in reprovados.select(
            F.explode(F.array_distinct(F.transform("_violacoes", lambda v: v.regra))).alias("regra")
        )
        .groupBy("regra")
        .count()
        .collect()
    }
    total = avaliado.count()
    for regra_id in sorted({r.id for r in regras}):
        motivos = sorted({r.motivo for r in regras if r.id == regra_id})
        dq.registrar(
            Resultado(
                regra_id,
                entidade,
                rotulo_janela,
                "bronze->silver",
                total,
                por_regra.get(regra_id, 0),
                "; ".join(motivos)[:500],
            )
        )
    res.quarentena = reprovados.count()
    res.validas = total - res.quarentena
    if res.quarentena:
        gravar_quarentena(spark, settings, entidade, reprovados, "bronze->silver", run_id)

    # DQ-18 por janela
    por_janela = (
        avaliado.groupBy(F.col("data_referencia").cast("string").alias("j"))
        .agg(
            F.count("*").alias("n"),
            F.sum(F.when(F.size("_violacoes") > 0, 1).otherwise(0)).alias("q"),
        )
        .collect()
    )
    for r in por_janela:
        pct = (r.q / r.n) if r.n else 0.0
        dq.registrar(
            Resultado(
                "DQ-18",
                entidade,
                r.j,
                "bronze->silver",
                r.n,
                r.q if pct > settings.max_pct_rejeitados else 0,
                f"{pct:.2%} rejeitados (limite {settings.max_pct_rejeitados:.0%})",
            )
        )

    # 4. dedup + MERGE
    lote = deduplicar(validos, ent)
    res.duplicadas = res.validas - lote.count()
    res.alteradas = merge_silver(spark, settings, ent, lote, run_id)
    resolver_quarentena(spark, settings, entidade, validos, "bronze->silver")

    # 5. DQ-09 (crítica)
    r09 = dq.registrar(checar_unicidade(spark, settings, ent))
    res.resultados.append(r09)
    dq.falhar_se_critico([r09])

    log.info(
        "Silver %s: lidas=%s válidas=%s quarentena=%s duplicadas=%s alteradas=%s",
        entidade,
        res.lidas,
        res.validas,
        res.quarentena,
        res.duplicadas,
        res.alteradas,
    )
    return res


SCHEMA_RESUMO = StructType(
    [
        StructField("entidade", StringType()),
        StructField("lidas", LongType()),
        StructField("validas", LongType()),
        StructField("quarentena", LongType()),
        StructField("duplicadas", LongType()),
        StructField("alteradas", LongType()),
    ]
)
