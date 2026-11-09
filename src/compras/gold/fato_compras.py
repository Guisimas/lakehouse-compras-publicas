"""fato_compras — grão: 1 item de 1 licitação ou compra direta.

Montagem incremental por partição ``ano_mes`` do evento (data de publicação da compra):

1. Partições afetadas = as dos itens alterados neste run, as dos itens cujas compras foram
   alteradas e as partições onde esses itens estavam antes (se a data do evento mudou).
2. Base = todos os itens da Silver nessas partições, com os atributos da compra.
3. DQ-11 (item sem compra válida) e DQ-10 (compra sem órgão válido) → quarentena.
4. Lookup das dimensões (sk = -1 quando falta referência opcional).
5. Staging → DQ-13/14/15 → ``replaceWhere`` nas partições afetadas.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from compras.common.delta import tabela_existe
from compras.config import Settings
from compras.gold.comum import (
    anos_meses,
    chave_modalidade,
    gravar_staging,
    lookup,
    publicar_fato,
    sk_data,
)
from compras.quality.reconciliation import checar_reconciliacao, checar_sks
from compras.quality.results import RegistradorDQ, Resultado
from compras.silver.transform import gravar_quarentena, resolver_quarentena

log = logging.getLogger(__name__)

NOME = "fato_compras"
SKS = [
    "sk_data_publicacao",
    "sk_data_resultado",
    "sk_orgao",
    "sk_fornecedor",
    "sk_item",
    "sk_modalidade",
]


def _itens_com_compra(spark: SparkSession, settings: Settings) -> DataFrame:
    itens = spark.table(settings.silver("itens_compra")).alias("i")
    lic = spark.table(settings.silver("licitacoes")).select(
        F.col("id_compra").alias("l_id_compra"),
        F.col("regime_legal").alias("l_regime_legal"),
        F.col("codigo_uasg").alias("l_codigo_uasg"),
        F.col("codigo_modalidade").alias("l_codigo_modalidade"),
        F.col("data_publicacao").alias("l_data_publicacao"),
        F.col("excluido").alias("l_excluido"),
        F.col("ultima_alteracao_run_id").alias("l_run_id"),
    )
    j = itens.join(lic, F.col("i.id_compra") == F.col("l_id_compra"), "left")
    data_evento = F.coalesce(
        F.col("l_data_publicacao"), F.col("data_item"), F.col("data_atualizacao").cast("date")
    )
    return j.select(
        "i.*",
        "l_id_compra",
        "l_run_id",
        F.coalesce("l_regime_legal", "regime_legal").alias("regime_legal_compra"),
        F.coalesce("l_codigo_uasg", "codigo_uasg").alias("uasg_compra"),
        F.coalesce("l_codigo_modalidade", "codigo_modalidade").alias("modalidade_compra"),
        F.coalesce("l_excluido", F.lit(False)).alias("compra_excluida"),
        data_evento.alias("data_evento"),
    ).withColumn("ano_mes", (F.year("data_evento") * 100 + F.month("data_evento")).cast("int"))


def construir_fato_compras(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    dq: RegistradorDQ,
    completo: bool = False,
) -> dict:
    if not (
        tabela_existe(spark, settings.silver("itens_compra"))
        and tabela_existe(spark, settings.silver("licitacoes"))
    ):
        log.warning("Silver de itens/licitações ainda não existe; fato_compras não construída")
        return {}
    todos = _itens_com_compra(spark, settings)

    # 1. partições afetadas
    if completo:
        afetadas = anos_meses(todos)
    else:
        mudou = todos.where(
            (F.col("ultima_alteracao_run_id") == run_id) | (F.col("l_run_id") == run_id)
        )
        afetadas_set = set(anos_meses(mudou))
        alvo = settings.gold(NOME)
        if tabela_existe(spark, alvo):
            antigas = (
                spark.table(alvo)
                .join(mudou.select("id_compra_item"), "id_compra_item", "left_semi")
                .select("ano_mes")
            )
            afetadas_set |= set(anos_meses(antigas))
        afetadas = sorted(afetadas_set)
    if not afetadas:
        log.info("fato_compras: nenhuma partição afetada neste run")
        return {"particoes": []}

    base = todos.where(F.col("ano_mes").isin(afetadas))

    # 3. consistência (DQ-11 e DQ-10)
    orgaos = (
        spark.table(settings.gold("dim_orgao"))
        .select(F.col("codigo_uasg").alias("_o_uasg"), F.col("sk_orgao"))
        .where(F.col("sk_orgao") != -1)
    )
    com_orgao = base.join(orgaos, F.col("uasg_compra") == F.col("_o_uasg"), "left")
    sem_compra = F.col("l_id_compra").isNull()
    sem_orgao = F.col("sk_orgao").isNull()
    violacoes = F.filter(
        F.array(
            F.when(
                sem_compra,
                F.struct(
                    F.lit("DQ-11").alias("regra"),
                    F.lit("item sem compra válida na Silver").alias("motivo"),
                ),
            ),
            F.when(
                sem_orgao,
                F.struct(
                    F.lit("DQ-10").alias("regra"),
                    F.lit("compra sem órgão (UASG) válido na dim_orgao").alias("motivo"),
                ),
            ),
        ),
        lambda x: x.isNotNull(),
    )
    avaliado = com_orgao.withColumn("_violacoes", violacoes)
    rejeitadas = avaliado.where(F.size("_violacoes") > 0)
    validas = avaliado.where(F.size("_violacoes") == 0)

    # 4. dimensões
    fato = validas.withColumn("_item_chave", F.concat_ws(":", "tipo_item", "codigo_item_catalogo"))
    fato = lookup(
        fato,
        spark.table(settings.gold("dim_item")),
        [("_item_chave", "item_chave")],
        "sk_item",
        "sk_item",
    )
    fato = fato.withColumn(
        "_mod_chave", chave_modalidade(F.col("regime_legal_compra"), F.col("modalidade_compra"))
    )
    fato = lookup(
        fato,
        spark.table(settings.gold("dim_modalidade")),
        [("_mod_chave", "modalidade_chave")],
        "sk_modalidade",
        "sk_modalidade",
    )
    forn = spark.table(settings.gold("dim_fornecedor")).select(
        F.col("ni_fornecedor").alias("_f_ni"),
        F.col("data_inicio_vigencia").alias("_f_ini"),
        F.col("data_fim_vigencia").alias("_f_fim"),
        F.col("sk_fornecedor").alias("_f_sk"),
    )
    data_forn = F.coalesce(F.col("data_resultado"), F.col("data_evento"), F.current_date())
    fato = fato.join(
        forn,
        (F.col("ni_fornecedor") == F.col("_f_ni"))
        & data_forn.between(F.col("_f_ini"), F.col("_f_fim")),
        "left",
    ).withColumn("sk_fornecedor", F.coalesce(F.col("_f_sk"), F.lit(-1)).cast("long"))

    fato = fato.select(
        sk_data(F.col("data_evento")).alias("sk_data_publicacao"),
        sk_data(F.col("data_resultado")).alias("sk_data_resultado"),
        F.col("sk_orgao").cast("long").alias("sk_orgao"),
        "sk_fornecedor",
        "sk_item",
        "sk_modalidade",
        "id_compra_item",
        "id_compra",
        "numero_item",
        F.col("regime_legal_compra").alias("regime_legal"),
        "sistema_origem",
        "compra_excluida",
        F.coalesce("quantidade_homologada", "quantidade").alias("quantidade"),
        F.coalesce("valor_unitario_homologado", "valor_unitario_estimado").alias("valor_unitario"),
        F.coalesce("valor_total_homologado", "valor_total_estimado").alias("valor_total_item"),
        F.col("valor_total_estimado").alias("valor_estimado"),
        F.col("valor_total_homologado").alias("valor_homologado"),
        F.col("valor_total_homologado").isNull().alias("valor_eh_estimado"),
        "arquivo_origem",
        F.lit(run_id).alias("run_id"),
        "ano_mes",
    )

    # 5. staging + checagens críticas antes de publicar
    stg = gravar_staging(spark, settings, NOME, fato)
    janela = f"{afetadas[0]}..{afetadas[-1]}"
    valor_base = "_valor_base"
    base_v = base.withColumn(
        valor_base, F.coalesce("valor_total_homologado", "valor_total_estimado")
    )
    rej_v = rejeitadas.withColumn(
        valor_base, F.coalesce("valor_total_homologado", "valor_total_estimado")
    )
    resultados = [checar_sks(stg, NOME, SKS, janela)]
    resultados += checar_reconciliacao(
        base_v, rej_v, stg, NOME, valor_base, "valor_total_item", settings.tolerancia_valor
    )
    n_rej = {
        r.regra: r["count"]
        for r in rejeitadas.select(
            F.explode(F.transform("_violacoes", lambda v: v.regra)).alias("regra")
        )
        .groupBy("regra")
        .count()
        .collect()
    }
    n_base = base.count()
    for regra in ("DQ-10", "DQ-11"):
        resultados.append(
            Resultado(regra, "itens_compra", janela, "silver->gold", n_base, n_rej.get(regra, 0))
        )
    for sk in ("sk_fornecedor", "sk_item", "sk_modalidade"):
        n = stg.where(F.col(sk) == -1).count()
        resultados.append(Resultado("DIM-DESCONHECIDO", NOME, janela, "gold", stg.count(), n, sk))
    for r in resultados:
        dq.registrar(r)
    dq.falhar_se_critico(resultados)  # levanta FalhaCritica: nada é publicado

    if rejeitadas.limit(1).count():
        q = (
            rejeitadas.withColumn("chave_registro", F.col("id_compra_item"))
            .withColumn("payload", F.to_json(F.struct(*[c for c in base.columns])))
            .withColumn("endpoint", F.col("sistema_origem"))
            .withColumn("data_referencia", F.col("data_evento"))
        )
        gravar_quarentena(spark, settings, "itens_compra", q, "silver->gold", run_id)
    resolver_quarentena(
        spark,
        settings,
        "itens_compra",
        stg.select(F.col("id_compra_item").alias("chave_registro")),
        "silver->gold",
    )
    publicar_fato(spark, settings, NOME, stg, afetadas)
    return {"particoes": afetadas, "linhas": stg.count(), "rejeitadas": sum(n_rej.values())}
