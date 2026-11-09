"""fato_contratos — grão: 1 contrato.

Separada da ``fato_compras`` porque valor de item e valor de contrato não podem ser somados
na mesma tabela sem contar dinheiro duas vezes. Particionada pelo ``ano_mes`` do início da
vigência. Órgão e fornecedor ausentes viram sk = -1 (DQ-12 é aviso, não quarentena).
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from compras.common.delta import tabela_existe
from compras.config import Settings
from compras.gold.comum import anos_meses, gravar_staging, lookup, publicar_fato, sk_data
from compras.gold.dimensoes import chaves_modalidade_contratos
from compras.quality.reconciliation import checar_reconciliacao, checar_sks
from compras.quality.results import RegistradorDQ, Resultado

log = logging.getLogger(__name__)

NOME = "fato_contratos"
SKS = [
    "sk_data_inicio_vigencia",
    "sk_data_fim_vigencia",
    "sk_orgao",
    "sk_fornecedor",
    "sk_modalidade",
]


def construir_fato_contratos(
    spark: SparkSession,
    settings: Settings,
    run_id: str,
    dq: RegistradorDQ,
    completo: bool = False,
) -> dict:
    if not tabela_existe(spark, settings.silver("contratos")):
        log.warning("Silver de contratos ainda não existe; fato_contratos não construída")
        return {}
    contratos = spark.table(settings.silver("contratos"))

    if completo:
        afetadas = anos_meses(contratos)
    else:
        mudou = contratos.where(F.col("ultima_alteracao_run_id") == run_id)
        afetadas_set = set(anos_meses(mudou))
        alvo = settings.gold(NOME)
        if tabela_existe(spark, alvo):
            antigas = spark.table(alvo).join(
                mudou.select("id_contrato"), "id_contrato", "left_semi"
            )
            afetadas_set |= set(anos_meses(antigas))
        afetadas = sorted(afetadas_set)
    if not afetadas:
        log.info("fato_contratos: nenhuma partição afetada neste run")
        return {"particoes": []}

    base = contratos.where(F.col("ano_mes").isin(afetadas))
    mods = chaves_modalidade_contratos(spark, settings).select(
        F.col("id_contrato").alias("_m_id"),
        F.col("modalidade_chave").alias("_mod_chave"),
        F.col("regime_legal").alias("_m_regime"),
    )
    fato = base.join(mods, F.col("id_contrato") == F.col("_m_id"), "left")
    fato = lookup(
        fato,
        spark.table(settings.gold("dim_orgao")),
        [("codigo_uasg", "codigo_uasg")],
        "sk_orgao",
        "sk_orgao",
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
    data_forn = F.coalesce(F.col("data_vigencia_inicio"), F.current_date())
    fato = fato.join(
        forn,
        (F.col("ni_fornecedor") == F.col("_f_ni"))
        & data_forn.between(F.col("_f_ini"), F.col("_f_fim")),
        "left",
    ).withColumn("sk_fornecedor", F.coalesce(F.col("_f_sk"), F.lit(-1)).cast("long"))

    fato = fato.select(
        sk_data(F.col("data_vigencia_inicio")).alias("sk_data_inicio_vigencia"),
        sk_data(F.col("data_vigencia_fim")).alias("sk_data_fim_vigencia"),
        "sk_orgao",
        "sk_fornecedor",
        "sk_modalidade",
        "id_contrato",
        "numero_contrato",
        "id_compra",
        F.coalesce("_m_regime", "regime_legal").alias("regime_legal"),
        "tipo_contrato",
        "categoria",
        "excluido",
        "valor_global",
        "valor_parcela",
        "numero_parcelas",
        "valor_acumulado",
        F.datediff("data_vigencia_fim", "data_vigencia_inicio").alias("prazo_dias"),
        "arquivo_origem",
        F.lit(run_id).alias("run_id"),
        "ano_mes",
    )

    stg = gravar_staging(spark, settings, NOME, fato)
    janela = f"{afetadas[0]}..{afetadas[-1]}"
    vazio = base.limit(0)
    resultados = [checar_sks(stg, NOME, SKS, janela)]
    resultados += checar_reconciliacao(
        base, vazio, stg, NOME, "valor_global", "valor_global", settings.tolerancia_valor
    )
    total = stg.count()
    sem_forn = stg.where(F.col("sk_fornecedor") == -1).count()
    resultados.append(
        Resultado(
            "DQ-12",
            "contratos",
            janela,
            "silver->gold",
            total,
            sem_forn,
            "contratos sem fornecedor na dim_fornecedor",
        )
    )
    for sk in ("sk_orgao", "sk_modalidade"):
        n = stg.where(F.col(sk) == -1).count()
        resultados.append(Resultado("DIM-DESCONHECIDO", NOME, janela, "gold", total, n, sk))
    for r in resultados:
        dq.registrar(r)
    dq.falhar_se_critico(resultados)

    publicar_fato(spark, settings, NOME, stg, afetadas)
    return {"particoes": afetadas, "linhas": total}
