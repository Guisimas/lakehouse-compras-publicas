"""Landing → Bronze.

- Uma tabela por entidade, dado como recebido: colunas originais (todas string) + ``payload``.
- Colunas de controle: ``data_ingestao``, ``arquivo_origem``, ``fonte_dados``,
  ``data_referencia``, ``run_id``, ``hash_registro`` (+ ``endpoint``, ``fanout``…).
- Particionada por ``data_referencia``; recarga da mesma janela usa ``replaceWhere``
  (data_referencia + endpoints presentes), então rodar duas vezes dá o mesmo resultado.
- Schema evolution ligado; toda coluna nova gera aviso no log e em ``dq_resultados``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from compras.common.delta import colunas, sobrescrever_particao, sql_lista
from compras.config import Settings
from compras.ingestion.landing import PREFIXO_SUCESSO, dir_janela, janela_de_chave
from compras.ingestion.storage import Storage

log = logging.getLogger(__name__)

COLUNAS_CONTROLE = (
    "data_ingestao",
    "arquivo_origem",
    "fonte_dados",
    "data_referencia",
    "tipo_janela",
    "run_id",
    "run_id_ingestao",
    "endpoint",
    "fanout",
    "sistema_origem",
    "regime_legal_origem",
    "hash_registro",
    "payload",
)

_META = (
    "struct<endpoint:string,fanout:string,pagina:int,regime_legal:string,"
    "sistema_origem:string,tipo_janela:string,data_referencia:string,run_id:string>"
)


@dataclass
class ResultadoBronze:
    entidade: str
    chave: str
    linhas: int = 0
    arquivos: int = 0
    endpoints: list[str] = field(default_factory=list)
    colunas_novas: list[str] = field(default_factory=list)


def _basename(p: str) -> str:
    return p.rstrip("/").rsplit("/", 1)[-1]


def _ler_json(spark: SparkSession, arquivos: list[str]) -> DataFrame:
    bruto = spark.read.text(arquivos, wholetext=True).select(
        F.col("value"), F.col("_metadata.file_path").alias("arquivo_origem")
    )
    env = bruto.select(
        "arquivo_origem",
        F.from_json("value", f"struct<meta:{_META}>").getField("meta").alias("meta"),
        # payload como texto bruto: objetos viram o JSON original
        F.get_json_object("value", "$.payload").alias("resposta"),
    )
    resultado = F.get_json_object("resposta", "$.resultado")
    registros_json = (
        F.when(F.ltrim("resposta").startswith("["), F.col("resposta"))
        .when(F.ltrim(resultado).startswith("{"), F.concat(F.lit("["), resultado, F.lit("]")))
        .otherwise(resultado)
    )
    return env.select(
        "arquivo_origem",
        F.lit("api").alias("fonte_dados"),
        F.col("meta.endpoint").alias("endpoint"),
        F.col("meta.fanout").alias("fanout"),
        F.col("meta.sistema_origem").alias("sistema_origem"),
        F.col("meta.regime_legal").alias("regime_legal_origem"),
        F.col("meta.tipo_janela").alias("tipo_janela"),
        F.col("meta.run_id").alias("run_id_ingestao"),
        F.explode_outer(F.from_json(registros_json, "array<string>")).alias("payload"),
    ).where(F.col("payload").isNotNull())


def _detectar_csv(storage: Storage, arquivo: str) -> tuple[str, str]:
    amostra = storage.read_bytes(arquivo)[:65536]
    try:
        primeira = amostra.decode("utf-8").splitlines()[0]
        encoding = "UTF-8"
    except UnicodeDecodeError:
        primeira = amostra.decode("latin-1").splitlines()[0]
        encoding = "ISO-8859-1"
    sep = ";" if primeira.count(";") > primeira.count(",") else ","
    return sep, encoding


def _ler_csv(spark: SparkSession, storage: Storage, arquivos: list[str]) -> DataFrame | None:
    partes = []
    for arq in arquivos:
        sep, encoding = _detectar_csv(storage, arq)
        df = (
            spark.read.option("header", True)
            .option("sep", sep)
            .option("encoding", encoding)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(arq)
        )
        endpoint = _basename(arq).split("__", 1)[0]
        partes.append(
            df.select(
                F.col("_metadata.file_path").alias("arquivo_origem"),
                F.lit("csv").alias("fonte_dados"),
                F.lit(endpoint).alias("endpoint"),
                F.lit("csv").alias("fanout"),
                F.lit("csv").alias("sistema_origem"),
                F.lit(None).cast("string").alias("regime_legal_origem"),
                F.lit("ano").alias("tipo_janela"),
                F.lit(None).cast("string").alias("run_id_ingestao"),
                F.to_json(F.struct(*[F.col(f"`{c}`") for c in df.columns])).alias("payload"),
            )
        )
    if not partes:
        return None
    out = partes[0]
    for p in partes[1:]:
        out = out.unionByName(p)
    return out


def carregar_janela(
    spark: SparkSession,
    settings: Settings,
    storage: Storage,
    entidade: str,
    chave: str,
    run_id: str,
) -> ResultadoBronze:
    res = ResultadoBronze(entidade, chave)
    diretorio = dir_janela(settings.landing, entidade, chave)
    arquivos = [f for f in storage.list_files(diretorio) if not _basename(f).startswith("_")]
    jsons = [f for f in arquivos if f.endswith(".json")]
    csvs = [f for f in arquivos if f.lower().endswith(".csv")]
    sucessos = [
        _basename(f)[len(PREFIXO_SUCESSO) :].split("__", 1)[0]
        for f in storage.list_files(diretorio)
        if _basename(f).startswith(PREFIXO_SUCESSO)
    ]
    res.arquivos = len(arquivos)
    res.endpoints = sorted({_basename(f).split("__", 1)[0] for f in arquivos} | set(sucessos))
    if not arquivos:
        log.warning("Nada no landing para %s/%s", entidade, chave)
        return res

    partes = []
    if jsons:
        partes.append(_ler_json(spark, jsons))
    if csvs:
        df_csv = _ler_csv(spark, storage, csvs)
        if df_csv is not None:
            partes.append(df_csv)
    base = partes[0]
    for p in partes[1:]:
        base = base.unionByName(p)

    base = base.withColumn("_m", F.from_json("payload", "map<string,string>"))
    chaves = sorted(r[0] for r in base.select(F.explode(F.map_keys("_m"))).distinct().collect())
    # Nomes que só diferem por maiúsculas colidem no Spark: mantém o primeiro e avisa.
    vistos: dict[str, str] = {}
    for k in chaves:
        if k.lower() in vistos or k.lower() in COLUNAS_CONTROLE:
            log.warning(
                "Coluna %s ignorada (colide com %s); segue no payload", k, vistos.get(k.lower())
            )
            continue
        vistos[k.lower()] = k
    originais = list(vistos.values())

    data_ref = janela_de_chave(chave)
    df = base.select(
        *[F.col("_m").getItem(k).alias(k) for k in originais],
        F.current_timestamp().alias("data_ingestao"),
        "arquivo_origem",
        "fonte_dados",
        F.lit(data_ref).alias("data_referencia"),
        "tipo_janela",
        F.lit(run_id).alias("run_id"),
        "run_id_ingestao",
        "endpoint",
        "fanout",
        "sistema_origem",
        "regime_legal_origem",
        F.sha2("payload", 256).alias("hash_registro"),
        "payload",
    )

    tabela = settings.bronze(entidade)
    existentes = {c.lower() for c in colunas(spark, tabela)}
    if existentes:
        res.colunas_novas = [c for c in originais if c.lower() not in existentes]
        for c in res.colunas_novas:
            log.warning("SCHEMA: coluna nova em %s: %s", tabela, c)

    predicado = (
        f"data_referencia = DATE'{data_ref.isoformat()}' "
        f"AND endpoint IN ({sql_lista(res.endpoints)})"
    )
    sobrescrever_particao(df, tabela, predicado, particoes=("data_referencia",), merge_schema=True)
    res.linhas = (
        spark.table(tabela)
        .where((F.col("data_referencia") == F.lit(data_ref)) & (F.col("run_id") == run_id))
        .count()
    )
    log.info("Bronze %s/%s: %s linhas de %s arquivos", entidade, chave, res.linhas, res.arquivos)
    return res


def chaves_carregadas(spark: SparkSession, settings: Settings, entidade: str) -> set[str]:
    tabela = settings.bronze(entidade)
    if not colunas(spark, tabela):
        return set()
    rows = spark.table(tabela).select("data_referencia", "tipo_janela").distinct().collect()
    return {
        str(r.data_referencia.year) if r.tipo_janela == "ano" else r.data_referencia.isoformat()
        for r in rows
    }
