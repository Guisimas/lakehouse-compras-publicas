from __future__ import annotations

import os
import sys
import uuid

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("compras-testes")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        # Ajustes para dados minúsculos em uma máquina só: o padrão (50 partições para
        # reconstruir o log Delta, 1 GB de heap) deixa a suíte lenta e, acumulando
        # tabelas na mesma sessão, a JVM entra em GC contínuo.
        .config("spark.driver.memory", "3g")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
    )
    s = configure_spark_with_delta_pip(builder).getOrCreate()
    yield s
    s.stop()


@pytest.fixture
def ambiente(spark, tmp_path, monkeypatch):
    """Catálogo isolado por teste (prefixo único nos schemas) + landing temporário."""
    from compras.common.tabelas import SCHEMAS_UC

    sufixo = uuid.uuid4().hex[:6]
    root = str(tmp_path / "landing").replace("\\", "/")
    settings = _settings_teste(sufixo, root)
    for schema in SCHEMAS_UC:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}_{sufixo}")
    monkeypatch.setenv("COMPRAS_LANDING_ROOT", root)
    return settings


def _settings_teste(sufixo: str, root: str):
    """Settings com schemas sufixados (``bronze_ab12cd``) no catálogo de sessão local."""
    from compras.config import Settings

    class S(Settings):
        def tabela(self, schema: str, nome: str) -> str:
            # Delta OSS só aceita nomes de 2 partes; no Unity Catalog usamos 3 partes
            return f"{schema}_{sufixo}.{nome}"

    return S(catalog="spark_catalog", landing_root=root)


os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
