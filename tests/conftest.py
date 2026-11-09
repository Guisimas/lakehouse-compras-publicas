from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))  # permite "from fixtures import api"


def _tem_java() -> bool:
    return bool(os.environ.get("JAVA_HOME") or shutil.which("java"))


def pytest_collection_modifyitems(config, items):
    try:
        import delta  # noqa: F401
        import pyspark  # noqa: F401

        motivo = None if _tem_java() else "Java 17 não encontrado"
    except ImportError:
        motivo = "pyspark/delta-spark não instalados (uv sync --extra spark)"
    if motivo:
        pular = pytest.mark.skip(reason=motivo)
        for item in items:
            if "spark" in item.keywords:
                item.add_marker(pular)
