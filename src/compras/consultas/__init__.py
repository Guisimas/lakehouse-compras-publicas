"""Consultas de negócio, empacotadas para o benchmark rodar no Databricks."""

from __future__ import annotations

from importlib import resources


def consultas_negocio() -> dict[str, str]:
    """As consultas q1..q7, na ordem, como texto SQL."""
    pasta = resources.files(__name__)
    return {
        p.name.removesuffix(".sql"): p.read_text(encoding="utf-8")
        for p in sorted(pasta.iterdir(), key=lambda x: x.name)
        if p.name.startswith("q") and p.name.endswith(".sql")
    }


def sem_comentarios(sql: str) -> str:
    linhas = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    return "\n".join(linhas).strip().rstrip(";")
