import textwrap
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]


def _sem_comentarios(sql: str) -> str:
    return "\n".join(ln for ln in sql.splitlines() if not ln.startswith("--")).strip()


def test_alerta_do_bundle_usa_a_consulta_versionada():
    yml = (RAIZ / "resources" / "compras.alert.yml").read_text(encoding="utf-8")
    bloco = yml.split("query_text: |\n", 1)[1].split("\n      evaluation:", 1)[0]
    sql = (RAIZ / "sql" / "alerta_qualidade.sql").read_text(encoding="utf-8")
    assert textwrap.dedent(bloco).strip() == _sem_comentarios(sql)
    assert "name: problemas_novos" in yml
