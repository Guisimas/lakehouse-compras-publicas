"""Carga histórica a partir dos CSVs do Portal de Dados Abertos.

Os CSVs são copiados sem alteração para o landing, na janela anual correspondente::

    <root>/<entidade>/data_referencia=<YYYY>/csv__<nome_do_arquivo>.csv

A Bronze lê esses arquivos com ``fonte_dados = 'csv'``. O mapeamento de colunas do CSV
para o schema canônico fica na Silver (``compras.silver.schemas``), que aceita aliases. Esse
caminho não foi validado contra os CSVs reais; a carga histórica usa a API.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import httpx

from compras.common.runtime import agora_utc
from compras.ingestion.control import Controle, RegistroLog
from compras.ingestion.landing import dir_janela
from compras.ingestion.storage import Storage

log = logging.getLogger(__name__)
_SEGURO = re.compile(r"[^0-9A-Za-z_.-]")


def ingerir_csv(
    *,
    origem: str,
    entidade: str,
    ano: int,
    storage: Storage,
    root: str,
    controle: Controle,
    run_id: str,
    endpoint: str = "csv",
) -> RegistroLog:
    """Copia um CSV (caminho local ou URL) para o landing e registra no log."""
    reg = RegistroLog(
        run_id=run_id,
        entidade=entidade,
        janela=str(ano),
        fonte_dados="csv",
        modo="historico",
        inicio=agora_utc().isoformat(),
        endpoints=1,
        fanouts=1,
    )
    t0 = time.monotonic()
    try:
        if origem.startswith(("http://", "https://")):
            with httpx.Client(timeout=300, follow_redirects=True) as http:
                resp = http.get(origem)
                resp.raise_for_status()
                dados = resp.content
            nome = origem.rsplit("/", 1)[-1].split("?", 1)[0]
        else:
            dados = Path(origem).read_bytes()
            nome = Path(origem).name
        nome = _SEGURO.sub("_", nome)
        if not nome.lower().endswith(".csv"):
            nome += ".csv"
        destino = f"{dir_janela(root, entidade, str(ano))}/{endpoint}__{nome}"
        storage.write_bytes(destino, dados)
        reg.paginas = 1
        reg.linhas = max(dados.count(b"\n") - 1, 0)
        reg.status = "sucesso"
        reg.detalhes = [{"arquivo": destino, "bytes": len(dados)}]
    except Exception as exc:
        reg.status = "falha"
        reg.erro = f"{type(exc).__name__}: {exc}"
        log.exception("Falha no backfill CSV de %s", origem)
    finally:
        reg.fim = agora_utc().isoformat()
        reg.duracao_s = round(time.monotonic() - t0, 3)
        controle.gravar_log(reg)
    return reg
