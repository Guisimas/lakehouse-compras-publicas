"""Utilidades de execução: run_id, relógio e janelas de data."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

FUSO = ZoneInfo("America/Sao_Paulo")


def novo_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]


def agora_utc() -> datetime:
    return datetime.now(UTC)


def hoje_brt() -> date:
    return datetime.now(FUSO).date()


def ontem_brt() -> date:
    return hoje_brt() - timedelta(days=1)


def em_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def dias(inicio: date, fim: date) -> Iterator[date]:
    """Itera de ``inicio`` até ``fim`` (inclusive)."""
    atual = inicio
    while atual <= fim:
        yield atual
        atual += timedelta(days=1)


def parse_data(valor: str | date) -> date:
    return valor if isinstance(valor, date) else date.fromisoformat(valor)


def planejar_janelas(
    *,
    data_inicio: date | None,
    data_fim: date | None,
    ultima_janela_ok: date | None,
    dias_reprocessar: Iterable[int] = (),
    max_dias_recuperacao: int = 31,
    referencia: date | None = None,
) -> list[date]:
    """Calcula as janelas diárias que um run deve processar.

    - Com ``data_inicio``/``data_fim`` explícitos (backfill), devolve exatamente o intervalo.
    - Sem eles, processa de ``ultima_janela_ok + 1`` até ontem (recuperando falhas, no máximo
      ``max_dias_recuperacao`` dias) e acrescenta janelas antigas a reprocessar (14, 30… dias antes)
      para capturar dados que a fonte publica com atraso.
    """
    fim = data_fim or (referencia or ontem_brt())
    if data_inicio is not None:
        return list(dias(data_inicio, fim))
    inicio = (ultima_janela_ok + timedelta(days=1)) if ultima_janela_ok else fim
    inicio = max(inicio, fim - timedelta(days=max_dias_recuperacao - 1))
    janelas = set(dias(inicio, fim)) if inicio <= fim else {fim}
    janelas.update(fim - timedelta(days=d) for d in dias_reprocessar if d > 0)
    return sorted(janelas)
