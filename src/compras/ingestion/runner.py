"""Execução da ingestão de várias entidades e janelas, compartilhada por CLI e task do job."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, timedelta

from compras.config import Settings
from compras.ingestion.client import ComprasClient
from compras.ingestion.control import Controle, RegistroLog
from compras.ingestion.endpoints import (
    ORDEM_ENTIDADES,
    Modo,
    dias_varredura,
    eh_snapshot,
    eh_varredura,
)
from compras.ingestion.ingestor import Ingestor
from compras.ingestion.landing import Landing, LandingFanout
from compras.ingestion.storage import Storage

log = logging.getLogger(__name__)


def executar_ingestao(
    *,
    settings: Settings,
    storage: Storage,
    entidades: Sequence[str],
    janelas: Sequence[date],
    hoje: date,
    run_id: str,
    modo: Modo = "incremental",
    forcar: bool = False,
    forcar_snapshot: bool = False,
    client: ComprasClient | None = None,
) -> list[RegistroLog]:
    landing = Landing(storage, settings.landing)
    controle = Controle(storage, settings.landing)
    proprio_client = client is None
    client = client or ComprasClient(
        settings.api_base_url,
        tamanho_pagina=settings.tamanho_pagina,
        req_por_segundo=settings.req_por_segundo,
        max_tentativas=settings.max_tentativas,
        timeout_s=settings.timeout_s,
    )
    # Fornecedores da varredura de contratos: só os de contratos novos (ver LandingFanout).
    fanout = LandingFanout(landing, contratos_incrementais=modo != "historico")
    ingestor = Ingestor(client, landing, controle, run_id, fanout)
    # Janelas antigas do run incremental (as de reprocessamento) são baixadas de novo.
    limite_refazer = (
        max(janelas) - timedelta(days=settings.dias_refazer_janela)
        if modo == "incremental" and janelas
        else None
    )
    resultados: list[RegistroLog] = []
    try:
        for entidade in [e for e in ORDEM_ENTIDADES if e in entidades]:
            if eh_snapshot(entidade):
                if forcar_snapshot or ingestor.snapshot_vencido(entidade, hoje):
                    resultados.append(
                        ingestor.ingerir(entidade, hoje, modo=modo, forcar=forcar_snapshot)
                    )
                else:
                    log.info("Snapshot de %s ainda válido; pulando", entidade)
                continue
            if eh_varredura(entidade):
                for janela in janelas_de_varredura(
                    ingestor, entidade, janelas, modo, forcar_snapshot
                ):
                    resultados.append(ingestor.ingerir(entidade, janela, modo=modo, forcar=forcar))
                continue
            for janela in janelas:
                refazer = limite_refazer is not None and janela <= limite_refazer
                resultados.append(
                    ingestor.ingerir(entidade, janela, modo=modo, forcar=forcar, refazer=refazer)
                )
    finally:
        if proprio_client:
            client.close()
    return resultados


def janelas_de_varredura(
    ingestor: Ingestor,
    entidade: str,
    janelas: Sequence[date],
    modo: Modo,
    forcar: bool = False,
) -> list[date]:
    """Datas de referência das varreduras a executar neste run.

    - Diário: uma varredura terminando na janela mais recente, se a última tiver
      ``refresh_dias`` ou mais (ou se ``forcar``). Nos outros dias, nada.
    - Histórico: blocos de ``dias_varredura`` cobrindo todo o intervalo pedido.
    """
    if not janelas:
        return []
    inicio, fim = min(janelas), max(janelas)
    if modo == "historico":
        passo = timedelta(days=dias_varredura(entidade))
        refs, ref = [], fim
        while ref >= inicio:
            refs.append(ref)
            ref -= passo
        return sorted(refs)
    if forcar or ingestor.snapshot_vencido(entidade, fim):
        return [fim]
    log.info("Varredura de %s ainda válida; pulando", entidade)
    return []


def resumo(resultados: Sequence[RegistroLog]) -> str:
    linhas = [
        f"{r.entidade:<13} {r.janela:<10} {r.status:<8} pág={r.paginas:<5} linhas={r.linhas:<7} "
        f"{r.duracao_s or 0:.1f}s {r.erro or ''}"
        for r in resultados
    ]
    return "\n".join(linhas)
