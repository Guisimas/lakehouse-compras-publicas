"""Monta o landing a partir das fixtures, exatamente como o ingestor grava."""

from __future__ import annotations

from datetime import UTC, datetime


def escrever_landing(settings, linhas, run_id_ingestao="ing-teste"):
    """Grava páginas no landing exatamente como o ingestor faria."""
    from compras.ingestion.control import Controle, RegistroLog
    from compras.ingestion.endpoints import endpoint
    from compras.ingestion.landing import Landing
    from compras.ingestion.storage import LocalStorage

    storage = LocalStorage()
    landing = Landing(storage, settings.landing)
    for entidade, chave, ep, fanout, registros in linhas:
        spec = endpoint(ep)
        envelope = {
            "meta": {
                "endpoint": ep,
                "entidade": entidade,
                "fanout": fanout,
                "pagina": 1,
                "regime_legal": spec.regime_legal,
                "sistema_origem": spec.sistema_origem,
                "tipo_janela": spec.tipo_janela,
                "data_referencia": chave,
                "run_id": run_id_ingestao,
            },
            "payload": {
                "resultado": registros,
                "totalPaginas": 1,
                "totalRegistros": len(registros),
                "paginasRestantes": 0,
            },
        }
        landing.gravar_pagina(entidade, chave, ep, fanout, 1, envelope)
        landing.marcar_sucesso(entidade, chave, ep, fanout, {"run_id": run_id_ingestao})
    Controle(storage, settings.landing).gravar_log(
        RegistroLog(
            run_id=run_id_ingestao,
            entidade="contratos",
            janela="2026-10-01",
            fonte_dados="api",
            modo="incremental",
            inicio=datetime.now(UTC).isoformat(),
            fim=datetime.now(UTC).isoformat(),
            status="sucesso",
            linhas=1,
        )
    )
