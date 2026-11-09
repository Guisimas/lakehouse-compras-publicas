"""CLI de ingestão.

Exemplos::

    # Varredura de contratos: 365 dias até a data (precisa do snapshot de órgãos). Roda
    # se a última tiver 7 dias ou mais; --forcar-snapshot faz rodar na hora.
    compras-ingest api --entidades orgaos,contratos --data-fim 2026-10-01 \
        --destino local --landing-root ./data/landing

    # Mesmo comando gravando direto no UC Volume (alternativa quando o workspace não alcança a API)
    compras-ingest api --data-inicio 2026-10-01 --data-fim 2026-10-07 --destino volume

    # Alternativa de emergência fora do Databricks: os últimos 7 dias + as janelas de 14 e
    # 30 dias atrás no Volume e, se tudo deu certo, dispara o job (que então só processa
    # Bronze → Gold)
    compras-ingest --destino volume api --disparar-job 123456789

    # Backfill histórico (inclui endpoints anuais do Legado)
    compras-ingest api --modo historico --data-inicio 2025-01-01 --data-fim 2025-12-31

    # CSV do portal para a carga histórica
    compras-ingest csv --entidade contratos --ano 2024 --origem ./contratos_2024.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import timedelta

from compras.common.runtime import (
    hoje_brt,
    novo_run_id,
    ontem_brt,
    parse_data,
    planejar_janelas,
)
from compras.config import ENTIDADES, Settings
from compras.ingestion.control import Controle
from compras.ingestion.csv_backfill import ingerir_csv
from compras.ingestion.runner import executar_ingestao, resumo
from compras.ingestion.storage import LocalStorage, Storage, VolumeStorage


def _storage(destino: str) -> Storage:
    return VolumeStorage() if destino == "volume" else LocalStorage()


def _settings(args: argparse.Namespace) -> Settings:
    import os

    if args.catalog:
        os.environ["COMPRAS_CATALOG"] = args.catalog
    if args.landing_root:
        os.environ["COMPRAS_LANDING_ROOT"] = args.landing_root
    elif args.destino == "local":
        os.environ.setdefault("COMPRAS_LANDING_ROOT", "./data/landing")
    return Settings()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="compras-ingest", description=__doc__.split("\n")[0])
    parser.add_argument("--destino", choices=("local", "volume"), default="local")
    parser.add_argument("--landing-root", help="Raiz do landing (padrão: Volume do catálogo)")
    parser.add_argument("--catalog", help="Catálogo do Unity Catalog (padrão: dev_compras)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="comando", required=True)

    api = sub.add_parser("api", help="Ingestão pela API REST")
    api.add_argument("--entidades", default=",".join(ENTIDADES))
    api.add_argument("--data-inicio", type=parse_data)
    api.add_argument("--data-fim", type=parse_data)
    api.add_argument("--modo", choices=("incremental", "historico"), default="incremental")
    api.add_argument(
        "--dias-recentes",
        type=int,
        default=7,
        help="Sem --data-inicio, baixa os N dias até --data-fim (padrão: a última semana)",
    )
    api.add_argument(
        "--dias-reprocessar",
        default="14,30",
        help="Janelas antigas baixadas de novo (como no job), separadas por vírgula",
    )
    api.add_argument(
        "--disparar-job",
        type=int,
        help="ID do job do Databricks a iniciar depois de uma ingestão sem falhas",
    )
    api.add_argument("--forcar", action="store_true", help="Baixa de novo janelas já completas")
    api.add_argument(
        "--forcar-snapshot",
        action="store_true",
        help="Refaz snapshots (órgãos, catálogo) e a varredura de contratos, mesmo no prazo",
    )

    csv = sub.add_parser("csv", help="Copia um CSV do portal para o landing")
    csv.add_argument("--entidade", required=True, choices=ENTIDADES)
    csv.add_argument("--ano", type=int, required=True)
    csv.add_argument("--origem", required=True, help="Caminho local ou URL do CSV")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = _settings(args)
    storage = _storage(args.destino)
    run_id = args.run_id or novo_run_id()

    if args.comando == "csv":
        reg = ingerir_csv(
            origem=args.origem,
            entidade=args.entidade,
            ano=args.ano,
            storage=storage,
            root=settings.landing,
            controle=Controle(storage, settings.landing),
            run_id=run_id,
        )
        print(resumo([reg]))
        return 0 if reg.status == "sucesso" else 1

    fim = args.data_fim or ontem_brt()
    inicio = args.data_inicio or fim - timedelta(days=max(args.dias_recentes, 1) - 1)
    reprocessar = [int(d) for d in args.dias_reprocessar.split(",") if d.strip()]
    janelas = planejar_janelas(
        data_inicio=None,
        data_fim=fim,
        ultima_janela_ok=inicio - timedelta(days=1),
        dias_reprocessar=[] if args.data_inicio else reprocessar,
        max_dias_recuperacao=(fim - inicio).days + 1,
    )
    entidades = [e.strip() for e in args.entidades.split(",") if e.strip()]
    desconhecidas = set(entidades) - set(ENTIDADES)
    if desconhecidas:
        parser.error(f"Entidades desconhecidas: {sorted(desconhecidas)}")
    resultados = executar_ingestao(
        settings=settings,
        storage=storage,
        entidades=entidades,
        janelas=janelas,
        hoje=hoje_brt(),
        run_id=run_id,
        modo=args.modo,
        forcar=args.forcar,
        forcar_snapshot=args.forcar_snapshot,
    )
    print(f"run_id={run_id}  landing={settings.landing}")
    print(resumo(resultados))
    ok = all(r.status == "sucesso" for r in resultados)
    if ok and args.disparar_job:
        from databricks.sdk import WorkspaceClient

        run = WorkspaceClient().jobs.run_now(job_id=args.disparar_job)
        print(f"job {args.disparar_job} iniciado: run {run.run_id}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
