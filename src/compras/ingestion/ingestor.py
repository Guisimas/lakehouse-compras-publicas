"""Orquestra a ingestão de uma entidade numa janela: fan-out, páginas, retomada e log.

Garantias:
- Idempotência: o destino depende só de entidade, janela, endpoint, fan-out e página.
  Um fan-out que já tem marcador ``_SUCCESS`` é pulado; com ``forcar=True`` ele é apagado e
  baixado de novo, sobrescrevendo os mesmos arquivos. Com ``refazer=True`` só os endpoints
  principais (sem ``depende_de``) são baixados de novo; complementos e fornecedores já
  baixados continuam pulados.
- Retomada: se um run caiu no meio, o próximo continua da última página gravada.
- Log: toda execução gera um registro de log, inclusive as que falham.
"""

from __future__ import annotations

import logging
import time
import traceback
from dataclasses import dataclass
from datetime import date, timedelta

from compras.common.runtime import agora_utc
from compras.ingestion.client import ComprasClient
from compras.ingestion.control import Controle, RegistroLog
from compras.ingestion.endpoints import (
    EndpointSpec,
    FanoutContext,
    Modo,
    endpoints_da_entidade,
)
from compras.ingestion.landing import Landing, chave_janela

log = logging.getLogger(__name__)


@dataclass
class ResultadoFanout:
    endpoint: str
    fanout: str
    paginas: int
    linhas: int
    pulado: bool


class Ingestor:
    def __init__(
        self,
        client: ComprasClient,
        landing: Landing,
        controle: Controle,
        run_id: str,
        fanout_ctx: FanoutContext | None = None,
    ):
        self.client = client
        self.landing = landing
        self.controle = controle
        self.run_id = run_id
        self.fanout_ctx = fanout_ctx

    # ------------------------------------------------------------------ público
    def snapshot_vencido(self, entidade: str, hoje: date) -> bool:
        wm = self.controle.ler_watermark(entidade)
        if wm is None or wm.ultima_janela is None:
            return True
        refresh = min(e.refresh_dias for e in endpoints_da_entidade(entidade))
        return date.fromisoformat(wm.ultima_janela) <= hoje - timedelta(days=refresh)

    def ingerir(
        self,
        entidade: str,
        janela: date,
        *,
        modo: Modo = "incremental",
        forcar: bool = False,
        refazer: bool = False,
    ) -> RegistroLog:
        specs = endpoints_da_entidade(entidade, modo)
        chave_log = janela.isoformat()
        reg = RegistroLog(
            run_id=self.run_id,
            entidade=entidade,
            janela=chave_log,
            fonte_dados="api",
            modo=modo,
            inicio=agora_utc().isoformat(),
            endpoints=len(specs),
        )
        t0 = time.monotonic()
        try:
            for spec in specs:
                try:
                    forcar_spec = forcar or (refazer and not spec.depende_de)
                    for r in self._ingerir_endpoint(spec, janela, forcar_spec):
                        reg.fanouts += 1
                        reg.fanouts_pulados += int(r.pulado)
                        reg.paginas += r.paginas
                        reg.linhas += r.linhas
                        reg.detalhes.append(r.__dict__)
                except Exception as exc:
                    if not spec.opcional:
                        raise
                    # Complemento fora do ar não derruba a entidade: os fan-outs que
                    # faltaram ficam sem _SUCCESS e voltam no próximo run.
                    log.warning(
                        "Complemento %s de %s/%s interrompido: %s",
                        spec.nome,
                        entidade,
                        chave_log,
                        exc,
                    )
                    reg.detalhes.append(
                        {"endpoint": spec.nome, "aviso": f"{type(exc).__name__}: {exc}"[:500]}
                    )
            reg.status = "sucesso"
        except Exception as exc:  # o log precisa existir mesmo na falha
            reg.status = "falha"
            reg.erro = f"{type(exc).__name__}: {exc}"
            log.error("Falha ao ingerir %s/%s: %s", entidade, chave_log, traceback.format_exc())
        finally:
            reg.fim = agora_utc().isoformat()
            reg.duracao_s = round(time.monotonic() - t0, 3)
            # detalhes por fan-out só quando cabem; contratos tem centenas de órgãos
            if len(reg.detalhes) > 200:
                reg.detalhes = [d for d in reg.detalhes if not d.get("pulado")][:200]
            self.controle.gravar_log(reg)
        if reg.status == "sucesso":
            self.controle.atualizar_watermark(entidade, janela, self.run_id, agora_utc())
        log.info(
            "%s %s %s: %s páginas, %s linhas, %.1fs",
            entidade,
            chave_log,
            reg.status,
            reg.paginas,
            reg.linhas,
            reg.duracao_s,
        )
        return reg

    # ------------------------------------------------------------------ interno
    def _ingerir_endpoint(self, spec: EndpointSpec, janela: date, forcar: bool):
        chave = chave_janela(janela, spec.tipo_janela)
        for fanout, params in spec.params(janela, self.fanout_ctx):
            yield self._ingerir_fanout(spec, chave, fanout, params, forcar)

    def _ingerir_fanout(
        self, spec: EndpointSpec, chave: str, fanout: str, params: dict, forcar: bool
    ) -> ResultadoFanout:
        entidade = spec.entidade
        if forcar:
            self.landing.limpar(entidade, chave, spec.nome, fanout)
        else:
            sucesso = self.landing.ler_sucesso(entidade, chave, spec.nome, fanout)
            if sucesso is not None:
                return ResultadoFanout(spec.nome, fanout, 0, 0, pulado=True)

        ultima = self.landing.ultima_pagina(entidade, chave, spec.nome, fanout)
        path, query = spec.requisicao(params)
        paginas = linhas = 0
        total_paginas = total_registros = None
        for pagina in self.client.paginas(
            path,
            query,
            paginado=spec.paginado,
            aceita_tamanho_pagina=spec.aceita_tamanho_pagina,
            pagina_inicial=ultima + 1,
            vazio_em_404=spec.vazio_em_404,
        ):
            total_paginas, total_registros = pagina.total_paginas, pagina.total_registros
            # Página vazia depois da primeira = já tínhamos tudo (run anterior caiu antes
            # de gravar o _SUCCESS). Não grava arquivo vazio.
            if not pagina.registros and pagina.numero > 1:
                break
            envelope = {
                "meta": {
                    "endpoint": spec.nome,
                    "entidade": entidade,
                    "path": path,
                    "fanout": fanout,
                    "params": {k: str(v) for k, v in params.items()},
                    "pagina": pagina.numero,
                    "url": pagina.url,
                    "total_paginas": total_paginas,
                    "total_registros": total_registros,
                    "regime_legal": spec.regime_legal,
                    "sistema_origem": spec.sistema_origem,
                    "tipo_janela": spec.tipo_janela,
                    "data_referencia": chave,
                    "run_id": self.run_id,
                    "baixado_em": agora_utc().isoformat(),
                },
                "payload": pagina.payload,
            }
            self.landing.gravar_pagina(entidade, chave, spec.nome, fanout, pagina.numero, envelope)
            paginas += 1
            linhas += len(pagina.registros)

        self.landing.marcar_sucesso(
            entidade,
            chave,
            spec.nome,
            fanout,
            {
                "run_id": self.run_id,
                "paginas_total": ultima + paginas,
                "total_paginas_api": total_paginas,
                "total_registros_api": total_registros,
                "concluido_em": agora_utc().isoformat(),
            },
        )
        return ResultadoFanout(spec.nome, fanout, paginas, linhas, pulado=False)
