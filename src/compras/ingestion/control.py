"""Controle da ingestão: log de execução e watermark.

A fonte da verdade fica no próprio landing, em JSON, para que a ingestão funcione igual
dentro do Databricks e numa máquina local. A task ``bronze`` espelha esses arquivos nas
tabelas Delta ``ctl_ingestao_log`` e ``ctl_watermark`` (ver ``compras.bronze.controle``).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime

from compras.ingestion.storage import Storage


@dataclass
class RegistroLog:
    run_id: str
    entidade: str
    janela: str
    fonte_dados: str
    modo: str
    inicio: str
    fim: str | None = None
    duracao_s: float | None = None
    endpoints: int = 0
    fanouts: int = 0
    fanouts_pulados: int = 0
    paginas: int = 0
    linhas: int = 0
    status: str = "em_execucao"  # sucesso | falha | em_execucao
    erro: str | None = None
    detalhes: list[dict] = field(default_factory=list)
    log_id: str = field(default_factory=lambda: uuid.uuid4().hex)


@dataclass
class Watermark:
    entidade: str
    ultima_janela: str | None
    status: str
    atualizado_em: str
    run_id: str


class Controle:
    def __init__(self, storage: Storage, root: str):
        self.storage = storage
        self.base = f"{root.rstrip('/')}/_controle"

    # --------------------------------------------------------------- log
    def gravar_log(self, reg: RegistroLog) -> str:
        dia = reg.inicio[:10]
        # log_id único: retries do mesmo run geram linhas distintas; o relógio do
        # Windows tem resolução de ~15 ms, então o instante de início pode colidir
        path = (
            f"{self.base}/ingestao_log/{dia}/"
            f"{reg.run_id}__{reg.entidade}__{reg.janela}__{reg.log_id[:12]}.json"
        )
        self.storage.write_bytes(path, json.dumps(asdict(reg), ensure_ascii=False).encode())
        return path

    def logs(self) -> list[dict]:
        saida = []
        for d in self.storage.list_dirs(f"{self.base}/ingestao_log"):
            for f in self.storage.list_files(d):
                if f.endswith(".json"):
                    saida.append(json.loads(self.storage.read_bytes(f)))
        return saida

    # --------------------------------------------------------------- watermark
    def _wm_path(self, entidade: str) -> str:
        return f"{self.base}/watermark/{entidade}.json"

    def ler_watermark(self, entidade: str) -> Watermark | None:
        path = self._wm_path(entidade)
        if not self.storage.exists(path):
            return None
        return Watermark(**json.loads(self.storage.read_bytes(path)))

    def atualizar_watermark(
        self, entidade: str, janela: date, run_id: str, agora: datetime
    ) -> Watermark:
        """Avança o watermark só para frente (reprocessar janela antiga não o rebaixa)."""
        atual = self.ler_watermark(entidade)
        nova = janela.isoformat()
        if atual and atual.ultima_janela and atual.ultima_janela >= nova:
            nova = atual.ultima_janela
        wm = Watermark(entidade, nova, "sucesso", agora.isoformat(), run_id)
        self.storage.write_bytes(self._wm_path(entidade), json.dumps(asdict(wm)).encode())
        return wm

    def watermarks(self) -> list[dict]:
        return [
            json.loads(self.storage.read_bytes(f))
            for f in self.storage.list_files(f"{self.base}/watermark")
            if f.endswith(".json")
        ]
