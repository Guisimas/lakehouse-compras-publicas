"""Layout do landing e leitura de dados já baixados.

Layout sempre função de entidade + janela, nunca do horário da execução::

    <root>/<entidade>/data_referencia=<janela>/<endpoint>__<fanout>__p00001.json
    <root>/<entidade>/data_referencia=<janela>/_SUCCESS__<endpoint>__<fanout>.json
    <root>/_controle/ingestao_log/<data_execucao>/<run_id>__<entidade>__<janela>.json
    <root>/_controle/watermark/<entidade>.json

``<janela>`` é ``YYYY-MM-DD`` (diária ou snapshot) ou ``YYYY`` (janela anual).
Cada página é gravada como ``{"meta": {...}, "payload": <resposta bruta da API>}``; o
``payload`` não sofre nenhuma alteração.
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta

from compras.common.validacao import cnpj_valido, somente_digitos
from compras.ingestion.storage import Storage

PREFIXO_SUCESSO = "_SUCCESS__"
_INSEGURO = re.compile(r"[^0-9A-Za-z_-]")
_PAGINA = re.compile(r"^(?P<endpoint>.+?)__(?P<fanout>.+?)__p(?P<n>\d{5})\.json$")


def chave_janela(janela: date, tipo: str) -> str:
    return str(janela.year) if tipo == "ano" else janela.isoformat()


def janela_de_chave(chave: str) -> date:
    return date(int(chave), 1, 1) if len(chave) == 4 else date.fromisoformat(chave)


def dir_janela(root: str, entidade: str, chave: str) -> str:
    return f"{root.rstrip('/')}/{entidade}/data_referencia={chave}"


def fanout_seguro(fanout: str) -> str:
    return _INSEGURO.sub("-", fanout)[:80]


def nome_pagina(endpoint: str, fanout: str, pagina: int) -> str:
    return f"{endpoint}__{fanout_seguro(fanout)}__p{pagina:05d}.json"


def nome_sucesso(endpoint: str, fanout: str) -> str:
    return f"{PREFIXO_SUCESSO}{endpoint}__{fanout_seguro(fanout)}.json"


def parse_nome_pagina(nome: str) -> tuple[str, str, int] | None:
    m = _PAGINA.match(nome)
    if not m:
        return None
    return m["endpoint"], m["fanout"], int(m["n"])


def _basename(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


class Landing:
    def __init__(self, storage: Storage, root: str):
        self.storage = storage
        self.root = root.rstrip("/")

    # --------------------------------------------------------------- escrita
    def gravar_pagina(
        self, entidade: str, chave: str, endpoint: str, fanout: str, pagina: int, envelope: dict
    ) -> str:
        path = f"{dir_janela(self.root, entidade, chave)}/{nome_pagina(endpoint, fanout, pagina)}"
        self.storage.write_bytes(path, json.dumps(envelope, ensure_ascii=False).encode("utf-8"))
        return path

    def marcar_sucesso(
        self, entidade: str, chave: str, endpoint: str, fanout: str, resumo: dict
    ) -> None:
        path = f"{dir_janela(self.root, entidade, chave)}/{nome_sucesso(endpoint, fanout)}"
        self.storage.write_bytes(path, json.dumps(resumo).encode("utf-8"))

    def ler_sucesso(self, entidade: str, chave: str, endpoint: str, fanout: str) -> dict | None:
        path = f"{dir_janela(self.root, entidade, chave)}/{nome_sucesso(endpoint, fanout)}"
        if not self.storage.exists(path):
            return None
        return json.loads(self.storage.read_bytes(path))

    def paginas_existentes(
        self, entidade: str, chave: str, endpoint: str, fanout: str
    ) -> list[str]:
        prefixo = f"{endpoint}__{fanout_seguro(fanout)}__p"
        return [
            f
            for f in self.storage.list_files(dir_janela(self.root, entidade, chave))
            if _basename(f).startswith(prefixo)
        ]

    def ultima_pagina(self, entidade: str, chave: str, endpoint: str, fanout: str) -> int:
        """Última página já gravada do fan-out (0 se nenhuma).

        Só lista a pasta quando a página 1 existe (retomada). A pasta de uma varredura de
        contratos tem milhares de arquivos, e listá-la num UC Volume a cada órgão custava ~30 s.
        """
        base = dir_janela(self.root, entidade, chave)
        if not self.storage.exists(f"{base}/{nome_pagina(endpoint, fanout, 1)}"):
            return 0
        return max(
            (
                p[2]
                for f in self.paginas_existentes(entidade, chave, endpoint, fanout)
                if (p := parse_nome_pagina(_basename(f)))
            ),
            default=0,
        )

    def limpar(self, entidade: str, chave: str, endpoint: str, fanout: str) -> None:
        for f in self.paginas_existentes(entidade, chave, endpoint, fanout):
            self.storage.delete(f)
        sucesso = f"{dir_janela(self.root, entidade, chave)}/{nome_sucesso(endpoint, fanout)}"
        self.storage.delete(sucesso)

    # --------------------------------------------------------------- leitura
    def chaves(self, entidade: str) -> list[str]:
        dirs = self.storage.list_dirs(f"{self.root}/{entidade}")
        return sorted(
            _basename(d).split("=", 1)[1]
            for d in dirs
            if _basename(d).startswith("data_referencia=")
        )

    def registros(self, entidade: str, chave: str, endpoint: str | None = None) -> list[dict]:
        saida: list[dict] = []
        for f in self.storage.list_files(dir_janela(self.root, entidade, chave)):
            partes = parse_nome_pagina(_basename(f))
            if partes is None or (endpoint and partes[0] != endpoint):
                continue
            envelope = json.loads(self.storage.read_bytes(f))
            payload = envelope.get("payload")
            if isinstance(payload, dict):
                res = payload.get("resultado") or []
                saida.extend([res] if isinstance(res, dict) else res)
            elif isinstance(payload, list):
                saida.extend(payload)
        return saida


class LandingFanout:
    """Implementa ``FanoutContext`` lendo o que já foi baixado para o landing."""

    def __init__(
        self,
        landing: Landing,
        contratos_incrementais: bool = False,
        dias_sem_reconsulta: int = 30,
    ):
        """Fornecedores vindos da varredura de contratos (um ano inteiro, ~25 mil CNPJs; a
        ~1,4 s por consulta, ~10 h). Com ``contratos_incrementais``:

        - só contam contratos incluídos depois da inclusão mais recente da varredura
          anterior. A fonte publica em lotes, então uma janela fixa de dias falharia;
          na primeira varredura nenhum conta (os fornecedores viram membros inferidos);

        Sem ``contratos_incrementais`` (backfill), todos os contratos contam.

        Em qualquer modo, CNPJs (de contratos ou de itens) com cadastro consultado nos
        últimos ``dias_sem_reconsulta`` dias ficam de fora: o mesmo fornecedor vende em muitos
        dias, e cada consulta custa ~1,4 s.
        """
        self.landing = landing
        self.contratos_incrementais = contratos_incrementais
        self.dias_sem_reconsulta = dias_sem_reconsulta
        self._orgaos: list[str] | None = None

    def codigos_orgao(self) -> list[str]:
        if self._orgaos is None:
            chaves = self.landing.chaves("orgaos")
            if not chaves:
                raise RuntimeError(
                    "Nenhum snapshot de órgãos no landing; rode a entidade 'orgaos' antes de "
                    "'contratos'."
                )
            regs = self.landing.registros("orgaos", chaves[-1], endpoint="orgao")
            self._orgaos = sorted(
                {
                    str(r["codigoOrgao"])
                    for r in regs
                    if r.get("codigoOrgao") is not None
                    and r.get("usoSisg", True)
                    # O cadastro traz ~12 mil órgãos, a maioria municipal e estadual, que
                    # não têm contratos nesse módulo (amostra conferida em 2026-11-06).
                    # Escopo do projeto: Governo Federal. Reduz o fan-out diário a ~1,1 mil.
                    and r.get("esfera", "F") == "F"
                },
                key=lambda s: (len(s), s),
            )
        return self._orgaos

    def ids_compra_legado(self, janela: date) -> list[str]:
        regs = self.landing.registros(
            "licitacoes", janela.isoformat(), endpoint="legado_licitacoes"
        )
        return sorted({str(r["id_compra"]) for r in regs if r.get("id_compra")})

    def compras_pncp_sem_catalogo(self, janela: date) -> list[str]:
        """Compras do PNCP da janela com algum item sem código do catálogo."""
        regs = self.landing.registros("itens_compra", janela.isoformat(), endpoint="pncp_itens")
        return sorted(
            {
                str(r["idContratacaoPNCP"])
                for r in regs
                if r.get("idContratacaoPNCP") and not r.get("codItemCatalogo")
            }
        )

    def cnpjs_fornecedores(self, janela: date) -> list[str]:
        chave = janela.isoformat()
        candidatos: set[str] = set()
        campos = ("niFornecedor", "codFornecedor", "cnpj_fornecedor", "nu_cnpj_vencedor")
        for r in self.landing.registros("itens_compra", chave):
            candidatos |= _cnpjs(r, campos)
        contratos = self.landing.registros("contratos", chave)
        if contratos and self.contratos_incrementais:
            corte = self._corte_inclusao(chave)
            do_contrato: set[str] = set()
            for r in contratos:
                incluido = str(r.get("dataHoraInclusao") or "")
                if corte is not None and (not incluido or incluido > corte):
                    do_contrato |= _cnpjs(r, campos)
            candidatos |= do_contrato
        else:
            for r in contratos:
                candidatos |= _cnpjs(r, campos)
        return sorted(candidatos - self._consultados_recentes(janela))

    def _corte_inclusao(self, chave: str) -> str | None:
        """Inclusão mais recente da varredura anterior (``None`` se esta é a primeira)."""
        anteriores = [c for c in self.landing.chaves("contratos") if c < chave]
        if not anteriores:
            return None
        datas = [
            str(r.get("dataHoraInclusao") or "")
            for r in self.landing.registros("contratos", anteriores[-1])
        ]
        return max(datas, default="")

    def _consultados_recentes(self, janela: date) -> set[str]:
        """CNPJs com cadastro baixado nos últimos ``dias_sem_reconsulta`` dias."""
        inicio = (janela - timedelta(days=self.dias_sem_reconsulta)).isoformat()
        prefixo = f"{PREFIXO_SUCESSO}fornecedor__cnpj"
        saida: set[str] = set()
        for chave in self.landing.chaves("fornecedores"):
            if not inicio <= chave <= janela.isoformat():
                continue
            for f in self.landing.storage.list_files(
                dir_janela(self.landing.root, "fornecedores", chave)
            ):
                nome = _basename(f)
                if nome.startswith(prefixo):
                    saida.add(nome[len(prefixo) :].removesuffix(".json"))
        return saida


def _cnpjs(registro: dict, campos: tuple[str, ...]) -> set[str]:
    valores = (somente_digitos(str(registro.get(c) or "")) for c in campos)
    return {v for v in valores if cnpj_valido(v)}
