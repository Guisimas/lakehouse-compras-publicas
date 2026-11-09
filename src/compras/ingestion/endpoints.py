"""Registro declarativo: entidade do projeto → endpoints da API.

Os nomes e parâmetros foram conferidos no OpenAPI (``/v3/api-docs``) em 2026-11.

Tipos de janela:
- ``dia``: uma janela por data de evento (publicação, inclusão, vigência).
- ``ano``: a API só filtra por ano (compras sem licitação do Legado); usado no backfill.
- ``snapshot``: cadastro completo, recarregado a cada ``refresh_dias``.
- ``varredura``: janela móvel longa (``dias_varredura`` até a data de referência), refeita a
  cada ``refresh_dias``. Usada em contratos, que a fonte publica com meses de atraso.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Literal, Protocol

TipoJanela = Literal["dia", "ano", "snapshot", "varredura"]
Modo = Literal["incremental", "historico"]

# Modalidades da Lei 14.133 no PNCP (codigoModalidade). O endpoint exige uma por chamada.
MODALIDADES_14133 = {
    1: "Leilão - Eletrônico",
    2: "Diálogo Competitivo",
    3: "Concurso",
    4: "Concorrência - Eletrônica",
    5: "Concorrência - Presencial",
    6: "Pregão - Eletrônico",
    7: "Pregão - Presencial",
    8: "Dispensa de Licitação",
    9: "Inexigibilidade",
    10: "Manifestação de Interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão - Presencial",
    14: "Inaplicabilidade da Licitação",
}


def _dia_seguinte(janela: date) -> date:
    """Fim exclusivo: nos endpoints do PNCP a data final não entra no resultado.

    Conferido na API real em 2026-11-06: ``Inicial=D, Final=D`` devolve zero registros e
    ``Inicial=D, Final=D+1`` devolve só o dia D. Se a API passar a tratar o fim como
    inclusivo, o dia D+1 entra duplicado e a dedup da Silver resolve; nada se perde.
    """
    return janela + timedelta(days=1)


DIAS_VARREDURA_CONTRATOS = 365


def inicio_varredura(referencia: date, dias: int) -> date:
    """Primeiro dia de uma varredura de ``dias`` dias que termina em ``referencia``.

    Com 365 dias, ``max - min`` fica em 364, dentro do limite da API de contratos (acima de
    365 ela responde HTTP 400). A API ainda soma um dia ao máximo, o que é inofensivo.
    """
    return referencia - timedelta(days=dias - 1)


class FanoutContext(Protocol):
    """Listas de que alguns endpoints dependem (vêm de dados já no landing)."""

    def codigos_orgao(self) -> list[str]: ...
    def ids_compra_legado(self, janela: date) -> list[str]: ...
    def cnpjs_fornecedores(self, janela: date) -> list[str]: ...


ParamsFn = Callable[[date, FanoutContext | None], list[tuple[str, dict[str, Any]]]]


@dataclass(frozen=True)
class EndpointSpec:
    nome: str
    entidade: str
    path: str
    sistema_origem: str
    params: ParamsFn
    tipo_janela: TipoJanela = "dia"
    regime_legal: str | None = None  # None = derivado do registro na Silver
    paginado: bool = True
    aceita_tamanho_pagina: bool = True
    modos: tuple[Modo, ...] = ("incremental", "historico")
    refresh_dias: int = 7  # snapshot e varredura
    dias_varredura: int = 365  # só para varredura (limite da API de contratos)
    depende_de: tuple[str, ...] = field(default_factory=tuple)


def _unico(params: dict[str, Any]) -> ParamsFn:
    return lambda _janela, _ctx: [("all", params)]


def _ctx(ctx: FanoutContext | None) -> FanoutContext:
    if ctx is None:
        raise ValueError("Este endpoint precisa de um FanoutContext")
    return ctx


ENDPOINTS: tuple[EndpointSpec, ...] = (
    # ---------------------------------------------------------------- referência
    EndpointSpec(
        nome="uasg",
        entidade="orgaos",
        path="/modulo-uasg/1_consultarUasg",
        sistema_origem="uasg",
        params=_unico({"statusUasg": True}),
        tipo_janela="snapshot",
        aceita_tamanho_pagina=False,
    ),
    EndpointSpec(
        nome="orgao",
        entidade="orgaos",
        path="/modulo-uasg/2_consultarOrgao",
        sistema_origem="uasg",
        params=_unico({"statusOrgao": True}),
        tipo_janela="snapshot",
        aceita_tamanho_pagina=False,
    ),
    EndpointSpec(
        nome="catmat",
        entidade="catalogo",
        path="/modulo-material/4_consultarItemMaterial",
        sistema_origem="catalogo",
        params=_unico({}),
        tipo_janela="snapshot",
    ),
    EndpointSpec(
        nome="catser",
        entidade="catalogo",
        path="/modulo-servico/6_consultarItemServico",
        sistema_origem="catalogo",
        params=_unico({}),
        tipo_janela="snapshot",
    ),
    # ---------------------------------------------------------------- licitações
    EndpointSpec(
        nome="pncp_contratacoes",
        entidade="licitacoes",
        path="/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
        sistema_origem="pncp",
        regime_legal="14133",
        params=lambda j, _c: [
            (
                f"mod{cod:02d}",
                {
                    "dataPublicacaoPncpInicial": j,
                    "dataPublicacaoPncpFinal": _dia_seguinte(j),
                    "codigoModalidade": cod,
                },
            )
            for cod in MODALIDADES_14133
        ],
    ),
    EndpointSpec(
        nome="legado_licitacoes",
        entidade="licitacoes",
        path="/modulo-legado/1_consultarLicitacao",
        sistema_origem="legado",
        regime_legal="8666",
        # pertence14133=false evita contar duas vezes o que já vem do PNCP.
        params=lambda j, _c: [
            (
                "all",
                {"data_publicacao_inicial": j, "data_publicacao_final": j, "pertence14133": False},
            )
        ],
    ),
    EndpointSpec(
        nome="legado_compras_sem_licitacao",
        entidade="licitacoes",
        path="/modulo-legado/5_consultarComprasSemLicitacao",
        sistema_origem="legado",
        params=lambda j, _c: [("all", {"dt_ano_aviso": j.year})],
        tipo_janela="ano",
        modos=("historico",),
    ),
    # ---------------------------------------------------------------- itens
    EndpointSpec(
        nome="pncp_itens",
        entidade="itens_compra",
        path="/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133",
        sistema_origem="pncp",
        regime_legal="14133",
        params=lambda j, _c: [
            ("all", {"dataInclusaoPncpInicial": j, "dataInclusaoPncpFinal": _dia_seguinte(j)})
        ],
    ),
    EndpointSpec(
        nome="legado_itens_licitacao",
        entidade="itens_compra",
        path="/modulo-legado/2.1_consultarItemLicitacao_Id",
        sistema_origem="legado",
        regime_legal="8666",
        # O endpoint de lista exige modalidade e não filtra por data; buscamos os itens
        # das licitações publicadas na janela, uma chamada por id_compra.
        params=lambda j, c: [(f"id{i}", {"id_compra": i}) for i in _ctx(c).ids_compra_legado(j)],
        paginado=False,
        depende_de=("licitacoes",),
    ),
    EndpointSpec(
        nome="legado_itens_sem_licitacao",
        entidade="itens_compra",
        path="/modulo-legado/6_consultarCompraItensSemLicitacao",
        sistema_origem="legado",
        params=lambda j, _c: [("all", {"dt_ano_aviso_licitacao": j.year})],
        tipo_janela="ano",
        modos=("historico",),
    ),
    # ---------------------------------------------------------------- contratos
    EndpointSpec(
        nome="contratos",
        entidade="contratos",
        path="/modulo-contratos/1_consultarContratos",
        sistema_origem="contratos",
        # A fonte publica contratos com meses de atraso em relação ao início da vigência,
        # então uma janela diária quase sempre vem vazia. Em vez disso, uma varredura
        # semanal pede, por órgão, os 365 dias até a data de referência (o máximo que a
        # API aceita). A Silver deduplica por id_contrato e só reescreve o que mudou.
        # codigoOrgao é obrigatório: uma chamada por órgão federal do último snapshot.
        params=lambda j, c: [
            (
                f"org{o}",
                {
                    "codigoOrgao": o,
                    "dataVigenciaInicialMin": inicio_varredura(j, DIAS_VARREDURA_CONTRATOS),
                    "dataVigenciaInicialMax": j,
                },
            )
            for o in _ctx(c).codigos_orgao()
        ],
        tipo_janela="varredura",
        refresh_dias=7,
        dias_varredura=DIAS_VARREDURA_CONTRATOS,
        depende_de=("orgaos",),
    ),
    # ---------------------------------------------------------------- fornecedores
    EndpointSpec(
        nome="fornecedor",
        entidade="fornecedores",
        path="/modulo-fornecedor/1_consultarFornecedor",
        sistema_origem="fornecedor",
        # Cadastro dos CNPJs que aparecem nos contratos e itens da janela. Assim o SCD2
        # acompanha quem efetivamente vende, sem baixar o SICAF inteiro todo dia.
        params=lambda j, c: [
            (f"cnpj{n}", {"cnpj": n, "ativo": True}) for n in _ctx(c).cnpjs_fornecedores(j)
        ],
        depende_de=("contratos", "itens_compra"),
    ),
)

ORDEM_ENTIDADES = ("orgaos", "catalogo", "licitacoes", "itens_compra", "contratos", "fornecedores")


def endpoints_da_entidade(entidade: str, modo: Modo = "incremental") -> list[EndpointSpec]:
    specs = [e for e in ENDPOINTS if e.entidade == entidade and modo in e.modos]
    if not any(e.entidade == entidade for e in ENDPOINTS):
        raise KeyError(f"Entidade desconhecida: {entidade}")
    return specs


def endpoint(nome: str) -> EndpointSpec:
    for e in ENDPOINTS:
        if e.nome == nome:
            return e
    raise KeyError(nome)


def eh_snapshot(entidade: str) -> bool:
    return all(e.tipo_janela == "snapshot" for e in ENDPOINTS if e.entidade == entidade)


def eh_varredura(entidade: str) -> bool:
    return all(e.tipo_janela == "varredura" for e in ENDPOINTS if e.entidade == entidade)


def refresh_dias(entidade: str) -> int:
    return min(e.refresh_dias for e in ENDPOINTS if e.entidade == entidade)


def dias_varredura(entidade: str) -> int:
    return min(e.dias_varredura for e in ENDPOINTS if e.entidade == entidade)
