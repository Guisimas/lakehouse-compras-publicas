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

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from string import Formatter
from typing import Any, Literal, Protocol

TipoJanela = Literal["dia", "ano", "snapshot", "varredura"]
Modo = Literal["incremental", "historico"]

# Modalidades da Lei 14.133 no endpoint de contratações (codigoModalidade). O endpoint exige
# uma por chamada. Os códigos seguem a tabela do SIASG, e não a do portal do PNCP: conferido na
# API real em 2026-11-11, consultando os códigos 1 a 24, 33, 44 e 99 em 12 meses
# (nov/2025 a out/2026), só estes quatro devolvem registros.
MODALIDADES_14133 = {
    3: "Concorrência",
    5: "Pregão",
    6: "Dispensa",
    7: "Inexigibilidade",
}


def _dia_seguinte(janela: date) -> date:
    """Fim exclusivo: nos endpoints do PNCP a data final não entra no resultado.

    Conferido na API real em 2026-11-06: ``Inicial=D, Final=D`` devolve zero registros e
    ``Inicial=D, Final=D+1`` devolve só o dia D. Se a API passar a tratar o fim como
    inclusivo, o dia D+1 entra duplicado e a dedup da Silver resolve; nada se perde.
    """
    return janela + timedelta(days=1)


DIAS_VARREDURA_CONTRATOS = 365


# API do PNCP (fora do dadosabertos), usada só para completar o código do catálogo dos itens.
PNCP_API = "https://pncp.gov.br/api/pncp"
_ID_CONTRATACAO_PNCP = re.compile(r"^(?P<cnpj>\d{14})-\d+-(?P<seq>\d+)/(?P<ano>\d{4})$")


def fanout_compra_pncp(id_contratacao: str) -> tuple[str, dict[str, Any]] | None:
    """``10637926000146-1-000290/2026`` → (``10637926000146-2026-290``, parâmetros da URL).

    O fan-out ``<cnpj>-<ano>-<sequencial>`` é a chave que a Silver usa para casar o item do
    PNCP com o item do dadosabertos.
    """
    m = _ID_CONTRATACAO_PNCP.match(id_contratacao.strip())
    if not m:
        return None
    seq = int(m["seq"])
    params = {"cnpj": m["cnpj"], "ano": m["ano"], "sequencial": seq}
    return f"{m['cnpj']}-{m['ano']}-{seq}", params


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
    def compras_pncp_sem_catalogo(self, janela: date) -> list[str]: ...


ParamsFn = Callable[[date, FanoutContext | None], list[tuple[str, dict[str, Any]]]]


@dataclass(frozen=True)
class EndpointSpec:
    nome: str
    entidade: str
    # Caminho relativo à API de Dados Abertos ou URL absoluta. Campos ``{nome}`` são
    # preenchidos com os parâmetros de mesmo nome, que saem da query string.
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
    vazio_em_404: bool = False  # 404 = nada a baixar (ex.: compra que saiu do PNCP)
    # Complemento: falha nele não derruba a entidade; o fan-out fica sem _SUCCESS e é
    # tentado de novo no próximo run.
    opcional: bool = False

    def requisicao(self, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Separa os parâmetros do caminho (``{nome}``) dos da query string."""
        campos = {n for _, n, _, _ in Formatter().parse(self.path) if n}
        path = self.path.format(**{k: params[k] for k in campos})
        return path, {k: v for k, v in params.items() if k not in campos}


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
        nome="pncp_itens_catalogo",
        entidade="itens_compra",
        # Desde mai/2026 o endpoint acima manda codItemCatalogo, codigoPdm, codigoClasse e
        # codigoGrupo vazios. A API do PNCP traz o código em catalogoCodigoItem; uma chamada
        # por compra com item sem código. Se o dadosabertos voltar a mandar o código, o
        # fan-out fica vazio sozinho.
        path=f"{PNCP_API}/v1/orgaos/{{cnpj}}/compras/{{ano}}/{{sequencial}}/itens",
        sistema_origem="pncp",
        regime_legal="14133",
        params=lambda j, c: [
            f for i in _ctx(c).compras_pncp_sem_catalogo(j) if (f := fanout_compra_pncp(i))
        ],
        vazio_em_404=True,
        opcional=True,
        depende_de=("itens_compra",),
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
