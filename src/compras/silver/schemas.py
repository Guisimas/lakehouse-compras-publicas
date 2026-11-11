"""Schema canônico de cada entidade na Silver e o mapeamento de cada endpoint para ele.

Cada endpoint (ou CSV) tem nomes de campo próprios; aqui eles viram um único schema em
snake_case por entidade. Os valores ainda são texto: a conversão de tipos acontece em
``transform.py`` a partir do ``tipo`` declarado em cada coluna, o que permite às regras de
qualidade distinguir "veio vazio" de "veio inválido".

Mapeamento: ``{coluna_canonica: Expr}``, em que ``Expr`` é o nome de uma coluna da Bronze,
uma tupla de candidatos (o primeiro não nulo vence) ou uma função ``Fonte -> Column``.
Colunas canônicas ausentes no mapeamento ficam nulas.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import Column
from pyspark.sql import functions as F


class Fonte:
    """Acesso tolerante às colunas da Bronze: coluna inexistente vira null."""

    def __init__(self, colunas_disponiveis: list[str]):
        self._cols = {c.lower(): c for c in colunas_disponiveis}

    def c(self, *nomes: str) -> Column:
        existentes = [F.col(f"`{self._cols[n.lower()]}`") for n in nomes if n.lower() in self._cols]
        if not existentes:
            return F.lit(None).cast("string")
        texto = [F.when(F.trim(e) != "", e) for e in existentes]
        return texto[0] if len(texto) == 1 else F.coalesce(*texto)

    def tem(self, nome: str) -> bool:
        return nome.lower() in self._cols


Expr = str | tuple[str, ...] | Callable[[Fonte], Column]


def resolver(expr: Expr, fonte: Fonte) -> Column:
    if isinstance(expr, str):
        return fonte.c(expr)
    if isinstance(expr, tuple):
        return fonte.c(*expr)
    return expr(fonte)


def lit(valor: str | None) -> Callable[[Fonte], Column]:
    return lambda _f: F.lit(valor).cast("string")


def regime_por_flag(flag: str, padrao: str = "8666") -> Callable[[Fonte], Column]:
    """``pertence14133`` = true → 14133; senão o regime padrão do endpoint."""
    return lambda f: F.when(F.lower(f.c(flag)) == "true", F.lit("14133")).otherwise(F.lit(padrao))


def tipo_item(expr: Callable[[Fonte], Column]) -> Callable[[Fonte], Column]:
    return lambda f: F.upper(F.substring(F.trim(expr(f)), 1, 1))


@dataclass(frozen=True)
class Coluna:
    nome: str
    tipo: str = "string"  # string|texto|uasg|ni|date|timestamp|int|boolean|decimal(p,s)


@dataclass(frozen=True)
class EntidadeSilver:
    nome: str
    chave: tuple[str, ...]
    colunas: tuple[Coluna, ...]
    mapeamentos: dict[str, dict[str, Expr]]
    # colunas de origem que precisam existir em cada endpoint (DQ-01)
    obrigatorias_origem: dict[str, tuple[str, ...]] = field(default_factory=dict)
    prioridade: dict[str, int] = field(default_factory=dict)  # menor vence no dedup
    data_evento: str | None = None  # gera ano_mes (partição)
    obrigatorias: tuple[str, ...] = ()  # DQ-02
    valores: tuple[str, ...] = ()  # DQ-04
    datas_nao_futuras: tuple[str, ...] = ()  # DQ-06
    tem_fornecedor: bool = False  # DQ-03

    @property
    def nomes(self) -> list[str]:
        return [c.nome for c in self.colunas]

    def tipo(self, nome: str) -> str:
        return next(c.tipo for c in self.colunas if c.nome == nome)


# ============================================================== licitações (grão: compra)
_LEGADO_LICITACAO: dict[str, Expr] = {
    "id_compra": "id_compra",
    "regime_legal": regime_por_flag("pertence14133"),
    "codigo_uasg": "uasg",
    "codigo_modalidade": "modalidade",
    "nome_modalidade": "nome_modalidade",
    "numero_processo": "numero_processo",
    "objeto": "objeto",
    "situacao": "situacao_aviso",
    "valor_total_estimado": "valor_estimado_total",
    "valor_total_homologado": "valor_homologado_total",
    "data_publicacao": "data_publicacao",
    "data_atualizacao": "dt_alteracao",
    "excluido": lit("false"),
}

LICITACOES = EntidadeSilver(
    nome="licitacoes",
    chave=("id_compra",),
    colunas=(
        Coluna("id_compra"),
        Coluna("sistema_origem"),
        Coluna("regime_legal"),
        Coluna("codigo_uasg", "uasg"),
        Coluna("codigo_modalidade"),
        Coluna("nome_modalidade", "texto"),
        Coluna("numero_processo", "texto"),
        Coluna("objeto", "texto"),
        Coluna("situacao", "texto"),
        Coluna("srp", "boolean"),
        Coluna("valor_total_estimado", "decimal(18,2)"),
        Coluna("valor_total_homologado", "decimal(18,2)"),
        Coluna("data_publicacao", "date"),
        Coluna("data_atualizacao", "timestamp"),
        Coluna("excluido", "boolean"),
    ),
    mapeamentos={
        "pncp_contratacoes": {
            "id_compra": "idCompra",
            "regime_legal": lit("14133"),
            "codigo_uasg": "unidadeOrgaoCodigoUnidade",
            "codigo_modalidade": "codigoModalidade",
            "nome_modalidade": "modalidadeNome",
            "numero_processo": "processo",
            "objeto": "objetoCompra",
            "situacao": "situacaoCompraNomePncp",
            "srp": "srp",
            "valor_total_estimado": "valorTotalEstimado",
            "valor_total_homologado": "valorTotalHomologado",
            "data_publicacao": "dataPublicacaoPncp",
            "data_atualizacao": ("dataAtualizacaoPncp", "dataInclusaoPncp"),
            "excluido": "contratacaoExcluida",
        },
        "legado_licitacoes": _LEGADO_LICITACAO,
        "legado_compras_sem_licitacao": {
            "id_compra": "id_compra",
            "regime_legal": regime_por_flag("pertence14133"),
            "codigo_uasg": "co_uasg",
            "codigo_modalidade": "co_modalidade_licitacao",
            "numero_processo": "nu_processo",
            "objeto": "ds_objeto_licitacao",
            "valor_total_estimado": "vr_estimado",
            "data_publicacao": "dt_publicacao",
            "data_atualizacao": "dt_alteracao",
            "excluido": lit("false"),
        },
        # CSV do portal: mesmos nomes do Legado (verificar a cada arquivo novo)
        "csv": {
            **_LEGADO_LICITACAO,
            "codigo_uasg": ("uasg", "co_uasg"),
            "codigo_modalidade": ("modalidade", "co_modalidade_licitacao"),
            "data_publicacao": ("data_publicacao", "dt_publicacao"),
            "data_atualizacao": ("dt_alteracao", "data_publicacao"),
        },
    },
    obrigatorias_origem={
        "pncp_contratacoes": ("idCompra", "dataPublicacaoPncp"),
        "legado_licitacoes": ("id_compra", "data_publicacao"),
        "legado_compras_sem_licitacao": ("id_compra", "dt_publicacao"),
    },
    prioridade={
        "pncp_contratacoes": 0,
        "legado_licitacoes": 1,
        "legado_compras_sem_licitacao": 1,
        "csv": 2,
    },
    data_evento="data_publicacao",
    obrigatorias=("id_compra", "data_publicacao", "codigo_uasg"),
    valores=("valor_total_estimado", "valor_total_homologado"),
    datas_nao_futuras=("data_publicacao",),
)

# ============================================================== itens (grão: item da compra)
ITENS_COMPRA = EntidadeSilver(
    nome="itens_compra",
    chave=("id_compra_item",),
    colunas=(
        Coluna("id_compra_item"),
        Coluna("id_compra"),
        Coluna("sistema_origem"),
        Coluna("regime_legal"),
        Coluna("numero_item", "int"),
        Coluna("tipo_item"),
        Coluna("codigo_item_catalogo"),
        Coluna("descricao", "texto"),
        Coluna("unidade", "texto"),
        Coluna("quantidade", "decimal(18,4)"),
        Coluna("valor_unitario_estimado", "decimal(18,4)"),
        Coluna("valor_total_estimado", "decimal(18,2)"),
        Coluna("quantidade_homologada", "decimal(18,4)"),
        Coluna("valor_unitario_homologado", "decimal(18,4)"),
        Coluna("valor_total_homologado", "decimal(18,2)"),
        Coluna("ni_fornecedor", "ni"),
        Coluna("nome_fornecedor", "texto"),
        Coluna("codigo_uasg", "uasg"),
        Coluna("codigo_modalidade"),
        Coluna("data_item", "date"),
        Coluna("data_resultado", "date"),
        Coluna("data_atualizacao", "timestamp"),
    ),
    mapeamentos={
        "pncp_itens": {
            "id_compra_item": "idCompraItem",
            "id_compra": "idCompra",
            "regime_legal": lit("14133"),
            "numero_item": ("numeroItemCompra", "numeroItemPncp"),
            "tipo_item": tipo_item(lambda f: f.c("materialOuServico", "materialOuServicoNome")),
            "codigo_item_catalogo": "codItemCatalogo",
            "descricao": ("descricaoResumida", "descricaodetalhada"),
            "unidade": "unidadeMedida",
            "quantidade": "quantidade",
            "valor_unitario_estimado": "valorUnitarioEstimado",
            "valor_total_estimado": "valorTotal",
            "quantidade_homologada": "quantidadeResultado",
            "valor_unitario_homologado": "valorUnitarioResultado",
            "valor_total_homologado": "valorTotalResultado",
            "ni_fornecedor": "codFornecedor",
            "nome_fornecedor": "nomeFornecedor",
            "codigo_uasg": "unidadeOrgaoCodigoUnidade",
            "data_item": "dataInclusaoPncp",
            "data_resultado": "dataResultado",
            "data_atualizacao": ("dataAtualizacaoPncp", "dataInclusaoPncp"),
        },
        "legado_itens_licitacao": {
            "id_compra_item": "id_compra_item",
            "id_compra": "id_compra",
            "regime_legal": lit("8666"),
            "numero_item": "numero_item_licitacao",
            "tipo_item": lambda f: F.when(f.c("codigo_item_material").isNotNull(), F.lit("M")).when(
                f.c("codigo_item_servico").isNotNull(), F.lit("S")
            ),
            "codigo_item_catalogo": ("codigo_item_material", "codigo_item_servico"),
            "descricao": ("descricao_item", "nome_material", "nome_servico"),
            "unidade": "unidade",
            "quantidade": "quantidade",
            "valor_unitario_estimado": "valor_estimado",
            "ni_fornecedor": ("cnpj_fornecedor", "cpf_vencedor"),
            "nome_fornecedor": ("nome_fornecedor", "nome_vencedor_pf"),
            "codigo_uasg": "uasg",
            "codigo_modalidade": "modalidade",
            "data_atualizacao": "dt_alteracao",
        },
        "legado_itens_sem_licitacao": {
            "id_compra_item": "id_compra_item",
            "id_compra": "id_compra",
            "regime_legal": lit("8666"),
            "numero_item": "nu_item_material",
            "tipo_item": tipo_item(lambda f: f.c("in_material_servico")),
            "codigo_item_catalogo": ("co_conjunto_materiais", "co_servico"),
            "descricao": ("ds_detalhada", "no_conjunto_materiais", "no_servico"),
            "unidade": "no_unidade_medida",
            "quantidade": "qt_material_alt",
            "valor_total_estimado": "vr_estimado_item",
            "ni_fornecedor": ("nu_cnpj_vencedor", "nu_cpf_vencedor"),
            "nome_fornecedor": "no_fornecedor_vencedor",
            "codigo_uasg": "co_uasg",
            "codigo_modalidade": "co_modalidade_licitacao",
            "data_item": "dt_publicacao",
            "data_atualizacao": "dt_alteracao",
        },
    },
    obrigatorias_origem={
        "pncp_itens": ("idCompraItem", "idCompra", "quantidade"),
        "legado_itens_licitacao": ("id_compra_item", "id_compra", "quantidade"),
        "legado_itens_sem_licitacao": ("id_compra_item", "id_compra"),
        # complemento: não vira linha, só completa o código do catálogo (transform.py)
        "pncp_itens_catalogo": ("numeroItem",),
    },
    prioridade={
        "pncp_itens": 0,
        "legado_itens_licitacao": 1,
        "legado_itens_sem_licitacao": 1,
        "csv": 2,
    },
    data_evento="data_item",
    obrigatorias=("id_compra_item", "id_compra"),
    valores=(
        "valor_unitario_estimado",
        "valor_total_estimado",
        "valor_unitario_homologado",
        "valor_total_homologado",
    ),
    datas_nao_futuras=("data_item", "data_resultado"),
    tem_fornecedor=True,
)

# ============================================================== contratos (grão: contrato)
CONTRATOS = EntidadeSilver(
    nome="contratos",
    chave=("id_contrato",),
    colunas=(
        Coluna("id_contrato"),
        Coluna("numero_contrato", "texto"),
        Coluna("sistema_origem"),
        Coluna("regime_legal"),
        Coluna("codigo_orgao", "texto"),
        Coluna("nome_orgao", "texto"),
        Coluna("codigo_uasg", "uasg"),
        Coluna("codigo_uasg_compra", "uasg"),
        Coluna("id_compra"),
        Coluna("numero_compra", "texto"),
        Coluna("codigo_modalidade"),
        Coluna("nome_modalidade", "texto"),
        Coluna("tipo_contrato", "texto"),
        Coluna("categoria", "texto"),
        Coluna("ni_fornecedor", "ni"),
        Coluna("nome_fornecedor", "texto"),
        Coluna("objeto", "texto"),
        Coluna("processo", "texto"),
        Coluna("data_vigencia_inicio", "date"),
        Coluna("data_vigencia_fim", "date"),
        Coluna("valor_global", "decimal(18,2)"),
        Coluna("valor_parcela", "decimal(18,2)"),
        Coluna("numero_parcelas", "int"),
        Coluna("valor_acumulado", "decimal(18,2)"),
        Coluna("data_inclusao", "timestamp"),
        Coluna("data_atualizacao", "timestamp"),
        Coluna("excluido", "boolean"),
    ),
    mapeamentos={
        "contratos": {
            # Chave natural: número de controle do PNCP; para contratos sem PNCP, a
            # combinação unidade gestora + tipo + número identifica o contrato.
            "id_contrato": lambda f: F.coalesce(
                f.c("numeroControlePncpContrato"),
                F.when(
                    f.c("numeroContrato").isNotNull(),
                    F.concat_ws(
                        "-",
                        F.coalesce(f.c("codigoUnidadeGestora"), F.lit("?")),
                        F.coalesce(f.c("codigoTipo"), F.lit("?")),
                        f.c("numeroContrato"),
                    ),
                ),
            ),
            "numero_contrato": "numeroContrato",
            "regime_legal": lambda f: F.when(
                f.c("numeroControlePncpCompra").isNotNull(), F.lit("14133")
            ).otherwise(F.lit("indefinido")),
            "codigo_orgao": "codigoOrgao",
            "nome_orgao": "nomeOrgao",
            "codigo_uasg": "codigoUnidadeGestora",
            "codigo_uasg_compra": "codigoUnidadeRealizadoraCompra",
            "id_compra": "idCompra",
            "numero_compra": "numeroCompra",
            "codigo_modalidade": "codigoModalidadeCompra",
            "nome_modalidade": "nomeModalidadeCompra",
            "tipo_contrato": "nomeTipo",
            "categoria": "nomeCategoria",
            "ni_fornecedor": "niFornecedor",
            "nome_fornecedor": "nomeRazaoSocialFornecedor",
            "objeto": "objeto",
            "processo": "processo",
            "data_vigencia_inicio": "dataVigenciaInicial",
            "data_vigencia_fim": "dataVigenciaFinal",
            "valor_global": "valorGlobal",
            "valor_parcela": "valorParcela",
            "numero_parcelas": "numeroParcelas",
            "valor_acumulado": "valorAcumulado",
            "data_inclusao": "dataHoraInclusao",
            "data_atualizacao": ("dataHoraExclusao", "dataHoraInclusao"),
            "excluido": "contratoExcluido",
        },
    },
    obrigatorias_origem={"contratos": ("numeroContrato", "dataVigenciaInicial", "valorGlobal")},
    prioridade={"contratos": 0, "csv": 2},
    data_evento="data_vigencia_inicio",
    obrigatorias=("id_contrato", "data_vigencia_inicio"),
    valores=("valor_global", "valor_parcela", "valor_acumulado"),
    datas_nao_futuras=("data_inclusao",),
    tem_fornecedor=True,
)

# ============================================================== fornecedores (cadastro)
FORNECEDORES = EntidadeSilver(
    nome="fornecedores",
    chave=("ni_fornecedor",),
    colunas=(
        Coluna("ni_fornecedor", "ni"),
        Coluna("sistema_origem"),
        Coluna("razao_social", "texto"),
        Coluna("porte_id", "int"),
        Coluna("porte_nome", "texto"),
        Coluna("natureza_juridica_id", "int"),
        Coluna("natureza_juridica_nome", "texto"),
        Coluna("uf", "texto"),
        Coluna("municipio", "texto"),
        Coluna("codigo_cnae", "int"),
        Coluna("nome_cnae", "texto"),
        Coluna("ativo", "boolean"),
        Coluna("habilitado_licitar", "boolean"),
        Coluna("data_atualizacao", "timestamp"),
    ),
    mapeamentos={
        "fornecedor": {
            "ni_fornecedor": ("cnpj", "cpf"),
            "razao_social": "nomeRazaoSocialFornecedor",
            "porte_id": "porteEmpresaId",
            "porte_nome": "porteEmpresaNome",
            "natureza_juridica_id": "naturezaJuridicaId",
            "natureza_juridica_nome": "naturezaJuridicaNome",
            "uf": "ufSigla",
            "municipio": "nomeMunicipio",
            "codigo_cnae": "codigoCnae",
            "nome_cnae": "nomeCnae",
            "ativo": "ativo",
            "habilitado_licitar": "habilitadoLicitar",
            # o cadastro não traz data de atualização: vale a data do snapshot
            "data_atualizacao": lambda f: f.c("data_referencia"),
        }
    },
    obrigatorias_origem={"fornecedor": ("nomeRazaoSocialFornecedor",)},
    prioridade={"fornecedor": 0},
    obrigatorias=("ni_fornecedor", "razao_social"),
    tem_fornecedor=True,
)

# ============================================================== órgãos (grão: UASG)
ORGAOS = EntidadeSilver(
    nome="orgaos",
    chave=("codigo_uasg",),
    colunas=(
        Coluna("codigo_uasg", "uasg"),
        Coluna("sistema_origem"),
        Coluna("nome_uasg", "texto"),
        Coluna("uf", "texto"),
        Coluna("municipio", "texto"),
        Coluna("codigo_orgao", "texto"),
        Coluna("uso_sisg", "boolean"),
        Coluna("ativo", "boolean"),
        Coluna("data_atualizacao", "timestamp"),
    ),
    mapeamentos={
        "uasg": {
            "codigo_uasg": "codigoUasg",
            "nome_uasg": "nomeUasg",
            "uf": "siglaUf",
            "municipio": "nomeMunicipioIbge",
            "codigo_orgao": "codigoOrgao",
            "uso_sisg": "usoSisg",
            "ativo": "statusUasg",
            "data_atualizacao": ("dataHoraMovimento", "data_referencia"),
        }
    },
    obrigatorias_origem={"uasg": ("codigoUasg", "nomeUasg")},
    prioridade={"uasg": 0},
    obrigatorias=("codigo_uasg",),
)

# Atributos do órgão (endpoint "orgao"), juntados à UASG dentro da própria entidade.
ORGAO_ATRIBUTOS: dict[str, Expr] = {
    "codigo_orgao": "codigoOrgao",
    "nome_orgao": "nomeOrgao",
    "codigo_orgao_superior": "codigoOrgaoSuperior",
    "nome_orgao_superior": "nomeOrgaoSuperior",
    "esfera": "esfera",
    "poder": "poder",
    "tipo_administracao": "nomeTipoAdministracao",
}

# ============================================================== catálogo CATMAT/CATSER
CATALOGO = EntidadeSilver(
    nome="catalogo",
    chave=("tipo_item", "codigo_item"),
    colunas=(
        Coluna("tipo_item"),
        Coluna("codigo_item"),
        Coluna("sistema_origem"),
        Coluna("descricao", "texto"),
        Coluna("codigo_grupo"),
        Coluna("nome_grupo", "texto"),
        Coluna("codigo_classe"),
        Coluna("nome_classe", "texto"),
        Coluna("codigo_subclasse_pdm"),
        Coluna("nome_subclasse_pdm", "texto"),
        Coluna("ativo", "boolean"),
        Coluna("data_atualizacao", "timestamp"),
    ),
    mapeamentos={
        "catmat": {
            "tipo_item": lit("M"),
            "codigo_item": "codigoItem",
            "descricao": "descricaoItem",
            "codigo_grupo": "codigoGrupo",
            "nome_grupo": "nomeGrupo",
            "codigo_classe": "codigoClasse",
            "nome_classe": "nomeClasse",
            "codigo_subclasse_pdm": "codigoPdm",
            "nome_subclasse_pdm": "nomePdm",
            "ativo": "statusItem",
            "data_atualizacao": ("dataHoraAtualizacao", "data_referencia"),
        },
        "catser": {
            "tipo_item": lit("S"),
            "codigo_item": "codigoServico",
            "descricao": "nomeServico",
            "codigo_grupo": "codigoGrupo",
            "nome_grupo": "nomeGrupo",
            "codigo_classe": "codigoClasse",
            "nome_classe": "nomeClasse",
            "codigo_subclasse_pdm": "codigoSubclasse",
            "nome_subclasse_pdm": "nomeSubclasse",
            "ativo": "statusServico",
            "data_atualizacao": ("dataHoraAtualizacao", "data_referencia"),
        },
    },
    obrigatorias_origem={"catmat": ("codigoItem",), "catser": ("codigoServico",)},
    prioridade={"catmat": 0, "catser": 0},
    obrigatorias=("codigo_item", "descricao"),
)

ENTIDADES_SILVER: dict[str, EntidadeSilver] = {
    e.nome: e for e in (LICITACOES, ITENS_COMPRA, CONTRATOS, FORNECEDORES, ORGAOS, CATALOGO)
}
