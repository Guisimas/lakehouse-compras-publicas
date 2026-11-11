"""Respostas fixas da API (amostra versionada), no formato exato dos endpoints.

Cenário da janela 2026-10-01, pensado para exercitar cada regra:

- licitações: 2 do PNCP válidas, 1 do Legado válida, 1 sem id (DQ-02), 1 com valor
  negativo (DQ-04);
- itens: item 1 duplicado (versão antiga + nova → dedup), item com CPF (mascarado), item
  com CNPJ de DV inválido (DQ-03), item com quantidade 0 (DQ-05), item cuja compra não
  existe (DQ-11 na Gold) e item do Legado;
- contratos: 1 válido e 1 com fim de vigência antes do início (DQ-07);
- fornecedor: Papelaria Exemplo (ME) em 2026-10-01; vira EPP em 2026-10-02 (SCD2).
"""

from __future__ import annotations

CNPJ_OK = "11222333000181"
CNPJ_DV_ERRADO = "11222333000182"
ID_ESTRANGEIRO = "US094788608"  # formato real visto na API (fornecedor estrangeiro)
COMPRA_PREGAO = "15308005900012026"
COMPRA_DISPENSA = "20000508000022026"
COMPRA_LEGADO = "15308005000032026"

UASG = [
    {
        "codigoUasg": "153080",
        "nomeUasg": "UNIVERSIDADE FEDERAL X",
        "siglaUf": "DF",
        "nomeMunicipioIbge": "Brasília",
        "codigoOrgao": 26000,
        "usoSisg": True,
        "statusUasg": True,
        "dataHoraMovimento": "2026-09-01T10:00:00",
    },
    {
        "codigoUasg": "200005",
        "nomeUasg": "SUPERINTENDENCIA Y",
        "siglaUf": "SP",
        "nomeMunicipioIbge": "São Paulo",
        "codigoOrgao": 30000,
        "usoSisg": True,
        "statusUasg": True,
        "dataHoraMovimento": "2026-09-01T10:00:00",
    },
]
ORGAO = [
    {
        "codigoOrgao": 26000,
        "nomeOrgao": "MINISTERIO DA EDUCACAO",
        "codigoOrgaoSuperior": 26000,
        "nomeOrgaoSuperior": "MINISTERIO DA EDUCACAO",
        "esfera": "F",
        "poder": "E",
        "nomeTipoAdministracao": "Administração Direta",
        "usoSisg": True,
        "statusOrgao": True,
    },
    {
        "codigoOrgao": 30000,
        "nomeOrgao": "MINISTERIO DA JUSTICA",
        "codigoOrgaoSuperior": 30000,
        "nomeOrgaoSuperior": "MINISTERIO DA JUSTICA",
        "esfera": "F",
        "poder": "E",
        "nomeTipoAdministracao": "Administração Direta",
        "usoSisg": True,
        "statusOrgao": True,
    },
]
CATMAT = [
    {
        "codigoItem": 150000,
        "descricaoItem": "PAPEL A4",
        "codigoGrupo": 75,
        "nomeGrupo": "UTENSILIOS",
        "codigoClasse": 7510,
        "nomeClasse": "MATERIAL DE ESCRITORIO",
        "codigoPdm": 1,
        "nomePdm": "PAPEL",
        "statusItem": True,
        "dataHoraAtualizacao": "2026-01-01T00:00:00",
    },
]
CATSER = [
    {
        "codigoServico": 5000,
        "nomeServico": "LIMPEZA PREDIAL",
        "codigoGrupo": 85,
        "nomeGrupo": "LIMPEZA",
        "codigoClasse": 851,
        "nomeClasse": "CONSERVACAO",
        "codigoSubclasse": 8511,
        "nomeSubclasse": "LIMPEZA",
        "statusServico": True,
        "dataHoraAtualizacao": "2026-01-01T00:00:00",
    },
]


def _pncp_compra(id_compra, uasg, mod, nome, est, hom, **extra):
    base = {
        "idCompra": id_compra,
        "unidadeOrgaoCodigoUnidade": uasg,
        "codigoModalidade": mod,
        "modalidadeNome": nome,
        "processo": "1/2026",
        "objetoCompra": "Objeto",
        "srp": False,
        "situacaoCompraNomePncp": "Divulgada no PNCP",
        "valorTotalEstimado": est,
        "valorTotalHomologado": hom,
        "dataPublicacaoPncp": "2026-10-01T09:00:00",
        "dataAtualizacaoPncp": "2026-10-01T09:00:00",
        "contratacaoExcluida": False,
    }
    base.update(extra)
    return base


# Códigos do SIASG, como a API devolve: 5 = Pregão, 6 = Dispensa.
PNCP_CONTRATACOES_MOD05 = [
    _pncp_compra(COMPRA_PREGAO, "153080", 5, "Pregão - Eletrônico", 1000.0, 900.0),
    _pncp_compra(None, "153080", 5, "Pregão - Eletrônico", 10.0, None),  # DQ-02
]
PNCP_CONTRATACOES_MOD06 = [
    _pncp_compra(COMPRA_DISPENSA, "200005", 6, "Dispensa", 500.0, None),
    _pncp_compra("20000508000992026", "200005", 6, "Dispensa", -5.0, None),  # DQ-04
    # DQ-04 (teto): erro de digitação na fonte, como o empenho real de R$ 2,8 trilhões
    _pncp_compra("20000508000993026", "200005", 6, "Dispensa", 2809000641300.01, None),
]
LEGADO_LICITACOES = [
    {
        "id_compra": COMPRA_LEGADO,
        "uasg": 153080,
        "modalidade": 5,
        "nome_modalidade": "Pregão",
        "numero_processo": "2/2026",
        "objeto": "Cadeiras",
        "situacao_aviso": "Publicado",
        "valor_estimado_total": 2000.0,
        "valor_homologado_total": None,
        "data_publicacao": "2026-10-01",
        "dt_alteracao": "2026-10-01T12:00:00",
        "pertence14133": False,
    },
]


def _pncp_item(
    id_item,
    id_compra,
    uasg,
    tipo,
    cod,
    qtd,
    unit,
    total,
    forn=None,
    nome=None,
    res_qtd=None,
    res_unit=None,
    res_total=None,
    atualizado="2026-10-01T09:00:00",
):
    return {
        "idCompraItem": id_item,
        "idCompra": id_compra,
        "unidadeOrgaoCodigoUnidade": uasg,
        "numeroItemCompra": 1,
        "materialOuServico": tipo,
        "codItemCatalogo": cod,
        "descricaoResumida": "Item",
        "unidadeMedida": "UN",
        "quantidade": qtd,
        "valorUnitarioEstimado": unit,
        "valorTotal": total,
        "codFornecedor": forn,
        "nomeFornecedor": nome,
        "quantidadeResultado": res_qtd,
        "valorUnitarioResultado": res_unit,
        "valorTotalResultado": res_total,
        "dataInclusaoPncp": "2026-10-01T09:00:00",
        "dataAtualizacaoPncp": atualizado,
        "dataResultado": "2026-10-01" if res_total is not None else None,
    }


PNCP_ITENS = [
    # versão antiga do item 1 (sem resultado) — descartada no dedup
    _pncp_item(
        f"{COMPRA_PREGAO}-1",
        COMPRA_PREGAO,
        "153080",
        "M",
        150000,
        100,
        10.0,
        1000.0,
        atualizado="2026-10-01T08:00:00",
    ),
    _pncp_item(
        f"{COMPRA_PREGAO}-1",
        COMPRA_PREGAO,
        "153080",
        "M",
        150000,
        100,
        10.0,
        1000.0,
        forn=CNPJ_OK,
        nome="PAPELARIA EXEMPLO LTDA",
        res_qtd=100,
        res_unit=9.0,
        res_total=900.0,
        atualizado="2026-10-01T18:00:00",
    ),
    _pncp_item(
        f"{COMPRA_DISPENSA}-1",
        COMPRA_DISPENSA,
        "200005",
        "S",
        5000,
        1,
        500.0,
        500.0,
        forn="123.456.789-09",
        nome="JOAO DA SILVA",
    ),
    _pncp_item(
        f"{COMPRA_PREGAO}-2",
        COMPRA_PREGAO,
        "153080",
        "M",
        150000,
        5,
        1.0,
        5.0,
        forn=CNPJ_DV_ERRADO,
        nome="CNPJ INVALIDO",
    ),  # DQ-03
    _pncp_item(f"{COMPRA_PREGAO}-3", COMPRA_PREGAO, "153080", "M", 150000, 0, 1.0, 0.0),  # DQ-05
    _pncp_item(
        f"{COMPRA_PREGAO}-4",
        COMPRA_PREGAO,
        "153080",
        "M",
        150000,
        1,
        100.0,
        100.0,
        forn=ID_ESTRANGEIRO,
        nome="IMMUNO AMERICAN CORP",
        res_qtd=1,
        res_unit=100.0,
        res_total=100.0,
    ),  # fornecedor estrangeiro: passa, com sk_fornecedor = -1
    _pncp_item("99999999999999999-1", "99999999999999999", "153080", "M", 150000, 2, 3.0, 6.0),
]
LEGADO_ITENS = [
    {
        "id_compra_item": f"{COMPRA_LEGADO}-1",
        "id_compra": COMPRA_LEGADO,
        "numero_item_licitacao": 1,
        "codigo_item_material": 150000,
        "nome_material": "CADEIRA",
        "descricao_item": "Cadeira giratória",
        "unidade": "UN",
        "quantidade": 10,
        "valor_estimado": 200.0,
        "cnpj_fornecedor": None,
        "uasg": 153080,
        "modalidade": 5,
        "dt_alteracao": "2026-10-01T12:00:00",
    },
]
CONTRATOS = [
    {
        "codigoOrgao": "26000",
        "nomeOrgao": "MINISTERIO DA EDUCACAO",
        "codigoUnidadeGestora": "153080",
        "numeroContrato": "00010/2026",
        "codigoTipo": "50",
        "nomeTipo": "Contrato",
        "nomeCategoria": "Compras",
        "niFornecedor": CNPJ_OK,
        "nomeRazaoSocialFornecedor": "PAPELARIA EXEMPLO LTDA",
        "objeto": "Papel",
        "codigoModalidadeCompra": "05",
        "nomeModalidadeCompra": "Pregão",
        "dataVigenciaInicial": "2026-10-01",
        "dataVigenciaFinal": "2027-09-30",
        "valorGlobal": 900.0,
        "numeroParcelas": 12,
        "valorParcela": 75.0,
        "valorAcumulado": 900.0,
        "dataHoraInclusao": "2026-10-01T15:00:00",
        "numeroControlePncpContrato": "00394445000101-2-000010/2026",
        "numeroControlePncpCompra": "00394445000101-1-000001/2026",
        "idCompra": COMPRA_PREGAO,
        "contratoExcluido": False,
    },
    {
        "codigoOrgao": "26000",
        "nomeOrgao": "MINISTERIO DA EDUCACAO",
        "codigoUnidadeGestora": "153080",
        "numeroContrato": "00011/2026",
        "codigoTipo": "50",
        "nomeTipo": "Contrato",
        "niFornecedor": CNPJ_OK,
        "dataVigenciaInicial": "2026-10-01",
        "dataVigenciaFinal": "2026-01-01",
        "valorGlobal": 10.0,  # DQ-07
        "dataHoraInclusao": "2026-10-01T15:00:00",
        "contratoExcluido": False,
    },
]


def fornecedor(porte: str) -> list[dict]:
    return [
        {
            "ativo": True,
            "cnpj": CNPJ_OK,
            "habilitadoLicitar": True,
            "codigoCnae": 4761003,
            "nomeCnae": "Papelaria",
            "nomeMunicipio": "Brasília",
            "naturezaJuridicaId": 2062,
            "naturezaJuridicaNome": "Sociedade Limitada",
            "porteEmpresaId": 1 if porte == "ME" else 3,
            "porteEmpresaNome": porte,
            "nomeRazaoSocialFornecedor": "PAPELARIA EXEMPLO LTDA",
            "ufSigla": "DF",
        }
    ]


# Item do PNCP com o código do catálogo vazio (como vem desde mai/2026) e o complemento da
# API do PNCP, que traz o código por compra.
ID_CONTRATACAO_PNCP = "00394445000101-1-000012/2026"
FANOUT_COMPRA_PNCP = "00394445000101-2026-12"
PNCP_ITEM_SEM_CODIGO = [
    {
        **_pncp_item(f"{COMPRA_PREGAO}-5", COMPRA_PREGAO, "153080", "M", None, 10, 5.0, 50.0),
        "idContratacaoPNCP": ID_CONTRATACAO_PNCP,
        "numeroItemPncp": 5,
    }
]
PNCP_ITENS_CATALOGO = [
    {"numeroItem": 4, "materialOuServico": "M", "catalogoCodigoItem": "999999"},
    {"numeroItem": 5, "materialOuServico": "M", "catalogoCodigoItem": "150000"},
]
JANELA_1_CATALOGO_PNCP = [
    ("itens_compra", "2026-10-01", "pncp_itens", "sem-codigo", PNCP_ITEM_SEM_CODIGO),
    ("itens_compra", "2026-10-01", "pncp_itens_catalogo", FANOUT_COMPRA_PNCP, PNCP_ITENS_CATALOGO),
]

# (entidade, chave_janela, endpoint, fanout, registros)
JANELA_1 = [
    ("orgaos", "2026-10-01", "uasg", "all", UASG),
    ("orgaos", "2026-10-01", "orgao", "all", ORGAO),
    ("catalogo", "2026-10-01", "catmat", "all", CATMAT),
    ("catalogo", "2026-10-01", "catser", "all", CATSER),
    ("licitacoes", "2026-10-01", "pncp_contratacoes", "mod05", PNCP_CONTRATACOES_MOD05),
    ("licitacoes", "2026-10-01", "pncp_contratacoes", "mod06", PNCP_CONTRATACOES_MOD06),
    ("licitacoes", "2026-10-01", "legado_licitacoes", "all", LEGADO_LICITACOES),
    ("itens_compra", "2026-10-01", "pncp_itens", "all", PNCP_ITENS),
    ("itens_compra", "2026-10-01", "legado_itens_licitacao", f"id{COMPRA_LEGADO}", LEGADO_ITENS),
    ("contratos", "2026-10-01", "contratos", "org26000", CONTRATOS),
    ("fornecedores", "2026-10-01", "fornecedor", f"cnpj{CNPJ_OK}", fornecedor("ME")),
]
JANELA_2_FORNECEDOR = [
    ("fornecedores", "2026-10-02", "fornecedor", f"cnpj{CNPJ_OK}", fornecedor("EPP")),
]
