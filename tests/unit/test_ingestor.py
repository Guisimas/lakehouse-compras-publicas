import json
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import respx

from compras.ingestion.client import ComprasClient
from compras.ingestion.control import Controle
from compras.ingestion.endpoints import ENDPOINTS, PNCP_API, endpoints_da_entidade
from compras.ingestion.ingestor import Ingestor
from compras.ingestion.landing import Landing, LandingFanout
from compras.ingestion.runner import janelas_de_varredura
from compras.ingestion.storage import LocalStorage

BASE = "https://api.teste"
J = date(2026, 10, 1)


@pytest.fixture
def ambiente(tmp_path):
    storage = LocalStorage()
    root = str(tmp_path / "landing").replace("\\", "/")
    landing = Landing(storage, root)
    controle = Controle(storage, root)
    client = ComprasClient(BASE, req_por_segundo=0, max_tentativas=2, sleep=lambda _s: None)
    yield landing, controle, client, Path(root)
    client.close()


def _ingestor(landing, controle, client, run_id="run-1"):
    return Ingestor(client, landing, controle, run_id, LandingFanout(landing))


def _paginado(total_paginas, por_pagina=2, campo="id"):
    def resp(req):
        n = int(req.url.params.get("pagina", 1))
        return httpx.Response(
            200,
            json={
                "resultado": [{campo: f"{n}-{i}"} for i in range(por_pagina)],
                "totalPaginas": total_paginas,
                "totalRegistros": total_paginas * por_pagina,
                "paginasRestantes": total_paginas - n,
            },
        )

    return resp


@respx.mock
def test_layout_deterministico_e_log(ambiente):
    landing, controle, client, root = ambiente
    respx.get(f"{BASE}/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133").mock(
        side_effect=_paginado(2)
    )
    reg = _ingestor(landing, controle, client).ingerir("itens_compra", J)
    assert reg.status == "sucesso"
    assert reg.paginas == 2 and reg.linhas == 4
    pasta = root / "itens_compra" / "data_referencia=2026-10-01"
    nomes = sorted(p.name for p in pasta.iterdir())
    assert nomes == [
        "_SUCCESS__pncp_itens__all.json",
        "pncp_itens__all__p00001.json",
        "pncp_itens__all__p00002.json",
    ]
    envelope = json.loads((pasta / "pncp_itens__all__p00001.json").read_text(encoding="utf-8"))
    assert envelope["meta"]["regime_legal"] == "14133"
    assert envelope["payload"]["resultado"][0] == {"id": "1-0"}  # conteúdo bruto preservado
    logs = controle.logs()
    assert len(logs) == 1 and logs[0]["status"] == "sucesso"
    assert controle.ler_watermark("itens_compra").ultima_janela == "2026-10-01"


@respx.mock
def test_idempotente_pula_janela_completa(ambiente):
    landing, controle, client, _ = ambiente
    rota = respx.get(f"{BASE}/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133").mock(
        side_effect=_paginado(2)
    )
    ing = _ingestor(landing, controle, client)
    ing.ingerir("itens_compra", J)
    chamadas = rota.call_count
    reg = ing.ingerir("itens_compra", J)
    assert rota.call_count == chamadas  # nada baixado de novo
    assert reg.fanouts_pulados == reg.fanouts == 1
    assert len(landing.registros("itens_compra", "2026-10-01")) == 4


@respx.mock
def test_forcar_baixa_de_novo_sem_duplicar(ambiente):
    landing, controle, client, _ = ambiente
    respx.get(f"{BASE}/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133").mock(
        side_effect=_paginado(3)
    )
    ing = _ingestor(landing, controle, client)
    ing.ingerir("itens_compra", J)
    ing.ingerir("itens_compra", J, forcar=True)
    assert len(landing.registros("itens_compra", "2026-10-01")) == 6


@respx.mock
def test_retoma_da_ultima_pagina_depois_de_falha(ambiente):
    landing, controle, client, _ = ambiente
    estado = {"falhar": True}

    def resp(req):
        n = int(req.url.params["pagina"])
        if n == 3 and estado["falhar"]:
            return httpx.Response(500)
        return _paginado(4)(req)

    rota = respx.get(f"{BASE}/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133").mock(
        side_effect=resp
    )
    ing = _ingestor(landing, controle, client)
    reg = ing.ingerir("itens_compra", J)
    assert reg.status == "falha" and "HTTP 500" in reg.erro
    assert controle.ler_watermark("itens_compra") is None
    assert len(landing.paginas_existentes("itens_compra", "2026-10-01", "pncp_itens", "all")) == 2

    estado["falhar"] = False
    antes = len(rota.calls)
    reg = ing.ingerir("itens_compra", J)
    assert reg.status == "sucesso"
    pedidas = [int(c.request.url.params["pagina"]) for c in list(rota.calls)[antes:]]
    assert pedidas == [3, 4]  # não baixou 1 e 2 de novo
    assert len(landing.registros("itens_compra", "2026-10-01")) == 8
    assert {r["status"] for r in controle.logs()} == {"falha", "sucesso"}


@respx.mock
def test_fanout_novo_nao_lista_a_pasta_da_janela(ambiente):
    # Num UC Volume, listar a pasta de uma varredura (milhares de arquivos) custa ~30 s.
    landing, controle, client, _ = ambiente
    listagens: list[str] = []
    original = landing.storage.list_files

    def list_files_contando(path):
        listagens.append(path)
        return original(path)

    landing.storage.list_files = list_files_contando
    respx.get(f"{BASE}/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133").mock(
        side_effect=_paginado(2)
    )
    reg = _ingestor(landing, controle, client).ingerir("itens_compra", J)
    assert reg.status == "sucesso" and reg.paginas == 2
    # Uma leitura da janela monta o fan-out do complemento do PNCP; nenhuma por fan-out.
    assert len([p for p in listagens if "itens_compra/data_referencia=" in p]) == 1


@respx.mock
def test_contratos_fanout_por_orgao_do_snapshot(ambiente):
    landing, controle, client, _ = ambiente
    respx.get(f"{BASE}/modulo-uasg/1_consultarUasg").mock(
        return_value=httpx.Response(
            200, json={"resultado": [{"codigoUasg": "153080"}], "totalPaginas": 1}
        )
    )
    respx.get(f"{BASE}/modulo-uasg/2_consultarOrgao").mock(
        return_value=httpx.Response(
            200,
            json={
                "resultado": [
                    {"codigoOrgao": 26000, "usoSisg": True},
                    {"codigoOrgao": 30000, "usoSisg": True},
                    {"codigoOrgao": 99999, "usoSisg": False},
                    {"codigoOrgao": 88888, "usoSisg": True, "esfera": "M"},
                ],
                "totalPaginas": 1,
            },
        )
    )
    contratos = respx.get(f"{BASE}/modulo-contratos/1_consultarContratos").mock(
        return_value=httpx.Response(200, json={"resultado": [], "totalPaginas": 0})
    )
    ing = _ingestor(landing, controle, client)
    assert ing.ingerir("orgaos", J).status == "sucesso"
    reg = ing.ingerir("contratos", J)
    assert reg.status == "sucesso" and reg.fanouts == 2
    orgaos = sorted(c.request.url.params["codigoOrgao"] for c in contratos.calls)
    assert orgaos == ["26000", "30000"]
    params = contratos.calls[0].request.url.params
    # varredura: 365 dias terminando na data de referência
    assert params["dataVigenciaInicialMin"] == "2025-10-02"
    assert params["dataVigenciaInicialMax"] == "2026-10-01"


@respx.mock
def test_fornecedores_consulta_cnpjs_validos_da_janela(ambiente):
    landing, controle, client, _ = ambiente
    landing.gravar_pagina(
        "contratos",
        "2026-10-01",
        "contratos",
        "org1",
        1,
        {
            "meta": {},
            "payload": {
                "resultado": [
                    {"niFornecedor": "11.222.333/0001-81"},
                    {"niFornecedor": "11222333000182"},  # DV inválido: não consulta
                    {"niFornecedor": "12345678909"},  # CPF: não consulta
                ]
            },
        },
    )
    rota = respx.get(f"{BASE}/modulo-fornecedor/1_consultarFornecedor").mock(
        return_value=httpx.Response(
            200, json={"resultado": [{"cnpj": "11222333000181"}], "totalPaginas": 1}
        )
    )
    reg = _ingestor(landing, controle, client).ingerir("fornecedores", J)
    assert reg.status == "sucesso"
    assert [c.request.url.params["cnpj"] for c in rota.calls] == ["11222333000181"]


def test_registro_de_endpoints_consistente():
    nomes = [e.nome for e in ENDPOINTS]
    assert len(nomes) == len(set(nomes))
    pncp = next(e for e in ENDPOINTS if e.nome == "pncp_contratacoes")
    params = pncp.params(J, None)
    assert [p["codigoModalidade"] for _, p in params] == [3, 5, 6, 7]  # códigos do SIASG
    assert [f for f, _ in params] == ["mod03", "mod05", "mod06", "mod07"]
    legado = next(e for e in ENDPOINTS if e.nome == "legado_licitacoes")
    assert legado.params(J, None)[0][1]["pertence14133"] is False
    # endpoints anuais do Legado ficam fora do modo diário
    assert {e.nome for e in endpoints_da_entidade("licitacoes", "incremental")} == {
        "pncp_contratacoes",
        "legado_licitacoes",
    }
    assert "legado_compras_sem_licitacao" in {
        e.nome for e in endpoints_da_entidade("licitacoes", "historico")
    }


def test_varredura_de_contratos_e_semanal(ambiente):
    landing, controle, client, _ = ambiente
    ing = _ingestor(landing, controle, client)
    semana = [J - timedelta(days=d) for d in range(7)]
    # nunca rodou: varre até a janela mais recente
    assert janelas_de_varredura(ing, "contratos", semana, "incremental") == [J]
    controle.atualizar_watermark("contratos", J, "run-1", datetime(2026, 10, 1, 12))
    # 6 dias depois ainda vale; no 7º dia, varre de novo
    assert janelas_de_varredura(ing, "contratos", [J + timedelta(days=6)], "incremental") == []
    assert janelas_de_varredura(ing, "contratos", [J + timedelta(days=7)], "incremental") == [
        J + timedelta(days=7)
    ]
    # --forcar-snapshot varre mesmo dentro do prazo
    assert janelas_de_varredura(ing, "contratos", [J], "incremental", forcar=True) == [J]


def test_backfill_de_contratos_em_blocos_de_365_dias(ambiente):
    landing, controle, client, _ = ambiente
    ing = _ingestor(landing, controle, client)
    dois_anos = [date(2024, 11, 1) + timedelta(days=d) for d in range(731)]
    refs = janelas_de_varredura(ing, "contratos", dois_anos, "historico")
    assert refs == [date(2024, 11, 1), date(2025, 11, 1), date(2026, 11, 1)]
    # cada bloco cobre 365 dias até a referência: juntos, cobrem o intervalo inteiro
    assert refs[0] - timedelta(days=364) <= min(dois_anos)


def test_fornecedores_da_varredura_so_de_contratos_novos(tmp_path):
    root = str(tmp_path / "landing").replace("\\", "/")
    landing = Landing(LocalStorage(), root)
    a, b, c, d = "11222333000181", "04252011000110", "33000167000101", "00000000000191"

    def varredura(chave, contratos):
        envelope = {"meta": {}, "payload": {"resultado": contratos}}
        landing.gravar_pagina("contratos", chave, "contratos", "org1", 1, envelope)

    varredura("2026-09-24", [{"niFornecedor": a, "dataHoraInclusao": "2026-09-01T10:00:00"}])
    varredura(
        "2026-10-01",
        [
            {"niFornecedor": a, "dataHoraInclusao": "2026-09-01T10:00:00"},  # já estava
            {"niFornecedor": b, "dataHoraInclusao": "2026-09-20T08:00:00"},  # novo
            {"niFornecedor": c},  # sem data de inclusão: entra
            {"niFornecedor": d, "dataHoraInclusao": "2026-09-30T08:00:00"},  # novo, mas...
        ],
    )
    # ... d teve o cadastro consultado há 6 dias
    landing.marcar_sucesso("fornecedores", "2026-09-25", "fornecedor", f"cnpj{d}", {})

    incremental = LandingFanout(landing, contratos_incrementais=True)
    assert incremental.cnpjs_fornecedores(J) == sorted([b, c])
    # primeira varredura: nenhum contrato conta (fornecedores viram membros inferidos)
    assert incremental.cnpjs_fornecedores(date(2026, 9, 24)) == []
    # backfill: todos os contratos, menos quem foi consultado nos últimos 30 dias
    assert LandingFanout(landing).cnpjs_fornecedores(J) == sorted([a, b, c])


def test_fornecedores_dos_itens_nao_repetem_consulta_recente(tmp_path):
    root = str(tmp_path / "landing").replace("\\", "/")
    landing = Landing(LocalStorage(), root)
    a, b = "11222333000181", "04252011000110"
    envelope = {"meta": {}, "payload": {"resultado": [{"codFornecedor": a}, {"codFornecedor": b}]}}
    landing.gravar_pagina("itens_compra", "2026-10-01", "pncp_itens", "all", 1, envelope)
    landing.marcar_sucesso("fornecedores", "2026-09-10", "fornecedor", f"cnpj{a}", {})
    landing.marcar_sucesso("fornecedores", "2026-08-01", "fornecedor", f"cnpj{b}", {})
    # a foi consultado há 21 dias; b há 61, então volta a ser consultado
    assert LandingFanout(landing).cnpjs_fornecedores(J) == [b]


ITENS_PNCP = f"{BASE}/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133"
URL_COMPRA_PNCP = f"{PNCP_API}/v1/orgaos/10637926000146/compras/2026/290/itens"


def _itens_sem_codigo(_req):
    base = {"idContratacaoPNCP": "10637926000146-1-000290/2026", "codItemCatalogo": None}
    com_codigo = {"idContratacaoPNCP": "11111111000111-1-000001/2026", "codItemCatalogo": 9}
    return httpx.Response(
        200,
        json={
            "resultado": [{**base, "numeroItemPncp": 1}, {**base, "numeroItemPncp": 2}, com_codigo],
            "totalPaginas": 1,
        },
    )


@respx.mock
def test_complemento_do_pncp_so_para_compras_sem_codigo(ambiente):
    landing, controle, client, root = ambiente
    respx.get(ITENS_PNCP).mock(side_effect=_itens_sem_codigo)
    rota = respx.get(URL_COMPRA_PNCP).mock(
        return_value=httpx.Response(200, json=[{"numeroItem": 1, "catalogoCodigoItem": "15156"}])
    )
    reg = _ingestor(landing, controle, client).ingerir("itens_compra", J)
    assert reg.status == "sucesso"
    assert rota.call_count == 1  # uma compra sem código; lista incompleta encerra
    assert rota.calls[0].request.url.params["pagina"] == "1"
    pasta = root / "itens_compra" / "data_referencia=2026-10-01"
    nome = "pncp_itens_catalogo__10637926000146-2026-290__p00001.json"
    envelope = json.loads((pasta / nome).read_text(encoding="utf-8"))
    assert envelope["meta"]["path"] == URL_COMPRA_PNCP
    assert envelope["payload"] == [{"numeroItem": 1, "catalogoCodigoItem": "15156"}]


@respx.mock
def test_complemento_fora_do_ar_nao_derruba_os_itens(ambiente):
    landing, controle, client, root = ambiente
    respx.get(ITENS_PNCP).mock(side_effect=_itens_sem_codigo)
    rota = respx.get(URL_COMPRA_PNCP).mock(return_value=httpx.Response(503))
    reg = _ingestor(landing, controle, client).ingerir("itens_compra", J)
    assert reg.status == "sucesso"
    assert any("aviso" in d for d in reg.detalhes)
    pasta = root / "itens_compra" / "data_referencia=2026-10-01"
    assert not (pasta / "_SUCCESS__pncp_itens_catalogo__10637926000146-2026-290.json").exists()

    # próximo run: os itens são pulados e só o complemento é tentado de novo
    rota.mock(return_value=httpx.Response(200, json=[]))
    reg2 = _ingestor(landing, controle, client, run_id="run-2").ingerir("itens_compra", J)
    assert reg2.status == "sucesso"
    assert (pasta / "_SUCCESS__pncp_itens_catalogo__10637926000146-2026-290.json").exists()


@respx.mock
def test_refazer_baixa_de_novo_so_os_endpoints_principais(ambiente):
    """Dia baixado vazio (a fonte publica itens com atraso) volta no reprocessamento."""
    landing, controle, client, _ = ambiente
    rota_itens = respx.get(ITENS_PNCP).mock(
        return_value=httpx.Response(200, json={"resultado": [], "totalPaginas": 0})
    )
    rota_compra = respx.get(URL_COMPRA_PNCP).mock(
        return_value=httpx.Response(200, json=[{"numeroItem": 1, "catalogoCodigoItem": "15156"}])
    )
    ing = _ingestor(landing, controle, client)
    ing.ingerir("itens_compra", J)
    assert landing.registros("itens_compra", "2026-10-01") == []

    # 14 dias depois os itens chegaram; sem refazer, o _SUCCESS do dia vazio barraria
    rota_itens.mock(side_effect=_itens_sem_codigo)
    assert ing.ingerir("itens_compra", J).linhas == 0
    reg = ing.ingerir("itens_compra", J, refazer=True)
    assert reg.status == "sucesso" and reg.linhas > 0
    assert rota_compra.call_count == 1

    # um novo reprocessamento baixa os itens de novo, mas não repete o complemento
    ing.ingerir("itens_compra", J, refazer=True)
    assert rota_compra.call_count == 1
