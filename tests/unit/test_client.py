import httpx
import pytest
import respx

from compras.ingestion.client import ApiError, ComprasClient

BASE = "https://api.teste"
PATH = "/modulo-x/1_consultar"


def _pagina(n, total, registros):
    return {
        "resultado": [{"id": f"{n}-{i}"} for i in range(registros)],
        "totalRegistros": total * registros,
        "totalPaginas": total,
        "paginasRestantes": total - n,
    }


@pytest.fixture
def client():
    esperas: list[float] = []
    c = ComprasClient(BASE, req_por_segundo=0, max_tentativas=4, sleep=esperas.append)
    c.esperas = esperas
    yield c
    c.close()


@respx.mock
def test_pagina_ate_total_paginas(client):
    rota = respx.get(f"{BASE}{PATH}").mock(
        side_effect=lambda req: httpx.Response(
            200, json=_pagina(int(req.url.params["pagina"]), 3, 2)
        )
    )
    paginas = list(client.paginas(PATH, {"data": "2026-10-01"}))
    assert [p.numero for p in paginas] == [1, 2, 3]
    assert sum(len(p.registros) for p in paginas) == 6
    assert rota.call_count == 3
    primeira = rota.calls[0].request.url.params
    assert primeira["tamanhoPagina"] == "500"
    assert primeira["data"] == "2026-10-01"


@respx.mock
def test_retoma_da_pagina_informada(client):
    rota = respx.get(f"{BASE}{PATH}").mock(
        side_effect=lambda req: httpx.Response(
            200, json=_pagina(int(req.url.params["pagina"]), 3, 1)
        )
    )
    paginas = list(client.paginas(PATH, {}, pagina_inicial=3))
    assert [p.numero for p in paginas] == [3]
    assert rota.call_count == 1


@respx.mock
def test_retry_em_429_respeita_retry_after(client):
    respx.get(f"{BASE}{PATH}").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(503),
            httpx.Response(200, json=_pagina(1, 1, 1)),
        ]
    )
    paginas = list(client.paginas(PATH, {}))
    assert len(paginas) == 1
    assert client.esperas[0] == 7.0  # Retry-After
    assert len(client.esperas) == 2


@respx.mock
def test_desiste_depois_do_maximo_de_tentativas(client):
    rota = respx.get(f"{BASE}{PATH}").mock(return_value=httpx.Response(500))
    with pytest.raises(ApiError):
        list(client.paginas(PATH, {}))
    assert rota.call_count == 4


@respx.mock
def test_erro_4xx_nao_tenta_de_novo(client):
    rota = respx.get(f"{BASE}{PATH}").mock(return_value=httpx.Response(400, text="parâmetro"))
    with pytest.raises(ApiError) as exc:
        list(client.paginas(PATH, {}))
    assert exc.value.status == 400
    assert rota.call_count == 1


@respx.mock
def test_400_de_instabilidade_da_api_tenta_de_novo(client):
    corpo = {"title": "Requisição Inválida", "status": 400, "detail": "Erro ao efetuar a consulta "}
    rota = respx.get(f"{BASE}{PATH}").mock(
        side_effect=[
            httpx.Response(400, json=corpo),
            httpx.Response(400, json=corpo),
            httpx.Response(200, json=_pagina(1, 1, 1)),
        ]
    )
    paginas = list(client.paginas(PATH, {}))
    assert len(paginas) == 1
    assert rota.call_count == 3


@respx.mock
def test_endpoint_nao_paginado_e_booleans(client):
    rota = respx.get(f"{BASE}{PATH}").mock(
        return_value=httpx.Response(200, json={"resultado": {"id": 1}})
    )
    paginas = list(client.paginas(PATH, {"ativo": True}, paginado=False))
    assert len(paginas) == 1 and paginas[0].registros == [{"id": 1}]
    params = rota.calls[0].request.url.params
    assert "pagina" not in params and params["ativo"] == "true"


@respx.mock
def test_sem_tamanho_pagina_para_por_lista_vazia(client):
    def resp(req):
        n = int(req.url.params["pagina"])
        assert "tamanhoPagina" not in req.url.params
        return httpx.Response(200, json={"resultado": [{"x": n}] if n < 3 else []})

    respx.get(f"{BASE}{PATH}").mock(side_effect=resp)
    paginas = list(client.paginas(PATH, {}, aceita_tamanho_pagina=False))
    assert [p.numero for p in paginas] == [1, 2, 3]
