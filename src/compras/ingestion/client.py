"""Cliente HTTP da API de Dados Abertos do Compras.gov.br.

Responsabilidades:
- paginação por ``pagina``/``tamanhoPagina``, parando por ``totalPaginas``/``paginasRestantes``;
- retry com backoff exponencial em 429, 5xx e erros de transporte, respeitando ``Retry-After``;
- limite de requisições por segundo;
- início em qualquer página, para retomar um download interrompido.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = logging.getLogger(__name__)

STATUS_RETENTAVEIS = {408, 425, 429, 500, 502, 503, 504}


class ApiError(RuntimeError):
    def __init__(self, mensagem: str, status: int | None = None):
        super().__init__(mensagem)
        self.status = status


class ErroRetentavel(ApiError):
    def __init__(self, mensagem: str, status: int | None = None, retry_after: float | None = None):
        super().__init__(mensagem, status)
        self.retry_after = retry_after


@dataclass(frozen=True)
class Pagina:
    numero: int
    payload: Any  # resposta bruta, sem nenhuma transformação
    registros: list[dict]
    total_paginas: int | None
    total_registros: int | None
    url: str

    @property
    def ultima(self) -> bool:
        if self.total_paginas is not None:
            return self.numero >= self.total_paginas
        return not self.registros


class LimitadorTaxa:
    """Garante um intervalo mínimo entre requisições (thread-safe)."""

    def __init__(self, req_por_segundo: float, sleep: Callable[[float], None] = time.sleep):
        self._intervalo = 1.0 / req_por_segundo if req_por_segundo > 0 else 0.0
        self._proxima = 0.0
        self._lock = threading.Lock()
        self._sleep = sleep

    def aguardar(self) -> None:
        with self._lock:
            agora = time.monotonic()
            espera = self._proxima - agora
            if espera > 0:
                self._sleep(espera)
                agora += espera
            self._proxima = agora + self._intervalo


def _extrair_registros(payload: Any) -> tuple[list[dict], int | None, int | None, int | None]:
    if isinstance(payload, list):
        return payload, None, None, None
    if isinstance(payload, dict):
        registros = payload.get("resultado")
        if registros is None:
            registros = []
        elif isinstance(registros, dict):
            registros = [registros]
        return (
            registros,
            payload.get("totalPaginas"),
            payload.get("totalRegistros"),
            payload.get("paginasRestantes"),
        )
    raise ApiError(f"Formato de resposta inesperado: {type(payload).__name__}")


class ComprasClient:
    def __init__(
        self,
        base_url: str = "https://dadosabertos.compras.gov.br",
        *,
        tamanho_pagina: int = 500,
        req_por_segundo: float = 2.0,
        max_tentativas: int = 6,
        timeout_s: float = 60.0,
        espera_max_s: float = 120.0,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.tamanho_pagina = max(10, min(500, tamanho_pagina))
        self._limitador = LimitadorTaxa(req_por_segundo, sleep)
        self._sleep = sleep
        self._max_tentativas = max_tentativas
        self._espera_exp = wait_exponential_jitter(initial=1, max=espera_max_s)
        self._http = httpx.Client(
            base_url=base_url,
            timeout=timeout_s,
            transport=transport,
            headers={"Accept": "application/json", "User-Agent": "compras-lakehouse/0.1"},
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> ComprasClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------ requisição única
    def _espera(self, estado: RetryCallState) -> float:
        exc = estado.outcome.exception() if estado.outcome else None
        if isinstance(exc, ErroRetentavel) and exc.retry_after is not None:
            return exc.retry_after
        return self._espera_exp(estado)

    def _get_uma_vez(self, path: str, params: dict[str, Any]) -> tuple[Any, str]:
        self._limitador.aguardar()
        resp = self._http.get(path, params=params)
        if resp.status_code in STATUS_RETENTAVEIS:
            retry_after = resp.headers.get("Retry-After")
            raise ErroRetentavel(
                f"HTTP {resp.status_code} em {resp.request.url}",
                resp.status_code,
                float(retry_after) if retry_after and retry_after.isdigit() else None,
            )
        if resp.status_code == 204:
            return {"resultado": [], "totalPaginas": 0, "totalRegistros": 0}, str(resp.url)
        if resp.status_code >= 400:
            raise ApiError(
                f"HTTP {resp.status_code} em {resp.request.url}: {resp.text[:300]}",
                resp.status_code,
            )
        try:
            return resp.json(), str(resp.url)
        except ValueError as exc:  # corpo truncado/HTML de erro: vale tentar de novo
            raise ErroRetentavel(f"JSON inválido em {resp.request.url}") from exc

    def get(self, path: str, params: dict[str, Any]) -> tuple[Any, str]:
        retrying = Retrying(
            stop=stop_after_attempt(self._max_tentativas),
            wait=self._espera,
            retry=retry_if_exception_type((ErroRetentavel, httpx.TransportError)),
            sleep=self._sleep,
            reraise=True,
            before_sleep=lambda s: log.warning(
                "Tentativa %s falhou (%s); tentando de novo",
                s.attempt_number,
                s.outcome.exception(),
            ),
        )
        return retrying(self._get_uma_vez, path, params)

    # ------------------------------------------------------------------ paginação
    def paginas(
        self,
        path: str,
        params: dict[str, Any],
        *,
        paginado: bool = True,
        aceita_tamanho_pagina: bool = True,
        pagina_inicial: int = 1,
    ) -> Iterator[Pagina]:
        """Itera as páginas a partir de ``pagina_inicial`` até a última."""
        numero = pagina_inicial
        while True:
            p = {k: _formatar(v) for k, v in params.items() if v is not None}
            if paginado:
                p["pagina"] = numero
                if aceita_tamanho_pagina:
                    p["tamanhoPagina"] = self.tamanho_pagina
            payload, url = self.get(path, p)
            registros, total_paginas, total_registros, restantes = _extrair_registros(payload)
            if total_paginas is None and restantes is not None:
                total_paginas = numero + int(restantes)
            pagina = Pagina(numero, payload, registros, total_paginas, total_registros, url)
            yield pagina
            if not paginado or pagina.ultima or not registros:
                return
            numero += 1


def _formatar(valor: Any) -> Any:
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if hasattr(valor, "isoformat"):
        return valor.isoformat()
    return valor
