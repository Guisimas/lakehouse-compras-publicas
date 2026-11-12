"""Configuração central: nomes de catálogo, schemas, volume e parâmetros da ingestão.

Nada de nome de tabela hardcoded fora daqui. Os valores vêm de variáveis de ambiente
(prefixo ``COMPRAS_``) ou de parâmetros de task do Databricks, que os tasks repassam.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

ENTIDADES_FATOS = ("licitacoes", "itens_compra", "contratos")
ENTIDADES_REFERENCIA = ("orgaos", "catalogo", "fornecedores")
ENTIDADES = ENTIDADES_REFERENCIA + ENTIDADES_FATOS
# Entidades com quarentena própria. As referências também validam: orgaos/catalogo
# reaproveitam o mesmo mecanismo das quatro principais.
ENTIDADES_QUARENTENA = ENTIDADES


def _env(nome: str, padrao: str) -> str:
    return os.environ.get(f"COMPRAS_{nome}", padrao)


@dataclass(frozen=True)
class Settings:
    catalog: str = field(default_factory=lambda: _env("CATALOG", "dev_compras"))
    # Raiz do landing. No Databricks: /Volumes/<catalog>/landing/raw. Local: ./data/landing
    landing_root: str = field(default_factory=lambda: _env("LANDING_ROOT", ""))
    api_base_url: str = field(
        default_factory=lambda: _env("API_BASE_URL", "https://dadosabertos.compras.gov.br")
    )
    tamanho_pagina: int = field(default_factory=lambda: int(_env("TAMANHO_PAGINA", "500")))
    req_por_segundo: float = field(default_factory=lambda: float(_env("REQ_POR_SEGUNDO", "2")))
    max_tentativas: int = field(default_factory=lambda: int(_env("MAX_TENTATIVAS", "6")))
    timeout_s: float = field(default_factory=lambda: float(_env("TIMEOUT_S", "60")))
    # No modo incremental, janelas com este atraso ou mais em relação à mais recente do run
    # são baixadas de novo (endpoints principais), mesmo já tendo _SUCCESS: a fonte publica
    # itens e resultados com semanas de atraso, e o marcador de um dia baixado vazio
    # impediria o dado de chegar.
    dias_refazer_janela: int = 14
    # Limiares das regras de aviso
    volume_tolerancia: float = 0.30  # DQ-17
    volume_dias_media: int = 28  # DQ-17
    max_pct_rejeitados: float = 0.02  # DQ-18
    freshness_horas: int = 24  # DQ-16
    tolerancia_valor: float = 0.01  # DQ-14
    # DQ-04: acima disso o valor é erro de digitação na fonte (ex.: empenho de laboratório
    # publicado com R$ 2,8 trilhões), e o registro vai para a quarentena.
    valor_maximo: int = 50_000_000_000

    @property
    def landing(self) -> str:
        return self.landing_root or f"/Volumes/{self.catalog}/landing/raw"

    def tabela(self, schema: str, nome: str) -> str:
        return f"{self.catalog}.{schema}.{nome}"

    # Atalhos por camada -------------------------------------------------------------
    def bronze(self, entidade: str) -> str:
        return self.tabela("bronze", f"bronze_{entidade}")

    def silver(self, entidade: str) -> str:
        return self.tabela("silver", f"silver_{entidade}")

    def quarentena(self, entidade: str) -> str:
        return self.tabela("quality", f"quarentena_{entidade}")

    def gold(self, nome: str) -> str:
        return self.tabela("gold", nome)

    @property
    def dq_resultados(self) -> str:
        return self.tabela("quality", "dq_resultados")

    @property
    def ctl_watermark(self) -> str:
        return self.tabela("ctl", "ctl_watermark")

    @property
    def ctl_ingestao_log(self) -> str:
        return self.tabela("ctl", "ctl_ingestao_log")
