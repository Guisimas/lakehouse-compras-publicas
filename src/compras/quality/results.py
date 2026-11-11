"""Registro dos resultados de qualidade em ``dq_resultados`` e reação a falhas.

- CRITICA: grava o resultado e interrompe a etapa (``FalhaCritica``); nada é publicado.
- QUARENTENA: o registro é desviado para ``quarentena_<entidade>`` e o pipeline segue.
- AVISO: o pipeline segue; o resultado alimenta alerta e página de qualidade.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum

from pyspark.sql import SparkSession

from compras.common.runtime import agora_utc
from compras.common.tabelas import DQ_RESULTADOS
from compras.config import Settings

log = logging.getLogger(__name__)


class Severidade(StrEnum):
    CRITICA = "critica"
    QUARENTENA = "quarentena"
    AVISO = "aviso"


@dataclass(frozen=True)
class RegraInfo:
    id: str
    dimensao: str
    severidade: Severidade
    descricao: str


# Catálogo das regras de qualidade. A implementação de cada regra está em silver/rules.py,
# gold/* (consistência e reconciliação) e quality/operational.py.
CATALOGO: dict[str, RegraInfo] = {
    r.id: r
    for r in (
        RegraInfo("DQ-01", "validade", Severidade.CRITICA, "Schema com colunas esperadas"),
        RegraInfo("DQ-02", "completude", Severidade.QUARENTENA, "Campos obrigatórios não nulos"),
        RegraInfo("DQ-03", "validade", Severidade.QUARENTENA, "CNPJ com 14 dígitos e DV válido"),
        RegraInfo(
            "DQ-04", "validade", Severidade.QUARENTENA, "Valores numéricos, ≥ 0 e até o teto"
        ),
        RegraInfo("DQ-05", "validade", Severidade.QUARENTENA, "Quantidade do item > 0"),
        RegraInfo("DQ-06", "validade", Severidade.QUARENTENA, "Data válida e não futura"),
        RegraInfo("DQ-07", "validade", Severidade.QUARENTENA, "Fim da vigência ≥ início"),
        RegraInfo("DQ-08", "completude", Severidade.QUARENTENA, "Contrato com órgão identificado"),
        RegraInfo("DQ-09", "unicidade", Severidade.CRITICA, "Chave natural única na Silver"),
        RegraInfo("DQ-10", "consistencia", Severidade.QUARENTENA, "Compra com órgão válido"),
        RegraInfo("DQ-11", "consistencia", Severidade.QUARENTENA, "Item com compra válida"),
        RegraInfo(
            "DQ-12", "consistencia", Severidade.AVISO, "Contrato com fornecedor identificado"
        ),
        RegraInfo("DQ-13", "completude", Severidade.CRITICA, "Sem surrogate key nula na fato"),
        RegraInfo("DQ-14", "consistencia", Severidade.CRITICA, "Soma Gold = soma Silver"),
        RegraInfo("DQ-15", "consistencia", Severidade.CRITICA, "Silver = Gold + rejeitadas"),
        RegraInfo("DQ-16", "atualidade", Severidade.AVISO, "Última carga com sucesso < 24 h"),
        RegraInfo("DQ-17", "volume", Severidade.AVISO, "Volume dentro de ±30% da média 28d"),
        RegraInfo("DQ-18", "validade", Severidade.AVISO, "Rejeitados ≤ 2% por entidade/janela"),
        RegraInfo("SCHEMA-NOVA-COLUNA", "validade", Severidade.AVISO, "Coluna nova na fonte"),
        RegraInfo("DIM-DESCONHECIDO", "consistencia", Severidade.AVISO, "Fato ligada a sk = -1"),
    )
}


class FalhaCritica(RuntimeError):
    """Regra crítica reprovada: a etapa para e nada é publicado na janela."""


@dataclass
class Resultado:
    regra: str
    entidade: str
    janela: str
    camada: str
    linhas_avaliadas: int
    linhas_reprovadas: int
    detalhe: str = ""
    severidade: Severidade | None = None

    @property
    def sev(self) -> Severidade:
        return self.severidade or CATALOGO[self.regra].severidade

    @property
    def status(self) -> str:
        if self.linhas_reprovadas == 0:
            return "ok"
        return {
            Severidade.CRITICA: "falha",
            Severidade.QUARENTENA: "quarentena",
            Severidade.AVISO: "aviso",
        }[self.sev]


class RegistradorDQ:
    def __init__(self, spark: SparkSession, settings: Settings, run_id: str):
        self.spark = spark
        self.settings = settings
        self.run_id = run_id
        self.pendentes: list[Resultado] = []

    def registrar(self, resultado: Resultado) -> Resultado:
        self.pendentes.append(resultado)
        nivel = logging.WARNING if resultado.status != "ok" else logging.INFO
        log.log(
            nivel,
            "[%s] %s %s/%s: %s de %s reprovadas %s",
            resultado.status.upper(),
            resultado.regra,
            resultado.entidade,
            resultado.janela,
            resultado.linhas_reprovadas,
            resultado.linhas_avaliadas,
            resultado.detalhe,
        )
        return resultado

    def gravar(self) -> None:
        if not self.pendentes:
            return
        agora = agora_utc()
        linhas = [
            (
                self.run_id,
                r.regra,
                CATALOGO[r.regra].dimensao,
                r.sev.value,
                r.entidade,
                r.janela,
                r.camada,
                int(r.linhas_avaliadas),
                int(r.linhas_reprovadas),
                r.status,
                r.detalhe[:2000],
                agora,
            )
            for r in self.pendentes
        ]
        self.spark.createDataFrame(linhas, DQ_RESULTADOS).write.format("delta").mode(
            "append"
        ).saveAsTable(self.settings.dq_resultados)
        self.pendentes = []

    def falhar_se_critico(self, resultados: list[Resultado] | None = None) -> None:
        """Grava o que está pendente e levanta ``FalhaCritica`` se alguma crítica falhou."""
        alvo = resultados if resultados is not None else list(self.pendentes)
        criticas = [r for r in alvo if r.status == "falha"]
        self.gravar()
        if criticas:
            raise FalhaCritica(
                "; ".join(f"{r.regra} {r.entidade}/{r.janela}: {r.detalhe}" for r in criticas)
            )
