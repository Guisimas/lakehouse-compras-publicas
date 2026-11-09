from datetime import date

from compras.common.runtime import dias, planejar_janelas


def test_backfill_explicito_devolve_intervalo():
    j = planejar_janelas(
        data_inicio=date(2026, 1, 30), data_fim=date(2026, 2, 2), ultima_janela_ok=date(2026, 5, 1)
    )
    assert j == list(dias(date(2026, 1, 30), date(2026, 2, 2)))


def test_incremental_retoma_do_watermark():
    j = planejar_janelas(
        data_inicio=None,
        data_fim=None,
        ultima_janela_ok=date(2026, 10, 1),
        referencia=date(2026, 10, 4),
    )
    assert j == [date(2026, 10, 2), date(2026, 10, 3), date(2026, 10, 4)]


def test_incremental_sem_watermark_processa_ontem():
    j = planejar_janelas(
        data_inicio=None, data_fim=None, ultima_janela_ok=None, referencia=date(2026, 10, 4)
    )
    assert j == [date(2026, 10, 4)]


def test_recuperacao_limitada_e_reprocessamento():
    j = planejar_janelas(
        data_inicio=None,
        data_fim=date(2026, 10, 31),
        ultima_janela_ok=date(2025, 1, 1),
        dias_reprocessar=(7, 30, 60),
        max_dias_recuperacao=3,
    )
    assert j == [
        date(2026, 9, 1),  # 60 dias antes
        date(2026, 10, 1),  # 30 dias antes
        date(2026, 10, 24),  # 7 dias antes
        date(2026, 10, 29),
        date(2026, 10, 30),
        date(2026, 10, 31),
    ]


def test_watermark_em_dia_nao_duplica():
    j = planejar_janelas(
        data_inicio=None, data_fim=date(2026, 10, 4), ultima_janela_ok=date(2026, 10, 4)
    )
    assert j == [date(2026, 10, 4)]
