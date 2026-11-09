import pytest

from compras.common.validacao import (
    cnpj_valido,
    mascarar_cpf,
    normalizar_ni,
    normalizar_uasg,
    somente_digitos,
)


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [
        ("11.222.333/0001-81", True),
        ("11222333000181", True),
        ("11222333000180", False),  # DV errado
        ("11222333000182", False),
        ("11111111111111", False),  # sequência repetida
        ("1122233300018", False),  # 13 dígitos
        ("", False),
        (None, False),
    ],
)
def test_cnpj_valido(valor, esperado):
    assert cnpj_valido(valor) is esperado


def test_mascarar_cpf():
    assert mascarar_cpf("123.456.789-09") == "***.456.789-**"
    assert mascarar_cpf("12345678909") == "***.456.789-**"
    assert mascarar_cpf("***.456.789-**") == "***.456.789-**"  # já mascarado pela fonte
    assert mascarar_cpf("123") is None
    assert mascarar_cpf("  ") is None


def test_normalizar_ni():
    assert normalizar_ni("11.222.333/0001-81") == ("11222333000181", "PJ")
    assert normalizar_ni("123.456.789-09") == ("***.456.789-**", "PF")
    assert normalizar_ni("***456789**") == ("***.456.789-**", "PF")
    assert normalizar_ni(None) == (None, None)
    assert normalizar_ni("US094788608") == ("US#########", "PE")
    assert normalizar_ni("000000000") == ("#########", "PE")


def test_normalizar_uasg():
    assert normalizar_uasg(153080) == "153080"
    assert normalizar_uasg(" 1530 ") == "001530"
    assert normalizar_uasg("") is None
    assert normalizar_uasg(None) is None


def test_somente_digitos():
    assert somente_digitos("a1.2-3") == "123"
