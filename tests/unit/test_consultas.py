from compras.consultas import consultas_negocio, sem_comentarios


def test_consultas_de_negocio_empacotadas():
    consultas = consultas_negocio()
    assert list(consultas) == [
        "q1_gasto_orgao_mes",
        "q2_fornecedores_contratos",
        "q3_itens_mais_comprados",
        "q4_evolucao_mensal",
        "q5_modalidades",
        "q6_economia_disputa",
        "q7_preco_por_item",
    ]
    for sql in consultas.values():
        corpo = sem_comentarios(sql)
        assert corpo.upper().startswith(("SELECT", "WITH"))
        assert ";" not in corpo  # uma instrução por arquivo
        assert "gold." in corpo
