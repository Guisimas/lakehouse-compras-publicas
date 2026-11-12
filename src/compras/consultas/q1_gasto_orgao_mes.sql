-- Q1. Quais órgãos têm os maiores gastos, por ano e mês?
-- Grão: itens de compra (fato_compras). Valor = homologado quando existe, senão estimado.
-- Execute com o catálogo do ambiente selecionado: USE CATALOG dev_compras;
WITH por_orgao AS (
  SELECT
    d.ano,
    d.mes,
    o.nome_orgao_superior,
    o.nome_orgao,
    SUM(f.valor_total_item)                         AS valor_total,
    SUM(CASE WHEN f.valor_eh_estimado THEN f.valor_total_item ELSE 0 END) AS parcela_estimada,
    COUNT(DISTINCT f.id_compra)                     AS compras,
    COUNT(*)                                        AS itens
  FROM gold.fato_compras f
  JOIN gold.dim_orgao o ON o.sk_orgao = f.sk_orgao
  JOIN gold.dim_data  d ON d.sk_data  = f.sk_data_publicacao
  WHERE NOT f.compra_excluida
  GROUP BY ALL
)
SELECT *
FROM por_orgao
QUALIFY ROW_NUMBER() OVER (PARTITION BY ano, mes ORDER BY valor_total DESC) <= 10
ORDER BY ano, mes, valor_total DESC;
