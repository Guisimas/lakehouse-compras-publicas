-- Q2. Quais fornecedores receberam os maiores valores em contratos?
-- Grão: contrato (fato_contratos). Nunca somar com fato_compras (dupla contagem).
-- O fornecedor é agrupado pelo NI; o nome exibido é o da versão atual (SCD2).
WITH atual AS (
  SELECT ni_fornecedor, razao_social, porte_nome, uf
  FROM gold.dim_fornecedor
  WHERE registro_atual
)
SELECT
  df.ni_fornecedor,
  a.razao_social,
  a.porte_nome,
  a.uf,
  COUNT(*)                AS contratos,
  SUM(f.valor_global)     AS valor_global,
  SUM(f.valor_acumulado)  AS valor_acumulado,
  ROUND(100 * SUM(f.valor_global) / SUM(SUM(f.valor_global)) OVER (), 2) AS pct_do_total
FROM gold.fato_contratos f
JOIN gold.dim_fornecedor df ON df.sk_fornecedor = f.sk_fornecedor
LEFT JOIN atual a ON a.ni_fornecedor = df.ni_fornecedor
WHERE NOT f.excluido
  AND f.sk_fornecedor <> -1
GROUP BY ALL
ORDER BY valor_global DESC
LIMIT 50;
