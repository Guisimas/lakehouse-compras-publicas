-- Q5. Quais modalidades (pregão, dispensa, inexigibilidade etc.) concentram mais volume?
SELECT
  m.regime_legal,
  m.grupo,
  m.sem_licitacao,
  COUNT(DISTINCT f.id_compra)  AS compras,
  SUM(f.valor_total_item)      AS valor_total,
  ROUND(100 * SUM(f.valor_total_item) / SUM(SUM(f.valor_total_item)) OVER (), 2) AS pct_valor
FROM gold.fato_compras f
JOIN gold.dim_modalidade m ON m.sk_modalidade = f.sk_modalidade
WHERE NOT f.compra_excluida
GROUP BY ALL
ORDER BY valor_total DESC;
