-- Q6. Quanto a disputa (pregão e concorrência) economiza em relação ao valor estimado?
-- A economia só é medida nos itens que já têm resultado homologado; a cobertura diz quantos
-- itens têm resultado (o de pregão sai semanas depois da publicação).
SELECT
  f.ano_mes,
  m.grupo                                                        AS modalidade,
  COUNT(*)                                                       AS itens,
  COUNT_IF(f.valor_homologado IS NOT NULL)                       AS itens_com_resultado,
  ROUND(100 * COUNT_IF(f.valor_homologado IS NOT NULL) / COUNT(*), 1) AS pct_cobertura,
  SUM(f.valor_estimado) FILTER (WHERE f.valor_homologado IS NOT NULL) AS estimado_com_resultado,
  SUM(f.valor_homologado)                                        AS homologado,
  ROUND(
    100 * (1 - SUM(f.valor_homologado)
               / SUM(f.valor_estimado) FILTER (WHERE f.valor_homologado IS NOT NULL)),
    2
  )                                                              AS pct_economia
FROM gold.fato_compras f
JOIN gold.dim_modalidade m ON m.sk_modalidade = f.sk_modalidade
WHERE NOT f.compra_excluida
  AND m.grupo IN ('Pregão', 'Concorrência')
  AND f.valor_estimado > 0     -- orçamento sigiloso vem zerado e não serve de base
GROUP BY ALL
ORDER BY f.ano_mes, modalidade;
