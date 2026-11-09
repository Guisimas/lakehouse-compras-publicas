-- Q4. Como os gastos evoluíram mês a mês?
WITH mensal AS (
  SELECT
    d.ano_mes,
    d.ano,
    d.nome_mes,
    SUM(f.valor_total_item)     AS valor_total,
    COUNT(DISTINCT f.id_compra) AS compras
  FROM gold.fato_compras f
  JOIN gold.dim_data d ON d.sk_data = f.sk_data_publicacao
  WHERE NOT f.compra_excluida
  GROUP BY ALL
)
SELECT
  *,
  ROUND(100 * (valor_total / LAG(valor_total) OVER (ORDER BY ano_mes) - 1), 1)       AS var_mes_pct,
  ROUND(100 * (valor_total / LAG(valor_total, 12) OVER (ORDER BY ano_mes) - 1), 1)   AS var_ano_pct,
  SUM(valor_total) OVER (PARTITION BY ano ORDER BY ano_mes)                          AS acumulado_ano
FROM mensal
ORDER BY ano_mes;
