-- Consultas extras

-- E1. Participação de micro e pequenas empresas nos contratos
SELECT
  COALESCE(df.porte_nome, 'não informado') AS porte,
  COUNT(*)                                 AS contratos,
  SUM(f.valor_global)                      AS valor_global,
  ROUND(100 * SUM(f.valor_global) / SUM(SUM(f.valor_global)) OVER (), 2) AS pct
FROM gold.fato_contratos f
JOIN gold.dim_fornecedor df ON df.sk_fornecedor = f.sk_fornecedor
WHERE NOT f.excluido
GROUP BY ALL
ORDER BY valor_global DESC;

-- E2. Fornecedores que concentram mais de 30% do valor contratado por um órgão
WITH por_orgao AS (
  SELECT o.nome_orgao, df.ni_fornecedor, SUM(f.valor_global) AS valor
  FROM gold.fato_contratos f
  JOIN gold.dim_orgao o       ON o.sk_orgao = f.sk_orgao
  JOIN gold.dim_fornecedor df ON df.sk_fornecedor = f.sk_fornecedor
  WHERE NOT f.excluido AND f.sk_fornecedor <> -1
  GROUP BY ALL
)
SELECT *, ROUND(100 * valor / SUM(valor) OVER (PARTITION BY nome_orgao), 2) AS pct_orgao
FROM por_orgao
QUALIFY valor / SUM(valor) OVER (PARTITION BY nome_orgao) > 0.30
ORDER BY pct_orgao DESC;

-- E3. Diferença entre valor estimado e homologado (economia na disputa), por modalidade
SELECT
  m.grupo,
  SUM(f.valor_estimado)    AS estimado,
  SUM(f.valor_homologado)  AS homologado,
  ROUND(100 * (1 - SUM(f.valor_homologado) / SUM(f.valor_estimado)), 2) AS economia_pct
FROM gold.fato_compras f
JOIN gold.dim_modalidade m ON m.sk_modalidade = f.sk_modalidade
WHERE NOT f.valor_eh_estimado AND f.valor_estimado > 0 AND NOT f.compra_excluida
GROUP BY ALL
ORDER BY homologado DESC;
