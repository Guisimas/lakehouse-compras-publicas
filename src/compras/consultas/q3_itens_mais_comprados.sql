-- Q3. Quais itens (materiais e serviços) são mais comprados, em quantidade e valor?
SELECT
  i.nome_tipo_item,
  i.codigo_item,
  i.descricao,
  i.nome_classe,
  COUNT(DISTINCT f.id_compra)  AS compras,
  SUM(f.quantidade)            AS quantidade_total,
  SUM(f.valor_total_item)      AS valor_total,
  PERCENTILE_APPROX(f.valor_unitario, 0.5) AS valor_unitario_mediano
FROM gold.fato_compras f
JOIN gold.dim_item i ON i.sk_item = f.sk_item
WHERE NOT f.compra_excluida
  AND f.sk_item <> -1          -- itens sem código de catálogo ficam fora do ranking
GROUP BY ALL
ORDER BY valor_total DESC
LIMIT 50;
