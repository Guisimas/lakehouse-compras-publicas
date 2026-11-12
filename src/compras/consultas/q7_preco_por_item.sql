-- Q7. Quais compras pagaram mais que o dobro do preço mediano do mesmo material?
-- Compara só materiais de mesmo código e mesma unidade, comprados por 3 ou mais órgãos.
-- Serviços ficam de fora: a unidade ("UNIDADE") não diz o escopo, então o preço não compara.
-- Preço = homologado quando existe, senão estimado (coluna base_preco).
WITH base AS (
  SELECT
    f.id_compra_item, f.id_compra, f.sk_orgao, f.unidade, f.quantidade, f.valor_unitario,
    f.valor_eh_estimado, f.ano_mes, i.codigo_item, i.descricao,
    coalesce(o.nome_orgao, o.nome_uasg) AS orgao
  FROM gold.fato_compras f
  JOIN gold.dim_item i  ON i.sk_item = f.sk_item
  JOIN gold.dim_orgao o ON o.sk_orgao = f.sk_orgao
  WHERE NOT f.compra_excluida
    AND i.tipo_item = 'M'
    AND f.unidade IS NOT NULL
    AND f.valor_unitario > 0
),
referencia AS (
  SELECT codigo_item, unidade, MEDIAN(valor_unitario) AS preco_mediano,
         COUNT(DISTINCT sk_orgao) AS orgaos_comparados
  FROM base
  GROUP BY codigo_item, unidade
  HAVING COUNT(DISTINCT sk_orgao) >= 3
)
SELECT
  b.ano_mes, b.orgao, b.id_compra, b.codigo_item, b.descricao, b.unidade, b.quantidade,
  b.valor_unitario, r.preco_mediano,
  ROUND(b.valor_unitario / r.preco_mediano, 1) AS vezes_mediana,
  r.orgaos_comparados,
  CASE WHEN b.valor_eh_estimado THEN 'estimado' ELSE 'homologado' END AS base_preco
FROM base b
JOIN referencia r USING (codigo_item, unidade)
WHERE b.valor_unitario > 2 * r.preco_mediano
ORDER BY vezes_mediana DESC
LIMIT 100;
