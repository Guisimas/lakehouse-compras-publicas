-- Rastreabilidade: de uma linha da Gold até o arquivo bruto no landing.
SELECT f.id_compra_item, f.run_id AS run_gold, s.ultima_alteracao_run_id AS run_silver,
       s.arquivo_origem, s.run_id_ingestao_bronze, b.payload
FROM gold.fato_compras f
JOIN silver.silver_itens_compra s ON s.id_compra_item = f.id_compra_item
JOIN bronze.bronze_itens_compra b ON b.hash_registro = s.hash_registro
WHERE f.id_compra_item = :id_compra_item;
