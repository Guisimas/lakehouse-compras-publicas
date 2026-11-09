-- Métricas reais para o README. Rode depois do backfill completo.
-- USE CATALOG dev_compras;

-- 1. Linhas por camada e entidade
SELECT 'bronze' AS camada, 'licitacoes' AS entidade, COUNT(*) AS linhas FROM bronze.bronze_licitacoes
UNION ALL SELECT 'bronze', 'itens_compra', COUNT(*) FROM bronze.bronze_itens_compra
UNION ALL SELECT 'bronze', 'contratos', COUNT(*) FROM bronze.bronze_contratos
UNION ALL SELECT 'bronze', 'fornecedores', COUNT(*) FROM bronze.bronze_fornecedores
UNION ALL SELECT 'silver', 'licitacoes', COUNT(*) FROM silver.silver_licitacoes
UNION ALL SELECT 'silver', 'itens_compra', COUNT(*) FROM silver.silver_itens_compra
UNION ALL SELECT 'silver', 'contratos', COUNT(*) FROM silver.silver_contratos
UNION ALL SELECT 'silver', 'fornecedores', COUNT(*) FROM silver.silver_fornecedores
UNION ALL SELECT 'gold', 'fato_compras', COUNT(*) FROM gold.fato_compras
UNION ALL SELECT 'gold', 'fato_contratos', COUNT(*) FROM gold.fato_contratos
UNION ALL SELECT 'quarentena', 'licitacoes', COUNT(*) FROM quality.quarentena_licitacoes WHERE NOT resolvido
UNION ALL SELECT 'quarentena', 'itens_compra', COUNT(*) FROM quality.quarentena_itens_compra WHERE NOT resolvido
UNION ALL SELECT 'quarentena', 'contratos', COUNT(*) FROM quality.quarentena_contratos WHERE NOT resolvido
UNION ALL SELECT 'quarentena', 'fornecedores', COUNT(*) FROM quality.quarentena_fornecedores WHERE NOT resolvido;

-- 2. Quantidade de licitações, contratos, fornecedores e itens (Gold)
SELECT
  (SELECT COUNT(DISTINCT id_compra) FROM gold.fato_compras)                AS licitacoes,
  (SELECT COUNT(*) FROM gold.fato_contratos)                               AS contratos,
  (SELECT COUNT(DISTINCT ni_fornecedor) FROM gold.dim_fornecedor WHERE sk_fornecedor <> -1) AS fornecedores,
  (SELECT COUNT(*) FROM gold.fato_compras)                                 AS itens;

-- 3. % rejeitados por entidade e regra (último resultado de cada janela)
SELECT entidade, regra,
       SUM(linhas_reprovadas) AS reprovadas,
       SUM(linhas_avaliadas)  AS avaliadas,
       ROUND(100 * SUM(linhas_reprovadas) / NULLIF(SUM(linhas_avaliadas), 0), 3) AS pct
FROM quality.dq_resultados
WHERE camada = 'bronze->silver' AND regra BETWEEN 'DQ-02' AND 'DQ-08'
GROUP BY ALL
ORDER BY entidade, regra;

-- 4. Tempo de execução da ingestão (mediana dos últimos 7 runs). O tempo total do job
--    está no histórico de runs do Workflows (ou em system.lakeflow.job_run_timeline).
WITH runs AS (
  SELECT run_id, SUM(duracao_s) AS duracao_s
  FROM ctl.ctl_ingestao_log
  GROUP BY run_id
  ORDER BY MAX(fim) DESC
  LIMIT 7
)
SELECT PERCENTILE(duracao_s, 0.5) AS mediana_ingestao_s FROM runs;

-- 5. Tempo das consultas antes/depois da otimização (gerado por `compras-task benchmark`)
SELECT rotulo, consulta, mediana_s, arquivos_tabela, bytes_tabela
FROM quality.benchmark_consultas
ORDER BY consulta, rotulo;

-- 6. Volume armazenado por camada (gravado por `compras-task benchmark`)
SELECT camada, ROUND(SUM(bytes) / POW(1024, 3), 3) AS gb, SUM(arquivos) AS arquivos
FROM quality.volume_tabelas
WHERE medido_em = (SELECT MAX(medido_em) FROM quality.volume_tabelas)
GROUP BY camada
ORDER BY camada;

-- 7. Versões SCD2: fornecedores com mais de uma versão vinda do cadastro
--    (a transição "inferido → cadastro" não conta como mudança cadastral)
SELECT COUNT(*) AS fornecedores_com_mudanca
FROM (
  SELECT ni_fornecedor
  FROM gold.dim_fornecedor
  WHERE origem_cadastro = 'cadastro'
  GROUP BY ni_fornecedor
  HAVING COUNT(*) > 1
);
