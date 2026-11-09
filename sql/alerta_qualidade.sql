-- Consulta para um Alerta do Databricks SQL (alerta em regras de aviso).
-- Crie um Alert com esta consulta, condição "reprovadas > 0", agendado após o job (07:30).
SELECT
  COUNT(*)                                    AS reprovadas,
  COLLECT_SET(CONCAT(regra, ' ', entidade))   AS regras
FROM quality.dq_resultados
WHERE status IN ('aviso', 'falha')
  AND data_execucao >= current_timestamp() - INTERVAL 24 HOURS;
