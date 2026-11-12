-- Consulta do alerta de qualidade (resources/compras.alert.yml, criado pelo bundle).
-- Compara o último run do job com o anterior e conta os avisos e falhas NOVOS: os avisos que
-- se repetem toda semana (ex.: itens sem fornecedor) não disparam o alerta de novo.
-- Atualidade da ingestão (DQ-16) dispara sempre, mesmo repetida.
-- Falha crítica já para o job e chega pelo e-mail de falha do próprio job.
WITH runs AS (
  SELECT run_id, row_number() OVER (ORDER BY MAX(data_execucao) DESC) AS ordem
  FROM quality.dq_resultados
  GROUP BY run_id
),
problemas AS (
  SELECT r.ordem, d.regra, d.entidade, coalesce(d.detalhe, '') AS detalhe
  FROM quality.dq_resultados d
  JOIN runs r ON r.run_id = d.run_id
  WHERE r.ordem <= 2 AND d.status IN ('aviso', 'falha')
),
novos AS (
  SELECT a.regra, a.entidade, a.detalhe
  FROM problemas a
  LEFT ANTI JOIN problemas p
    ON p.ordem = 2 AND p.regra = a.regra AND p.entidade = a.entidade AND p.detalhe = a.detalhe
  WHERE a.ordem = 1
  UNION
  SELECT regra, entidade, detalhe FROM problemas WHERE ordem = 1 AND regra = 'DQ-16'
)
SELECT
  COUNT(*)                                                    AS problemas_novos,
  concat_ws('; ', collect_list(concat(regra, ' ', entidade))) AS regras
FROM novos
