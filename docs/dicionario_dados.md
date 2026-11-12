# Dicionário de dados

O catálogo do ambiente é `dev_compras` ou `prd_compras`. Os schemas são `landing` (Volume
`raw`), `bronze`, `silver`, `gold`, `quality` e `ctl`. O schema canônico de cada entidade da
Silver, com tipos e mapeamento por endpoint, está em `src/compras/silver/schemas.py`, que é a
fonte da verdade.

## Bronze — `bronze.bronze_<entidade>`

Colunas originais da API, todas como string e com os nomes da fonte, mais:

| Coluna | Descrição |
| --- | --- |
| `data_ingestao` | Momento da carga na Bronze |
| `arquivo_origem` | Caminho do arquivo no landing |
| `fonte_dados` | `api` ou `csv` |
| `data_referencia` | Janela (partição). Janela anual = 1º de janeiro |
| `tipo_janela` | `dia`, `ano` ou `snapshot` |
| `run_id` / `run_id_ingestao` | Run do pipeline / run da ingestão que baixou o arquivo |
| `endpoint`, `fanout`, `sistema_origem`, `regime_legal_origem` | Procedência dentro da API |
| `hash_registro` | sha256 do registro bruto |
| `payload` | JSON original do registro |

## Silver — `silver.silver_<entidade>`

| Entidade | Chave natural | Partição | Destaques |
| --- | --- | --- | --- |
| `licitacoes` | `id_compra` | `ano_mes` (publicação) | `regime_legal`, `codigo_uasg` (6 dígitos), `codigo_modalidade`, valores estimado/homologado `decimal(18,2)`, `excluido` |
| `itens_compra` | `id_compra_item` | `ano_mes` (data do item) | `tipo_item` M/S, `codigo_item_catalogo` (quando a fonte manda vazio, vem da API do PNCP), quantidade e unitário `decimal(18,4)`, totais `decimal(18,2)`, `ni_fornecedor` (CNPJ ou CPF mascarado), `tipo_pessoa` |
| `contratos` | `id_contrato` (nº de controle PNCP, ou unidade-tipo-número) | `ano_mes` (início da vigência) | vigência, valor global/parcela/acumulado, `ni_fornecedor` |
| `fornecedores` | `ni_fornecedor` | — | razão social, porte, natureza jurídica, UF, município, CNAE, ativo, habilitado |
| `orgaos` | `codigo_uasg` | — | UASG + órgão + órgão superior, esfera, poder |
| `catalogo` | (`tipo_item`, `codigo_item`) | — | CATMAT/CATSER: descrição, grupo, classe, PDM/subclasse |

Colunas técnicas em toda tabela Silver:

| Coluna | Descrição |
| --- | --- |
| `prioridade_origem` | Desempate entre fontes no dedup (0 = PNCP) |
| `hash_linha` | Hash dos atributos de negócio (MERGE só reescreve se mudou) |
| `arquivo_origem`, `hash_registro`, `run_id_ingestao_bronze` | Rastreabilidade |
| `ultima_alteracao_run_id`, `data_processamento` | Run que gravou a versão atual |

## Gold — `gold.*`

### `fato_compras` (grão: 1 item de 1 compra; partição `ano_mes`)

| Coluna | Descrição |
| --- | --- |
| `sk_data_publicacao`, `sk_data_resultado` | → `dim_data` (yyyymmdd; -1 = sem data) |
| `sk_orgao`, `sk_fornecedor`, `sk_item`, `sk_modalidade` | → dimensões (-1 = desconhecido) |
| `id_compra_item`, `id_compra`, `numero_item` | Dimensões degeneradas |
| `unidade` | Unidade de medida do item (maiúsculas, sem espaços nas pontas). Compara preço só entre itens de mesmo código e mesma unidade |
| `regime_legal`, `sistema_origem`, `compra_excluida` | Atributos de contexto |
| `quantidade`, `valor_unitario`, `valor_total_item` | Homologado quando existe, senão estimado |
| `valor_estimado`, `valor_homologado`, `valor_eh_estimado` | Para medir a economia na disputa |
| `arquivo_origem`, `run_id` | Rastreabilidade |

### `fato_contratos` (grão: 1 contrato; partição `ano_mes` do início da vigência)

`sk_data_inicio_vigencia`, `sk_data_fim_vigencia`, `sk_orgao`, `sk_fornecedor`,
`sk_modalidade`, `id_contrato`, `numero_contrato`, `id_compra`, `regime_legal`,
`tipo_contrato`, `categoria`, `excluido`, `valor_global`, `valor_parcela`, `numero_parcelas`,
`valor_acumulado`, `prazo_dias`, `arquivo_origem`, `run_id`.

### Dimensões

| Tabela | Chave natural | Tipo | Colunas |
| --- | --- | --- | --- |
| `dim_data` | `data` | estática | `sk_data` (yyyymmdd), ano, trimestre, mês, nome do mês, `ano_mes`, dia, dia da semana, fim de semana, ano fiscal |
| `dim_fornecedor` | `ni_fornecedor` | **SCD2** | atributos do cadastro, `situacao`, `origem_cadastro` (cadastro/inferido), `hash_atributos`, `data_inicio_vigencia`, `data_fim_vigencia` (9999-12-31 = atual), `registro_atual` |
| `dim_orgao` | `codigo_uasg` | SCD1 | UASG, UF, município, órgão, órgão superior, esfera, poder, tipo de administração |
| `dim_item` | `item_chave` (`M:123`) | SCD1 | tipo, código, descrição, grupo, classe, subclasse/PDM |
| `dim_modalidade` | `modalidade_chave` (`14133:5`; códigos do SIASG nas duas leis) | SCD1 | regime, código, nome, grupo, `sem_licitacao`, origem (seed/observada) |

## Controle e qualidade

| Tabela | Conteúdo |
| --- | --- |
| `ctl.ctl_ingestao_log` | Uma linha por execução de ingestão (`log_id`), com páginas, linhas, duração, status e erro |
| `ctl.ctl_watermark` | (`entidade`, `camada`) → última janela ok. A entidade `pipeline` na camada `gold` guia o job diário |
| `quality.dq_resultados` | Uma linha por regra, entidade, janela e run |
| `quality.quarentena_<entidade>` | Registros reprovados, com a regra e o motivo |
| `quality.benchmark_consultas`, `quality.volume_tabelas` | Benchmark de antes e depois e bytes/arquivos por tabela |
