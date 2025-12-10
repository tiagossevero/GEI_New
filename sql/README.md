# GEI - Scripts SQL de Tabelas Intermediárias

## Visão Geral

Este diretório contém os scripts SQL para criar as tabelas intermediárias do Sistema de Detecção de Grupos Econômicos (GEI).

## Estrutura dos Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `00_executar_todos.sql` | Script principal com documentação e ordem de execução |
| `01_tabelas_base.sql` | Tabelas fundamentais: gei_cnpj, gei_cadastro, gei_percent |
| `02_faturamento.sql` | Faturamento PGDAS (Simples) e DIME (Normal) |
| `03_nfe_inconsistencias.sql` | NFe/NFCe e views de detecção de inconsistências |
| `04_socios.sql` | Análise de vínculos societários |
| `05_ccs_contas.sql` | Contas bancárias (CCS) |
| `06_c115.sql` | Convênio 115 (energia/utilidades) |
| `07_funcionarios_pagamentos.sql` | Funcionários, pagamentos e indícios |
| `08_contador.sql` | Contadores compartilhados |
| `09_score_consolidado.sql` | Score final e ranking de risco |

## Ordem de Execução

**IMPORTANTE:** Os scripts devem ser executados na ordem numérica devido às dependências entre tabelas.

```
1. 01_tabelas_base.sql
2. 02_faturamento.sql
3. 03_nfe_inconsistencias.sql
4. 04_socios.sql
5. 05_ccs_contas.sql
6. 06_c115.sql
7. 07_funcionarios_pagamentos.sql
8. 08_contador.sql
9. 09_score_consolidado.sql
```

## Tabelas Criadas

### Tabelas Base
- `gei_cnpj` - Tabela central com CNPJs dos grupos econômicos
- `gei_cadastro` - Dados cadastrais completos
- `gei_percent` - Métricas percentuais dos grupos

### Faturamento
- `gei_pgdas` - Faturamento do Simples Nacional
- `gei_dime` - Faturamento do Regime Normal
- `gei_faturamento` - View consolidada (PGDAS + DIME)
- `gei_faturamento_metricas` - Métricas de faturamento por grupo

### NFe/NFCe
- `gei_nfe` - View unificada de notas fiscais
- `gei_nfe_completo` - NFe com flags de inconsistência
- Views de inconsistências:
  - `gei_ip_incons` - IP de transmissão compartilhado
  - `gei_email_incons` - Email compartilhado
  - `gei_tel_emit_incons` - Telefone emitente compartilhado
  - `gei_tel_dest_incons` - Telefone destinatário compartilhado
  - `gei_end_emit_incons` - Endereço emitente compartilhado
  - `gei_end_dest_incons` - Endereço destinatário compartilhado
  - `gei_clientes_incons` - Cliente compartilhado
  - `gei_fornecedores_incons` - Fornecedor compartilhado
  - `gei_codigos_incons` - Código de produto compartilhado
  - `gei_produtos_incons` - Descrição de produto compartilhada

### Sócios
- `gei_socios` - Vínculos societários
- `gei_socios_compartilhados` - Sócios em múltiplas empresas
- `gei_socios_metricas` - Métricas de sócios por grupo
- `gei_pares_empresas_relacionadas` - Pares de empresas por sócio comum

### CCS (Contas Bancárias)
- `gei_ccs_contas` - Contas bancárias
- `gei_ccs_cpf_compartilhado` - CPF em múltiplas contas
- `gei_ccs_padroes_coordenados` - Aberturas simultâneas
- `gei_ccs_sobreposicao_responsaveis` - Sobreposição de responsabilidade
- `gei_ccs_metricas_grupo` - Métricas CCS por grupo
- `gei_ccs_ranking_risco` - Ranking de risco CCS

### C115 (Convênio 115)
- `gei_c115_identificadores` - Identificadores de consumo
- `gei_c115_identificador_tomador_compartilhado`
- `gei_c115_tel_contato_compartilhado`
- `gei_c115_tel_unidade_compartilhado`
- `gei_c115_pares_cnpjs_relacionados`
- `gei_c115_metricas_grupos`
- `gei_c115_ranking_risco_grupo_economico`

### Funcionários e Pagamentos
- `gei_funcionarios_detalhe` - Detalhes de funcionários
- `gei_funcionarios_cpf_compartilhado` - Funcionários em múltiplas empresas
- `gei_funcionarios_metricas_grupo` - Métricas de funcionários
- `gei_pagamentos_detalhe` - Meios de pagamento
- `gei_pagamentos_metricas_grupo` - Métricas de pagamentos
- `gei_indicios` - Indícios fiscais
- `gei_indicios_metricas_grupo` - Métricas de indícios

### Contadores
- `gei_contador` - Contadores das empresas
- `gei_contador_compartilhado` - Contadores em múltiplas empresas
- `gei_contador_metricas` - Métricas de contadores

### Score e Ranking
- `gei_score_consolidado` - Score final consolidado de cada grupo
- `gei_ranking_final` - Ranking dos grupos por nível de risco
- `gei_top15_criticos` - Top 15 grupos críticos

## Uso no Streamlit

Após criar as tabelas, o aplicativo Streamlit pode consultá-las diretamente:

```python
# Exemplo de consulta
query = """
SELECT * FROM gessimples.gei_ranking_final
WHERE nivel_risco IN ('CRÍTICO', 'ALTO')
ORDER BY ranking
LIMIT 15
"""
df = pd.read_sql(query, engine)
```

## Notas Importantes

1. **Ajuste de Tabelas de Origem**: Verifique os nomes das tabelas de origem no seu ambiente antes de executar.

2. **Períodos de Data**: Os scripts usam período de 2020-2025 por padrão. Ajuste conforme necessário.

3. **Performance**: Execute `COMPUTE STATS` após criar cada tabela para otimizar as consultas.

4. **Regime Misto**: A nova tabela `gei_dime` permite identificar grupos com empresas em regimes tributários diferentes (Simples + Normal).
