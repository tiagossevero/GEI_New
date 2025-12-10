-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 00 - SCRIPT PRINCIPAL DE EXECUÇÃO
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
--
-- INSTRUÇÕES:
-- Execute este script no Hue/Impala para criar todas as tabelas
-- na ordem correta de dependência.
--
-- ORDEM DE EXECUÇÃO:
-- 1. 01_tabelas_base.sql       - Tabelas fundamentais (gei_cnpj, gei_cadastro)
-- 2. 02_faturamento.sql        - Faturamento PGDAS e DIME
-- 3. 03_nfe_inconsistencias.sql - NFe e views de inconsistências
-- 4. 04_socios.sql             - Análise de sócios
-- 5. 05_ccs_contas.sql         - Contas bancárias
-- 6. 06_c115.sql               - Convênio 115
-- 7. 07_funcionarios_pagamentos.sql - Funcionários e pagamentos
-- 8. 08_contador.sql           - Contadores
-- 9. 09_score_consolidado.sql  - Score final e ranking
--
-- IMPORTANTE:
-- - Ajuste os nomes das tabelas de origem conforme seu ambiente
-- - Verifique os períodos de data antes de executar
-- - Execute COMPUTE STATS após criar cada tabela para otimização
-- =====================================================

-- Verificar se o schema existe
CREATE DATABASE IF NOT EXISTS gessimples;

-- =====================================================
-- RESUMO DAS TABELAS CRIADAS
-- =====================================================
/*
TABELAS BASE:
- gessimples.gei_cnpj                    - CNPJs dos grupos econômicos
- gessimples.gei_cadastro                - Dados cadastrais completos
- gessimples.gei_percent                 - Métricas percentuais

FATURAMENTO:
- gessimples.gei_pgdas                   - Faturamento Simples Nacional
- gessimples.gei_dime                    - Faturamento Regime Normal
- gessimples.gei_faturamento             - View consolidada (PGDAS + DIME)
- gessimples.gei_faturamento_metricas    - Métricas de faturamento por grupo

NFe/NFCe:
- gessimples.gei_nfe                     - View de notas fiscais
- gessimples.gei_nfe_completo            - NFe com flags de inconsistência
- gessimples.gei_ip_incons               - IP compartilhado
- gessimples.gei_email_incons            - Email compartilhado
- gessimples.gei_tel_emit_incons         - Telefone emitente compartilhado
- gessimples.gei_tel_dest_incons         - Telefone destinatário compartilhado
- gessimples.gei_end_emit_incons         - Endereço emitente compartilhado
- gessimples.gei_end_dest_incons         - Endereço destinatário compartilhado
- gessimples.gei_clientes_incons         - Cliente compartilhado
- gessimples.gei_fornecedores_incons     - Fornecedor compartilhado
- gessimples.gei_codigos_incons          - Código produto compartilhado
- gessimples.gei_produtos_incons         - Descrição produto compartilhada

SÓCIOS:
- gessimples.gei_socios                  - Vínculos societários
- gessimples.gei_socios_compartilhados   - Sócios em múltiplas empresas
- gessimples.gei_socios_metricas         - Métricas de sócios
- gessimples.gei_pares_empresas_relacionadas - Pares por sócio comum

CCS (CONTAS BANCÁRIAS):
- gessimples.gei_ccs_contas              - Contas bancárias
- gessimples.gei_ccs_cpf_compartilhado   - CPF em múltiplas contas
- gessimples.gei_ccs_padroes_coordenados - Aberturas simultâneas
- gessimples.gei_ccs_sobreposicao_responsaveis - Sobreposição temporal
- gessimples.gei_ccs_metricas_grupo      - Métricas CCS
- gessimples.gei_ccs_ranking_risco       - Ranking de risco CCS

C115 (CONVÊNIO 115):
- gessimples.gei_c115_identificadores    - Identificadores de consumo
- gessimples.gei_c115_identificador_tomador_compartilhado
- gessimples.gei_c115_tel_contato_compartilhado
- gessimples.gei_c115_tel_unidade_compartilhado
- gessimples.gei_c115_pares_cnpjs_relacionados
- gessimples.gei_c115_metricas_grupos
- gessimples.gei_c115_ranking_risco_grupo_economico

FUNCIONÁRIOS E PAGAMENTOS:
- gessimples.gei_funcionarios_detalhe    - Funcionários por empresa
- gessimples.gei_funcionarios_cpf_compartilhado
- gessimples.gei_funcionarios_metricas_grupo
- gessimples.gei_pagamentos_detalhe      - Meios de pagamento
- gessimples.gei_pagamentos_metricas_grupo
- gessimples.gei_indicios                - Indícios fiscais
- gessimples.gei_indicios_metricas_grupo

CONTADORES:
- gessimples.gei_contador                - Contadores das empresas
- gessimples.gei_contador_compartilhado  - Contadores em múltiplas empresas
- gessimples.gei_contador_metricas       - Métricas de contadores

SCORE E RANKING:
- gessimples.gei_score_consolidado       - Score final de cada grupo
- gessimples.gei_ranking_final           - Ranking por risco
- gessimples.gei_top15_criticos          - Top 15 grupos críticos
*/

-- =====================================================
-- CONSULTAS ÚTEIS APÓS EXECUÇÃO
-- =====================================================

-- Ver top 15 grupos mais críticos
-- SELECT * FROM gessimples.gei_top15_criticos;

-- Ver grupos com faturamento acima do limite
-- SELECT * FROM gessimples.gei_ranking_final WHERE sn_acima_limite_sn = 1;

-- Ver grupos com regime misto (Simples + Normal)
-- SELECT * FROM gessimples.gei_ranking_final WHERE sn_regime_misto = 1;

-- Ver detalhes de um grupo específico
-- SELECT * FROM gessimples.gei_cnpj WHERE num_grupo = 123;
-- SELECT * FROM gessimples.gei_faturamento WHERE num_grupo = 123;
-- SELECT * FROM gessimples.gei_socios WHERE num_grupo = 123;
