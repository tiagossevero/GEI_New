-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 05 - TABELAS CCS (CONTAS BANCÁRIAS)
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: Análise de contas bancárias (CCS)
-- =====================================================

-- =====================================================
-- 1. GEI_CCS_CONTAS - Contas bancárias dos grupos
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_ccs_contas;

CREATE TABLE gessimples.gei_ccs_contas AS
SELECT
    gc.num_grupo,
    gc.cnpj AS cnpj_empresa,
    c.nr_cpf AS cpf_responsavel,
    c.nm_responsavel,
    c.nm_banco,
    c.cd_agencia,
    c.nr_conta,
    c.tp_conta,
    c.tp_responsavel,
    c.dt_abertura,
    c.dt_encerramento,
    c.dt_inicio_responsavel,
    c.dt_final_responsavel,
    CASE
        WHEN c.dt_encerramento IS NULL THEN 'ATIVA'
        ELSE 'ENCERRADA'
    END AS status_conta
FROM gessimples.gei_cnpj gc
INNER JOIN usr_sat_fsn.fsn_conta_bancaria c
    ON gc.cnpj = REGEXP_REPLACE(TRIM(CAST(c.nr_cnpj AS STRING)), '[^0-9]', '')
WHERE (c.valido IS NULL OR c.valido = 1);

COMPUTE STATS gessimples.gei_ccs_contas;

-- =====================================================
-- 2. GEI_CCS_CPF_COMPARTILHADO - CPFs em múltiplas contas
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_ccs_cpf_compartilhado;

CREATE TABLE gessimples.gei_ccs_cpf_compartilhado AS
SELECT
    num_grupo,
    cpf_responsavel,
    nm_responsavel,
    COUNT(DISTINCT cnpj_empresa) AS qtd_empresas,
    COUNT(DISTINCT nr_conta) AS qtd_contas,
    COLLECT_SET(cnpj_empresa) AS cnpjs_vinculados,
    COLLECT_SET(nm_banco) AS bancos_vinculados,
    MIN(dt_abertura) AS primeira_conta,
    MAX(dt_abertura) AS ultima_conta
FROM gessimples.gei_ccs_contas
WHERE status_conta = 'ATIVA'
GROUP BY num_grupo, cpf_responsavel, nm_responsavel
HAVING COUNT(DISTINCT cnpj_empresa) > 1;

COMPUTE STATS gessimples.gei_ccs_cpf_compartilhado;

-- =====================================================
-- 3. GEI_CCS_PADROES_COORDENADOS - Aberturas simultâneas
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_ccs_padroes_coordenados;

CREATE TABLE gessimples.gei_ccs_padroes_coordenados AS
SELECT
    num_grupo,
    dt_abertura,
    COUNT(DISTINCT cnpj_empresa) AS qtd_empresas,
    COUNT(DISTINCT nr_conta) AS qtd_contas,
    COLLECT_SET(cnpj_empresa) AS cnpjs_envolvidos,
    COLLECT_SET(nm_banco) AS bancos_envolvidos
FROM gessimples.gei_ccs_contas
WHERE dt_abertura IS NOT NULL
GROUP BY num_grupo, dt_abertura
HAVING COUNT(DISTINCT cnpj_empresa) > 1;

COMPUTE STATS gessimples.gei_ccs_padroes_coordenados;

-- =====================================================
-- 4. GEI_CCS_SOBREPOSICAO_RESPONSAVEIS - Sobreposição temporal
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_ccs_sobreposicao_responsaveis;

CREATE TABLE gessimples.gei_ccs_sobreposicao_responsaveis AS
SELECT
    c1.num_grupo,
    c1.cpf_responsavel,
    c1.nm_responsavel,
    c1.cnpj_empresa AS cnpj_1,
    c2.cnpj_empresa AS cnpj_2,
    c1.dt_inicio_responsavel AS inicio_1,
    c1.dt_final_responsavel AS fim_1,
    c2.dt_inicio_responsavel AS inicio_2,
    c2.dt_final_responsavel AS fim_2,
    -- Calcular período de sobreposição
    GREATEST(c1.dt_inicio_responsavel, c2.dt_inicio_responsavel) AS inicio_sobreposicao,
    LEAST(
        COALESCE(c1.dt_final_responsavel, CURRENT_DATE()),
        COALESCE(c2.dt_final_responsavel, CURRENT_DATE())
    ) AS fim_sobreposicao
FROM gessimples.gei_ccs_contas c1
INNER JOIN gessimples.gei_ccs_contas c2
    ON c1.num_grupo = c2.num_grupo
    AND c1.cpf_responsavel = c2.cpf_responsavel
    AND c1.cnpj_empresa < c2.cnpj_empresa
WHERE c1.status_conta = 'ATIVA'
  AND c2.status_conta = 'ATIVA'
  -- Verificar sobreposição temporal
  AND c1.dt_inicio_responsavel <= COALESCE(c2.dt_final_responsavel, CURRENT_DATE())
  AND c2.dt_inicio_responsavel <= COALESCE(c1.dt_final_responsavel, CURRENT_DATE());

COMPUTE STATS gessimples.gei_ccs_sobreposicao_responsaveis;

-- =====================================================
-- 5. GEI_CCS_METRICAS_GRUPO - Métricas CCS por grupo
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_ccs_metricas_grupo;

CREATE TABLE gessimples.gei_ccs_metricas_grupo AS
SELECT
    c.num_grupo,
    COUNT(DISTINCT c.cnpj_empresa) AS qtd_empresas_com_conta,
    COUNT(DISTINCT c.nr_conta) AS qtd_contas_total,
    COUNT(DISTINCT c.cpf_responsavel) AS qtd_responsaveis_total,
    COUNT(DISTINCT c.nm_banco) AS qtd_bancos_distintos,

    -- Métricas de compartilhamento
    COALESCE(cpf.qtd_cpfs_compartilhados, 0) AS qtd_cpfs_compartilhados,
    COALESCE(pad.qtd_aberturas_coordenadas, 0) AS qtd_aberturas_coordenadas,
    COALESCE(sob.qtd_sobreposicoes, 0) AS qtd_sobreposicoes,

    -- Score de risco CCS
    (COALESCE(cpf.qtd_cpfs_compartilhados, 0) * 3 +
     COALESCE(pad.qtd_aberturas_coordenadas, 0) * 2 +
     COALESCE(sob.qtd_sobreposicoes, 0) * 1
    ) AS score_risco_ccs

FROM gessimples.gei_ccs_contas c
LEFT JOIN (
    SELECT num_grupo, COUNT(*) AS qtd_cpfs_compartilhados
    FROM gessimples.gei_ccs_cpf_compartilhado
    GROUP BY num_grupo
) cpf ON c.num_grupo = cpf.num_grupo
LEFT JOIN (
    SELECT num_grupo, COUNT(*) AS qtd_aberturas_coordenadas
    FROM gessimples.gei_ccs_padroes_coordenados
    GROUP BY num_grupo
) pad ON c.num_grupo = pad.num_grupo
LEFT JOIN (
    SELECT num_grupo, COUNT(*) AS qtd_sobreposicoes
    FROM gessimples.gei_ccs_sobreposicao_responsaveis
    GROUP BY num_grupo
) sob ON c.num_grupo = sob.num_grupo
GROUP BY c.num_grupo,
         cpf.qtd_cpfs_compartilhados,
         pad.qtd_aberturas_coordenadas,
         sob.qtd_sobreposicoes;

COMPUTE STATS gessimples.gei_ccs_metricas_grupo;

-- =====================================================
-- 6. GEI_CCS_RANKING_RISCO - Ranking de risco CCS
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_ccs_ranking_risco;

CREATE TABLE gessimples.gei_ccs_ranking_risco AS
SELECT
    num_grupo,
    score_risco_ccs,
    qtd_cpfs_compartilhados,
    qtd_aberturas_coordenadas,
    qtd_sobreposicoes,
    CASE
        WHEN score_risco_ccs >= 10 THEN 'CRÍTICO'
        WHEN score_risco_ccs >= 5 THEN 'ALTO'
        WHEN score_risco_ccs >= 2 THEN 'MÉDIO'
        ELSE 'BAIXO'
    END AS nivel_risco_ccs,
    ROW_NUMBER() OVER (ORDER BY score_risco_ccs DESC) AS ranking
FROM gessimples.gei_ccs_metricas_grupo
WHERE score_risco_ccs > 0;

COMPUTE STATS gessimples.gei_ccs_ranking_risco;
