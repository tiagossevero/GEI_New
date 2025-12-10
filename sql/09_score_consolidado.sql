-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 09 - SCORE CONSOLIDADO E RANKING FINAL
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: Cálculo do score final e ranking dos grupos
-- =====================================================

-- =====================================================
-- 1. GEI_SCORE_CONSOLIDADO - Score final por grupo
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_score_consolidado;

CREATE TABLE gessimples.gei_score_consolidado AS
SELECT
    gc.num_grupo,

    -- Contagens básicas
    COUNT(DISTINCT gc.cnpj) AS qntd_cnpj,

    -- ==================== MÉTRICAS DE FATURAMENTO ====================
    COALESCE(fm.receita_total_grupo, 0) AS receita_total_grupo,
    COALESCE(fm.sn_acima_limite_sn, 0) AS sn_acima_limite_sn,
    COALESCE(fm.sn_regime_misto, 0) AS sn_regime_misto,
    COALESCE(fm.qtd_cnpjs_pgdas, 0) AS qtd_cnpjs_pgdas,
    COALESCE(fm.qtd_cnpjs_dime, 0) AS qtd_cnpjs_dime,

    -- Score de faturamento (0-10)
    CASE
        WHEN fm.sn_acima_limite_sn = 1 THEN 5
        ELSE 0
    END +
    CASE
        WHEN fm.sn_regime_misto = 1 THEN 2
        ELSE 0
    END AS score_faturamento,

    -- ==================== MÉTRICAS DE SÓCIOS ====================
    COALESCE(sm.qtd_socios_compartilhados, 0) AS qtd_socios_compartilhados,
    COALESCE(sm.indice_interconexao, 0) AS indice_interconexao,

    -- Score de sócios (0-10)
    LEAST(COALESCE(sm.qtd_socios_compartilhados, 0) * 2, 10) AS score_socios,

    -- ==================== MÉTRICAS CCS ====================
    COALESCE(ccs.score_risco_ccs, 0) AS score_risco_ccs,
    COALESCE(ccs.qtd_cpfs_compartilhados, 0) AS qtd_cpfs_ccs_compartilhados,

    -- ==================== MÉTRICAS C115 ====================
    COALESCE(c115.score_risco_c115, 0) AS score_risco_c115,
    COALESCE(c115.qtd_identificadores_compartilhados, 0) AS qtd_identificadores_c115,

    -- ==================== MÉTRICAS NFE ====================
    COALESCE(nfe.score_nfe, 0) AS score_nfe,

    -- ==================== MÉTRICAS FUNCIONÁRIOS ====================
    COALESCE(func.total_funcionarios, 0) AS total_funcionarios,
    COALESCE(func.qtd_cpfs_compartilhados, 0) AS qtd_cpfs_func_compartilhados,

    -- ==================== MÉTRICAS CONTADOR ====================
    COALESCE(cont.qtd_contadores_compartilhados, 0) AS qtd_contadores_compartilhados,
    cont.situacao_contador,

    -- Score contador (0-5)
    CASE
        WHEN cont.situacao_contador = 'MESMO_CONTADOR' THEN 3
        WHEN cont.situacao_contador = 'CONTADOR_COMPARTILHADO' THEN 2
        ELSE 0
    END AS score_contador,

    -- ==================== MÉTRICAS INDÍCIOS ====================
    COALESCE(ind.qtd_total_indicios, 0) AS qtd_total_indicios,
    COALESCE(ind.qtd_indicios_alta, 0) AS qtd_indicios_alta,

    -- Score indícios (0-10)
    LEAST(COALESCE(ind.qtd_indicios_alta, 0) * 3 + COALESCE(ind.qtd_total_indicios, 0), 10) AS score_indicios,

    -- ==================== SCORE FINAL ====================
    (
        -- Faturamento (peso 25%)
        (CASE WHEN fm.sn_acima_limite_sn = 1 THEN 5 ELSE 0 END +
         CASE WHEN fm.sn_regime_misto = 1 THEN 2 ELSE 0 END) * 2.5 +

        -- Sócios (peso 25%)
        LEAST(COALESCE(sm.qtd_socios_compartilhados, 0) * 2, 10) * 2.5 +

        -- CCS (peso 15%)
        LEAST(COALESCE(ccs.score_risco_ccs, 0), 10) * 1.5 +

        -- C115 (peso 10%)
        LEAST(COALESCE(c115.score_risco_c115, 0), 10) * 1.0 +

        -- NFe (peso 10%)
        LEAST(COALESCE(nfe.score_nfe, 0), 10) * 1.0 +

        -- Contador (peso 5%)
        CASE WHEN cont.situacao_contador = 'MESMO_CONTADOR' THEN 5 ELSE 0 END * 0.5 +

        -- Indícios (peso 10%)
        LEAST(COALESCE(ind.qtd_indicios_alta, 0) * 3 + COALESCE(ind.qtd_total_indicios, 0), 10) * 1.0
    ) AS score_final

FROM gessimples.gei_cnpj gc
LEFT JOIN gessimples.gei_faturamento_metricas fm ON gc.num_grupo = fm.num_grupo
LEFT JOIN gessimples.gei_socios_metricas sm ON gc.num_grupo = sm.num_grupo
LEFT JOIN gessimples.gei_ccs_metricas_grupo ccs ON gc.num_grupo = ccs.num_grupo
LEFT JOIN gessimples.gei_c115_metricas_grupos c115 ON gc.num_grupo = c115.num_grupo
LEFT JOIN (
    SELECT
        COALESCE(grupo_emit, grupo_dest) AS num_grupo,
        SUM(score_inconsistencias) AS score_nfe
    FROM gessimples.gei_nfe_completo
    GROUP BY COALESCE(grupo_emit, grupo_dest)
) nfe ON gc.num_grupo = nfe.num_grupo
LEFT JOIN gessimples.gei_funcionarios_metricas_grupo func ON gc.num_grupo = func.num_grupo
LEFT JOIN gessimples.gei_contador_metricas cont ON gc.num_grupo = cont.num_grupo
LEFT JOIN gessimples.gei_indicios_metricas_grupo ind ON gc.num_grupo = ind.num_grupo
GROUP BY
    gc.num_grupo,
    fm.receita_total_grupo, fm.sn_acima_limite_sn, fm.sn_regime_misto,
    fm.qtd_cnpjs_pgdas, fm.qtd_cnpjs_dime,
    sm.qtd_socios_compartilhados, sm.indice_interconexao,
    ccs.score_risco_ccs, ccs.qtd_cpfs_compartilhados,
    c115.score_risco_c115, c115.qtd_identificadores_compartilhados,
    nfe.score_nfe,
    func.total_funcionarios, func.qtd_cpfs_compartilhados,
    cont.qtd_contadores_compartilhados, cont.situacao_contador,
    ind.qtd_total_indicios, ind.qtd_indicios_alta;

COMPUTE STATS gessimples.gei_score_consolidado;

-- =====================================================
-- 2. GEI_RANKING_FINAL - Ranking dos grupos por risco
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_ranking_final;

CREATE TABLE gessimples.gei_ranking_final AS
SELECT
    num_grupo,
    qntd_cnpj,
    score_final,
    score_faturamento,
    score_socios,
    score_risco_ccs AS score_ccs,
    score_risco_c115 AS score_c115,
    score_nfe,
    score_contador,
    score_indicios,

    -- Classificação de risco
    CASE
        WHEN score_final >= 70 THEN 'CRÍTICO'
        WHEN score_final >= 50 THEN 'ALTO'
        WHEN score_final >= 30 THEN 'MÉDIO'
        WHEN score_final >= 10 THEN 'BAIXO'
        ELSE 'MÍNIMO'
    END AS nivel_risco,

    -- Ranking
    ROW_NUMBER() OVER (ORDER BY score_final DESC) AS ranking,

    -- Métricas resumidas
    receita_total_grupo,
    qtd_socios_compartilhados,
    qtd_total_indicios,
    sn_acima_limite_sn,
    sn_regime_misto

FROM gessimples.gei_score_consolidado
WHERE qntd_cnpj >= 2  -- Mínimo de 2 empresas para ser grupo
ORDER BY score_final DESC;

COMPUTE STATS gessimples.gei_ranking_final;

-- =====================================================
-- 3. VIEW: TOP 15 GRUPOS CRÍTICOS
-- =====================================================

DROP VIEW IF EXISTS gessimples.gei_top15_criticos;

CREATE VIEW gessimples.gei_top15_criticos AS
SELECT *
FROM gessimples.gei_ranking_final
WHERE ranking <= 15
  AND nivel_risco IN ('CRÍTICO', 'ALTO');
