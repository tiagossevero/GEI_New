-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 02 - TABELAS DE FATURAMENTO
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: Tabelas de faturamento PGDAS (Simples) e DIME (Normal)
-- =====================================================

-- =====================================================
-- 1. GEI_PGDAS - Faturamento do Simples Nacional
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_pgdas;

CREATE TABLE gessimples.gei_pgdas AS
WITH base AS (
    SELECT
        REGEXP_REPLACE(TRIM(CAST(nu_cnpj AS STRING)), '[^0-9]', '') AS cnpj,
        nu_per_ref AS periodo,
        vl_rec_bruta_estab AS receita_mensal,
        SUM(vl_rec_bruta_estab) OVER (
            PARTITION BY nu_cnpj
            ORDER BY nu_per_ref
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS vl_rec_bruta_12m
    FROM usr_sat_ods.sna_pgdasd_estabelecimento_raw
    WHERE nu_per_ref BETWEEN 202001 AND 202512
)
SELECT
    gc.num_grupo,
    b.cnpj,
    b.periodo,
    b.receita_mensal,
    b.vl_rec_bruta_12m,
    'PGDAS' AS fonte,
    'Simples Nacional' AS regime
FROM base b
INNER JOIN gessimples.gei_cnpj gc ON b.cnpj = gc.cnpj
WHERE b.vl_rec_bruta_12m IS NOT NULL;

COMPUTE STATS gessimples.gei_pgdas;

-- =====================================================
-- 2. GEI_DIME - Faturamento do Regime Normal
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_dime;

CREATE TABLE gessimples.gei_dime AS
WITH base AS (
    SELECT
        REGEXP_REPLACE(TRIM(CAST(NU_CNPJ AS STRING)), '[^0-9]', '') AS cnpj,
        nu_per_ref AS periodo,
        COALESCE(VL_FATURAMENTO, 0) AS faturamento_mensal,
        COALESCE(VL_RECEITA_BRUTA, 0) AS receita_bruta_mensal,
        COALESCE(VL_TOT_CRED, 0) AS total_creditos,
        COALESCE(VL_TOT_DEB, 0) AS total_debitos,
        COALESCE(VL_DEB_RECOLHER, 0) AS debito_recolher,
        sn_com_movimento AS com_movimento,
        sn_cancelada
    FROM usr_sat_ods.ods_decl_dime_raw
    WHERE sn_cancelada = 0
    AND nu_per_ref BETWEEN 202001 AND 202512
),
receitas_12m AS (
    SELECT
        cnpj,
        periodo,
        faturamento_mensal,
        receita_bruta_mensal,
        total_creditos,
        total_debitos,
        debito_recolher,
        com_movimento,
        SUM(faturamento_mensal) OVER (
            PARTITION BY cnpj
            ORDER BY periodo
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS vl_faturamento_12m,
        CASE
            WHEN faturamento_mensal = 0 THEN 'SEM MOVIMENTO'
            WHEN total_creditos = 0 AND total_debitos = 0 THEN 'ZERADA'
            ELSE 'NORMAL'
        END AS situacao_declaracao
    FROM base
)
SELECT
    gc.num_grupo,
    r.cnpj,
    r.periodo,
    r.faturamento_mensal,
    r.receita_bruta_mensal,
    r.vl_faturamento_12m AS vl_rec_bruta_12m,
    r.total_creditos,
    r.total_debitos,
    r.debito_recolher,
    r.com_movimento,
    r.situacao_declaracao,
    'DIME' AS fonte,
    'Regime Normal' AS regime
FROM receitas_12m r
INNER JOIN gessimples.gei_cnpj gc ON r.cnpj = gc.cnpj
WHERE r.vl_faturamento_12m IS NOT NULL;

COMPUTE STATS gessimples.gei_dime;

-- =====================================================
-- 3. GEI_FATURAMENTO - Visão consolidada (PGDAS + DIME)
-- =====================================================

DROP VIEW IF EXISTS gessimples.gei_faturamento;

CREATE VIEW gessimples.gei_faturamento AS
-- PGDAS (Simples Nacional)
SELECT
    num_grupo,
    cnpj,
    periodo,
    receita_mensal AS faturamento_mensal,
    vl_rec_bruta_12m,
    fonte,
    regime,
    NULL AS total_creditos,
    NULL AS total_debitos,
    NULL AS debito_recolher,
    NULL AS situacao_declaracao
FROM gessimples.gei_pgdas

UNION ALL

-- DIME (Regime Normal)
SELECT
    num_grupo,
    cnpj,
    periodo,
    faturamento_mensal,
    vl_rec_bruta_12m,
    fonte,
    regime,
    total_creditos,
    total_debitos,
    debito_recolher,
    situacao_declaracao
FROM gessimples.gei_dime;

-- =====================================================
-- 4. GEI_FATURAMENTO_METRICAS - Métricas por grupo
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_faturamento_metricas;

CREATE TABLE gessimples.gei_faturamento_metricas AS
SELECT
    f.num_grupo,

    -- Contagens
    COUNT(DISTINCT f.cnpj) AS qtd_cnpjs_total,
    COUNT(DISTINCT CASE WHEN f.fonte = 'PGDAS' THEN f.cnpj END) AS qtd_cnpjs_pgdas,
    COUNT(DISTINCT CASE WHEN f.fonte = 'DIME' THEN f.cnpj END) AS qtd_cnpjs_dime,

    -- Receitas máximas
    MAX(CASE WHEN f.fonte = 'PGDAS' THEN f.vl_rec_bruta_12m END) AS receita_max_pgdas,
    MAX(CASE WHEN f.fonte = 'DIME' THEN f.vl_rec_bruta_12m END) AS receita_max_dime,

    -- Receita total do grupo (soma dos máximos por CNPJ)
    SUM(receita_max_cnpj) AS receita_total_grupo,

    -- Indicadores de risco
    CASE WHEN SUM(receita_max_cnpj) > 4800000 THEN 1 ELSE 0 END AS sn_acima_limite_sn,
    CASE
        WHEN COUNT(DISTINCT CASE WHEN f.fonte = 'PGDAS' THEN f.cnpj END) > 0
         AND COUNT(DISTINCT CASE WHEN f.fonte = 'DIME' THEN f.cnpj END) > 0
        THEN 1 ELSE 0
    END AS sn_regime_misto

FROM gessimples.gei_faturamento f
INNER JOIN (
    -- Receita máxima por CNPJ
    SELECT num_grupo, cnpj, MAX(vl_rec_bruta_12m) AS receita_max_cnpj
    FROM gessimples.gei_faturamento
    GROUP BY num_grupo, cnpj
) rm ON f.num_grupo = rm.num_grupo AND f.cnpj = rm.cnpj
GROUP BY f.num_grupo;

COMPUTE STATS gessimples.gei_faturamento_metricas;
