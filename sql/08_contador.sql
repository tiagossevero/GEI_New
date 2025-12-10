-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 08 - TABELA DE CONTADORES
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: Análise de contadores compartilhados
-- =====================================================

-- =====================================================
-- 1. GEI_CONTADOR - Contadores dos grupos
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_contador;

CREATE TABLE gessimples.gei_contador AS
SELECT
    gc.num_grupo,
    gc.cnpj AS cnpj_empresa,
    c.nu_cnpj_contador AS cnpj_contador,
    c.nm_contador,
    c.nu_crc AS crc_contador,
    c.dt_inicio_vinculo,
    c.dt_fim_vinculo,
    CASE WHEN c.dt_fim_vinculo IS NULL THEN 1 ELSE 0 END AS sn_ativo
FROM gessimples.gei_cnpj gc
INNER JOIN usr_sat_ods.vw_ods_contrib c
    ON gc.cnpj = REGEXP_REPLACE(TRIM(CAST(c.nu_cnpj AS STRING)), '[^0-9]', '')
WHERE c.nu_cnpj_contador IS NOT NULL;

COMPUTE STATS gessimples.gei_contador;

-- =====================================================
-- 2. GEI_CONTADOR_COMPARTILHADO - Contadores em múltiplas empresas
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_contador_compartilhado;

CREATE TABLE gessimples.gei_contador_compartilhado AS
SELECT
    num_grupo,
    cnpj_contador,
    nm_contador,
    crc_contador,
    COUNT(DISTINCT cnpj_empresa) AS qtd_empresas,
    COLLECT_SET(cnpj_empresa) AS cnpjs_vinculados
FROM gessimples.gei_contador
WHERE sn_ativo = 1
GROUP BY num_grupo, cnpj_contador, nm_contador, crc_contador
HAVING COUNT(DISTINCT cnpj_empresa) > 1;

COMPUTE STATS gessimples.gei_contador_compartilhado;

-- =====================================================
-- 3. GEI_CONTADOR_METRICAS - Métricas de contadores
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_contador_metricas;

CREATE TABLE gessimples.gei_contador_metricas AS
SELECT
    c.num_grupo,
    COUNT(DISTINCT c.cnpj_empresa) AS qtd_empresas_com_contador,
    COUNT(DISTINCT c.cnpj_contador) AS qtd_contadores_distintos,
    COALESCE(comp.qtd_contadores_compartilhados, 0) AS qtd_contadores_compartilhados,
    MAX(comp.max_empresas_por_contador) AS max_empresas_por_contador,

    -- Indicador de risco
    CASE
        WHEN COALESCE(comp.qtd_contadores_compartilhados, 0) > 0
             AND COUNT(DISTINCT c.cnpj_contador) = 1
        THEN 'MESMO_CONTADOR'
        WHEN COALESCE(comp.qtd_contadores_compartilhados, 0) > 0
        THEN 'CONTADOR_COMPARTILHADO'
        ELSE 'CONTADORES_DISTINTOS'
    END AS situacao_contador

FROM gessimples.gei_contador c
LEFT JOIN (
    SELECT
        num_grupo,
        COUNT(*) AS qtd_contadores_compartilhados,
        MAX(qtd_empresas) AS max_empresas_por_contador
    FROM gessimples.gei_contador_compartilhado
    GROUP BY num_grupo
) comp ON c.num_grupo = comp.num_grupo
WHERE c.sn_ativo = 1
GROUP BY c.num_grupo, comp.qtd_contadores_compartilhados, comp.max_empresas_por_contador;

COMPUTE STATS gessimples.gei_contador_metricas;
