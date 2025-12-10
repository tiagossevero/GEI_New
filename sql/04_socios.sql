-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 04 - TABELAS DE SÓCIOS
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: Análise de vínculos societários
-- =====================================================

-- =====================================================
-- 1. GEI_SOCIOS - Vínculos societários dos grupos
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_socios;

CREATE TABLE gessimples.gei_socios AS
SELECT
    gc.num_grupo,
    gc.cnpj,
    v.nu_cpf_cnpj AS cpf_socio,
    v.nm_socio,
    v.tp_qualificacao,
    v.nm_qualificacao,
    v.dt_entrada,
    v.dt_saida,
    v.vl_participacao_capital,
    v.sn_relacao_ativa
FROM gessimples.gei_cnpj gc
INNER JOIN usr_sat_ods.vw_cad_vinculo v
    ON gc.cnpj = REGEXP_REPLACE(TRIM(CAST(v.nu_cnpj AS STRING)), '[^0-9]', '')
WHERE v.tp_vinculo = 'SOCIO'
  AND (v.sn_relacao_ativa = 1 OR v.dt_saida IS NULL);

COMPUTE STATS gessimples.gei_socios;

-- =====================================================
-- 2. GEI_SOCIOS_COMPARTILHADOS - CPFs em múltiplos CNPJs
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_socios_compartilhados;

CREATE TABLE gessimples.gei_socios_compartilhados AS
SELECT
    num_grupo,
    cpf_socio,
    nm_socio,
    COUNT(DISTINCT cnpj) AS qtd_empresas,
    COLLECT_SET(cnpj) AS cnpjs_vinculados,
    SUM(vl_participacao_capital) AS participacao_total,
    MIN(dt_entrada) AS primeira_entrada,
    MAX(dt_entrada) AS ultima_entrada
FROM gessimples.gei_socios
WHERE sn_relacao_ativa = 1
GROUP BY num_grupo, cpf_socio, nm_socio
HAVING COUNT(DISTINCT cnpj) > 1;

COMPUTE STATS gessimples.gei_socios_compartilhados;

-- =====================================================
-- 3. GEI_SOCIOS_METRICAS - Métricas de sócios por grupo
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_socios_metricas;

CREATE TABLE gessimples.gei_socios_metricas AS
SELECT
    num_grupo,
    COUNT(DISTINCT cpf_socio) AS qtd_socios_total,
    COUNT(DISTINCT cnpj) AS qtd_empresas_com_socios,
    SUM(CASE WHEN qtd_empresas > 1 THEN 1 ELSE 0 END) AS qtd_socios_compartilhados,
    MAX(qtd_empresas) AS max_empresas_por_socio,
    AVG(qtd_empresas) AS media_empresas_por_socio,
    -- Índice de interconexão (socios compartilhados / total socios)
    ROUND(
        SUM(CASE WHEN qtd_empresas > 1 THEN 1 ELSE 0 END) * 1.0 /
        NULLIF(COUNT(DISTINCT cpf_socio), 0),
        4
    ) AS indice_interconexao
FROM (
    SELECT
        num_grupo,
        cpf_socio,
        cnpj,
        COUNT(DISTINCT cnpj) OVER (PARTITION BY num_grupo, cpf_socio) AS qtd_empresas
    FROM gessimples.gei_socios
    WHERE sn_relacao_ativa = 1
) base
GROUP BY num_grupo;

COMPUTE STATS gessimples.gei_socios_metricas;

-- =====================================================
-- 4. GEI_PARES_EMPRESAS_RELACIONADAS - Pares por sócio comum
-- =====================================================

DROP VIEW IF EXISTS gessimples.gei_pares_empresas_relacionadas;

CREATE VIEW gessimples.gei_pares_empresas_relacionadas AS
SELECT DISTINCT
    s1.num_grupo,
    s1.cnpj AS cnpj_1,
    s2.cnpj AS cnpj_2,
    s1.cpf_socio,
    s1.nm_socio,
    s1.vl_participacao_capital AS participacao_cnpj_1,
    s2.vl_participacao_capital AS participacao_cnpj_2
FROM gessimples.gei_socios s1
INNER JOIN gessimples.gei_socios s2
    ON s1.num_grupo = s2.num_grupo
    AND s1.cpf_socio = s2.cpf_socio
    AND s1.cnpj < s2.cnpj  -- Evita duplicatas e auto-referência
WHERE s1.sn_relacao_ativa = 1
  AND s2.sn_relacao_ativa = 1;
