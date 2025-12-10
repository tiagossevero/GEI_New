-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 01 - TABELAS BASE
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: Tabelas fundamentais do sistema GEI
-- =====================================================

-- =====================================================
-- 1. GEI_CNPJ - Tabela central de CNPJs dos grupos econômicos
-- =====================================================
-- Esta é a tabela principal que define quais CNPJs pertencem a cada grupo

DROP TABLE IF EXISTS gessimples.gei_cnpj;

CREATE TABLE gessimples.gei_cnpj AS
SELECT
    num_grupo,
    REGEXP_REPLACE(TRIM(CAST(cnpj AS STRING)), '[^0-9]', '') AS cnpj,
    nm_razao_social,
    nm_fantasia,
    cd_cnae,
    nm_cnae,
    nm_municipio,
    nm_reg_apuracao,
    dt_inicio_atividade,
    dt_fim_atividade,
    sn_ativo
FROM (
    -- Aqui você deve definir a lógica de identificação dos grupos
    -- Exemplo: empresas com sócios compartilhados, mesmo contador, etc.
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY cnpj) AS num_grupo,
        c.nu_cnpj AS cnpj,
        c.nm_razao_social,
        c.nm_fantasia,
        c.cd_cnae_principal AS cd_cnae,
        c.nm_cnae_principal AS nm_cnae,
        c.nm_municipio,
        c.nm_reg_apuracao,
        c.dt_inicio_atividade,
        c.dt_fim_atividade,
        c.sn_ativo
    FROM usr_sat_ods.vw_ods_contrib c
    WHERE c.sn_ativo = 1
    -- Adicione aqui os critérios de seleção dos grupos
) base;

-- Criar índices para performance
COMPUTE STATS gessimples.gei_cnpj;

-- =====================================================
-- 2. GEI_CADASTRO - Dados cadastrais completos
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_cadastro;

CREATE TABLE gessimples.gei_cadastro AS
SELECT
    gc.num_grupo,
    gc.cnpj,
    c.nm_razao_social,
    c.nm_fantasia,
    c.cd_cnae_principal AS cd_cnae,
    c.nm_cnae_principal AS nm_cnae,
    c.nm_municipio,
    c.nm_reg_apuracao,
    c.dt_inicio_atividade,
    c.sn_ativo,
    c.nm_logradouro,
    c.nu_imovel,
    c.nm_complemento,
    c.nm_bairro,
    c.nu_cep,
    c.nu_telefone,
    c.nm_email,
    c.nu_cnpj_contador AS cnpj_contador,
    c.nm_contador
FROM gessimples.gei_cnpj gc
LEFT JOIN usr_sat_ods.vw_ods_contrib c
    ON gc.cnpj = REGEXP_REPLACE(TRIM(CAST(c.nu_cnpj AS STRING)), '[^0-9]', '');

COMPUTE STATS gessimples.gei_cadastro;

-- =====================================================
-- 3. GEI_PERCENT - Métricas percentuais dos grupos
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_percent;

CREATE TABLE gessimples.gei_percent AS
SELECT
    gc.num_grupo,
    COUNT(DISTINCT gc.cnpj) AS qntd_cnpj,
    COUNT(DISTINCT CASE WHEN gc.nm_reg_apuracao LIKE '%SIMPLES%' THEN gc.cnpj END) AS qntd_sn,
    COUNT(DISTINCT CASE WHEN gc.nm_reg_apuracao NOT LIKE '%SIMPLES%' THEN gc.cnpj END) AS qntd_normal,
    MAX(p.vl_rec_bruta_12m) AS valor_max,
    SUM(p.vl_rec_bruta_12m) AS valor_total,
    AVG(p.vl_rec_bruta_12m) AS valor_medio
FROM gessimples.gei_cnpj gc
LEFT JOIN gessimples.gei_faturamento p ON gc.cnpj = p.cnpj
GROUP BY gc.num_grupo;

COMPUTE STATS gessimples.gei_percent;
