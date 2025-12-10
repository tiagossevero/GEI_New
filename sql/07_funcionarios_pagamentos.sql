-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 07 - TABELAS DE FUNCIONÁRIOS E PAGAMENTOS
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: Análise de funcionários (RAIS/CAGED) e meios de pagamento
-- =====================================================

-- =====================================================
-- 1. GEI_FUNCIONARIOS_DETALHE - Funcionários por empresa
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_funcionarios_detalhe;

CREATE TABLE gessimples.gei_funcionarios_detalhe AS
SELECT
    gc.num_grupo,
    gc.cnpj,
    r.cpf,
    r.nm_trabalhador,
    r.cbo AS cd_ocupacao,
    r.nm_ocupacao,
    r.dt_admissao,
    r.dt_desligamento,
    r.vl_remuneracao,
    r.tp_vinculo,
    r.ano_base,
    CASE WHEN r.dt_desligamento IS NULL THEN 1 ELSE 0 END AS sn_ativo
FROM gessimples.gei_cnpj gc
INNER JOIN rais_caged.vw_rais_vinculos r
    ON gc.cnpj = REGEXP_REPLACE(TRIM(CAST(r.cnpj_cei AS STRING)), '[^0-9]', '')
WHERE r.ano_base >= 2022;

COMPUTE STATS gessimples.gei_funcionarios_detalhe;

-- =====================================================
-- 2. GEI_FUNCIONARIOS_CPF_COMPARTILHADO - CPFs em múltiplas empresas
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_funcionarios_cpf_compartilhado;

CREATE TABLE gessimples.gei_funcionarios_cpf_compartilhado AS
SELECT
    num_grupo,
    cpf,
    nm_trabalhador,
    COUNT(DISTINCT cnpj) AS qtd_empresas,
    COLLECT_SET(cnpj) AS cnpjs_vinculados,
    MIN(dt_admissao) AS primeira_admissao,
    MAX(dt_admissao) AS ultima_admissao,
    SUM(CASE WHEN sn_ativo = 1 THEN 1 ELSE 0 END) AS qtd_vinculos_ativos
FROM gessimples.gei_funcionarios_detalhe
GROUP BY num_grupo, cpf, nm_trabalhador
HAVING COUNT(DISTINCT cnpj) > 1;

COMPUTE STATS gessimples.gei_funcionarios_cpf_compartilhado;

-- =====================================================
-- 3. GEI_FUNCIONARIOS_METRICAS_GRUPO - Métricas por grupo
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_funcionarios_metricas_grupo;

CREATE TABLE gessimples.gei_funcionarios_metricas_grupo AS
SELECT
    f.num_grupo,
    COUNT(DISTINCT f.cnpj) AS qtd_empresas_com_funcionarios,
    COUNT(DISTINCT f.cpf) AS total_funcionarios,
    SUM(CASE WHEN f.sn_ativo = 1 THEN 1 ELSE 0 END) AS funcionarios_ativos,
    AVG(f.vl_remuneracao) AS remuneracao_media,
    MAX(f.vl_remuneracao) AS remuneracao_maxima,
    SUM(f.vl_remuneracao) AS folha_total,

    -- Compartilhamentos
    COALESCE(comp.qtd_cpfs_compartilhados, 0) AS qtd_cpfs_compartilhados,

    -- Métricas por empresa
    AVG(func_por_empresa.qtd_funcionarios) AS media_funcionarios_por_empresa

FROM gessimples.gei_funcionarios_detalhe f
LEFT JOIN (
    SELECT num_grupo, COUNT(*) AS qtd_cpfs_compartilhados
    FROM gessimples.gei_funcionarios_cpf_compartilhado
    GROUP BY num_grupo
) comp ON f.num_grupo = comp.num_grupo
LEFT JOIN (
    SELECT num_grupo, cnpj, COUNT(DISTINCT cpf) AS qtd_funcionarios
    FROM gessimples.gei_funcionarios_detalhe
    WHERE sn_ativo = 1
    GROUP BY num_grupo, cnpj
) func_por_empresa ON f.num_grupo = func_por_empresa.num_grupo AND f.cnpj = func_por_empresa.cnpj
GROUP BY f.num_grupo, comp.qtd_cpfs_compartilhados;

COMPUTE STATS gessimples.gei_funcionarios_metricas_grupo;

-- =====================================================
-- 4. GEI_PAGAMENTOS_DETALHE - Meios de pagamento (cartões, etc)
-- =====================================================
-- Nota: Esta tabela depende da estrutura de dados de pagamentos disponível

DROP TABLE IF EXISTS gessimples.gei_pagamentos_detalhe;

CREATE TABLE gessimples.gei_pagamentos_detalhe AS
SELECT
    gc.num_grupo,
    gc.cnpj AS identificador,
    'CNPJ' AS tipo_identificador,
    p.nu_per_ref AS periodo,
    p.vl_total AS valor_total,
    p.tp_operacao,
    p.nm_adquirente
FROM gessimples.gei_cnpj gc
INNER JOIN (
    -- Adapte esta subquery conforme a tabela de pagamentos disponível
    SELECT
        nu_cnpj_cpf AS identificador,
        nu_per_ref,
        SUM(vl_operacao) AS vl_total,
        tp_operacao,
        nm_adquirente
    FROM usr_sat_ods.ods_declar_meios_pagtos  -- Ajuste o nome da tabela
    GROUP BY nu_cnpj_cpf, nu_per_ref, tp_operacao, nm_adquirente
) p ON gc.cnpj = REGEXP_REPLACE(TRIM(CAST(p.identificador AS STRING)), '[^0-9]', '')

UNION ALL

-- Pagamentos dos sócios (CPFs)
SELECT
    gs.num_grupo,
    gs.cpf_socio AS identificador,
    'CPF' AS tipo_identificador,
    p.nu_per_ref AS periodo,
    p.vl_total AS valor_total,
    p.tp_operacao,
    p.nm_adquirente
FROM gessimples.gei_socios gs
INNER JOIN (
    SELECT
        nu_cnpj_cpf AS identificador,
        nu_per_ref,
        SUM(vl_operacao) AS vl_total,
        tp_operacao,
        nm_adquirente
    FROM usr_sat_ods.ods_declar_meios_pagtos  -- Ajuste o nome da tabela
    GROUP BY nu_cnpj_cpf, nu_per_ref, tp_operacao, nm_adquirente
) p ON gs.cpf_socio = REGEXP_REPLACE(TRIM(CAST(p.identificador AS STRING)), '[^0-9]', '')
WHERE gs.sn_relacao_ativa = 1;

COMPUTE STATS gessimples.gei_pagamentos_detalhe;

-- =====================================================
-- 5. GEI_PAGAMENTOS_METRICAS_GRUPO - Métricas de pagamentos
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_pagamentos_metricas_grupo;

CREATE TABLE gessimples.gei_pagamentos_metricas_grupo AS
SELECT
    num_grupo,
    SUM(CASE WHEN tipo_identificador = 'CNPJ' THEN valor_total ELSE 0 END) AS valor_total_empresas,
    SUM(CASE WHEN tipo_identificador = 'CPF' THEN valor_total ELSE 0 END) AS valor_total_socios,
    SUM(valor_total) AS valor_total_grupo,
    COUNT(DISTINCT CASE WHEN tipo_identificador = 'CNPJ' THEN identificador END) AS qtd_empresas_com_pagamentos,
    COUNT(DISTINCT CASE WHEN tipo_identificador = 'CPF' THEN identificador END) AS qtd_socios_com_pagamentos,
    COUNT(DISTINCT periodo) AS qtd_periodos,
    AVG(valor_total) AS valor_medio_por_registro
FROM gessimples.gei_pagamentos_detalhe
GROUP BY num_grupo;

COMPUTE STATS gessimples.gei_pagamentos_metricas_grupo;

-- =====================================================
-- 6. GEI_INDICIOS - Indícios fiscais
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_indicios;

CREATE TABLE gessimples.gei_indicios AS
SELECT
    gc.num_grupo,
    gc.cnpj,
    i.cd_indicio,
    i.tx_descricao_indicio,
    i.dt_geracao,
    i.vl_indicio,
    i.tp_gravidade
FROM gessimples.gei_cnpj gc
INNER JOIN usr_sat_ods.ods_indicios_fiscais i  -- Ajuste o nome da tabela
    ON gc.cnpj = REGEXP_REPLACE(TRIM(CAST(i.nu_cnpj AS STRING)), '[^0-9]', '')
WHERE i.dt_geracao >= ADD_MONTHS(CURRENT_DATE(), -24);  -- Últimos 24 meses

COMPUTE STATS gessimples.gei_indicios;

-- =====================================================
-- 7. GEI_INDICIOS_METRICAS_GRUPO - Métricas de indícios
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_indicios_metricas_grupo;

CREATE TABLE gessimples.gei_indicios_metricas_grupo AS
SELECT
    num_grupo,
    COUNT(*) AS qtd_total_indicios,
    COUNT(DISTINCT cnpj) AS qtd_cnpjs_com_indicios,
    COUNT(DISTINCT cd_indicio) AS qtd_tipos_indicios,
    SUM(vl_indicio) AS valor_total_indicios,
    COUNT(CASE WHEN tp_gravidade = 'ALTA' THEN 1 END) AS qtd_indicios_alta,
    COUNT(CASE WHEN tp_gravidade = 'MEDIA' THEN 1 END) AS qtd_indicios_media,
    COUNT(CASE WHEN tp_gravidade = 'BAIXA' THEN 1 END) AS qtd_indicios_baixa
FROM gessimples.gei_indicios
GROUP BY num_grupo;

COMPUTE STATS gessimples.gei_indicios_metricas_grupo;
