-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 06 - TABELAS C115 (CONVÊNIO 115 - ENERGIA/UTILIDADES)
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: Análise de dados do Convênio 115
-- =====================================================

-- =====================================================
-- 1. GEI_C115_IDENTIFICADORES - Identificadores de consumo
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_c115_identificadores;

CREATE TABLE gessimples.gei_c115_identificadores AS
SELECT
    gc.num_grupo,
    gc.cnpj AS cnpj_tomador,
    c.nu_identificador_tomador,
    c.nu_tel_contato,
    c.nu_tel_ou_unidade_consumidora,
    c.dt_emissao,
    COUNT(*) AS qtd_registros
FROM gessimples.gei_cnpj gc
INNER JOIN c115.c115_dados_cadastrais_dest c
    ON gc.cnpj = REGEXP_REPLACE(TRIM(CAST(c.nu_cnpj_cpf_tomador AS STRING)), '[^0-9]', '')
GROUP BY
    gc.num_grupo,
    gc.cnpj,
    c.nu_identificador_tomador,
    c.nu_tel_contato,
    c.nu_tel_ou_unidade_consumidora,
    c.dt_emissao;

COMPUTE STATS gessimples.gei_c115_identificadores;

-- =====================================================
-- 2. GEI_C115_IDENTIFICADOR_TOMADOR_COMPARTILHADO
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_c115_identificador_tomador_compartilhado;

CREATE TABLE gessimples.gei_c115_identificador_tomador_compartilhado AS
SELECT
    num_grupo,
    nu_identificador_tomador AS identificador_compartilhado,
    COUNT(DISTINCT cnpj_tomador) AS qtd_cnpjs,
    COLLECT_SET(cnpj_tomador) AS cnpjs_vinculados,
    SUM(qtd_registros) AS total_registros
FROM gessimples.gei_c115_identificadores
WHERE nu_identificador_tomador IS NOT NULL
  AND TRIM(nu_identificador_tomador) != ''
GROUP BY num_grupo, nu_identificador_tomador
HAVING COUNT(DISTINCT cnpj_tomador) > 1;

COMPUTE STATS gessimples.gei_c115_identificador_tomador_compartilhado;

-- =====================================================
-- 3. GEI_C115_TEL_CONTATO_COMPARTILHADO
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_c115_tel_contato_compartilhado;

CREATE TABLE gessimples.gei_c115_tel_contato_compartilhado AS
SELECT
    num_grupo,
    nu_tel_contato AS telefone_compartilhado,
    COUNT(DISTINCT cnpj_tomador) AS qtd_cnpjs,
    COLLECT_SET(cnpj_tomador) AS cnpjs_vinculados,
    SUM(qtd_registros) AS total_registros
FROM gessimples.gei_c115_identificadores
WHERE nu_tel_contato IS NOT NULL
  AND TRIM(nu_tel_contato) != ''
GROUP BY num_grupo, nu_tel_contato
HAVING COUNT(DISTINCT cnpj_tomador) > 1;

COMPUTE STATS gessimples.gei_c115_tel_contato_compartilhado;

-- =====================================================
-- 4. GEI_C115_TEL_UNIDADE_COMPARTILHADO
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_c115_tel_unidade_compartilhado;

CREATE TABLE gessimples.gei_c115_tel_unidade_compartilhado AS
SELECT
    num_grupo,
    nu_tel_ou_unidade_consumidora AS unidade_compartilhada,
    COUNT(DISTINCT cnpj_tomador) AS qtd_cnpjs,
    COLLECT_SET(cnpj_tomador) AS cnpjs_vinculados,
    SUM(qtd_registros) AS total_registros
FROM gessimples.gei_c115_identificadores
WHERE nu_tel_ou_unidade_consumidora IS NOT NULL
  AND TRIM(nu_tel_ou_unidade_consumidora) != ''
GROUP BY num_grupo, nu_tel_ou_unidade_consumidora
HAVING COUNT(DISTINCT cnpj_tomador) > 1;

COMPUTE STATS gessimples.gei_c115_tel_unidade_compartilhado;

-- =====================================================
-- 5. GEI_C115_PARES_CNPJS_RELACIONADOS
-- =====================================================

DROP VIEW IF EXISTS gessimples.gei_c115_pares_cnpjs_relacionados;

CREATE VIEW gessimples.gei_c115_pares_cnpjs_relacionados AS
-- Por identificador
SELECT DISTINCT
    num_grupo,
    cnpjs_vinculados[0] AS cnpj_1,
    cnpjs_vinculados[1] AS cnpj_2,
    'IDENTIFICADOR' AS tipo_vinculo,
    identificador_compartilhado AS valor_vinculo
FROM gessimples.gei_c115_identificador_tomador_compartilhado
WHERE SIZE(cnpjs_vinculados) = 2

UNION ALL

-- Por telefone de contato
SELECT DISTINCT
    num_grupo,
    cnpjs_vinculados[0] AS cnpj_1,
    cnpjs_vinculados[1] AS cnpj_2,
    'TELEFONE_CONTATO' AS tipo_vinculo,
    telefone_compartilhado AS valor_vinculo
FROM gessimples.gei_c115_tel_contato_compartilhado
WHERE SIZE(cnpjs_vinculados) = 2

UNION ALL

-- Por unidade consumidora
SELECT DISTINCT
    num_grupo,
    cnpjs_vinculados[0] AS cnpj_1,
    cnpjs_vinculados[1] AS cnpj_2,
    'UNIDADE_CONSUMIDORA' AS tipo_vinculo,
    unidade_compartilhada AS valor_vinculo
FROM gessimples.gei_c115_tel_unidade_compartilhado
WHERE SIZE(cnpjs_vinculados) = 2;

-- =====================================================
-- 6. GEI_C115_METRICAS_GRUPOS
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_c115_metricas_grupos;

CREATE TABLE gessimples.gei_c115_metricas_grupos AS
SELECT
    i.num_grupo,
    COUNT(DISTINCT i.cnpj_tomador) AS qtd_cnpjs_c115,
    COUNT(DISTINCT i.nu_identificador_tomador) AS qtd_identificadores,
    COUNT(DISTINCT i.nu_tel_contato) AS qtd_telefones_contato,
    COUNT(DISTINCT i.nu_tel_ou_unidade_consumidora) AS qtd_unidades_consumidoras,

    -- Compartilhamentos
    COALESCE(id.qtd_identificadores_compartilhados, 0) AS qtd_identificadores_compartilhados,
    COALESCE(tel.qtd_telefones_compartilhados, 0) AS qtd_telefones_compartilhados,
    COALESCE(uni.qtd_unidades_compartilhadas, 0) AS qtd_unidades_compartilhadas,

    -- Score de risco C115
    (COALESCE(id.qtd_identificadores_compartilhados, 0) * 3 +
     COALESCE(tel.qtd_telefones_compartilhados, 0) * 2 +
     COALESCE(uni.qtd_unidades_compartilhadas, 0) * 2
    ) AS score_risco_c115

FROM gessimples.gei_c115_identificadores i
LEFT JOIN (
    SELECT num_grupo, COUNT(*) AS qtd_identificadores_compartilhados
    FROM gessimples.gei_c115_identificador_tomador_compartilhado
    GROUP BY num_grupo
) id ON i.num_grupo = id.num_grupo
LEFT JOIN (
    SELECT num_grupo, COUNT(*) AS qtd_telefones_compartilhados
    FROM gessimples.gei_c115_tel_contato_compartilhado
    GROUP BY num_grupo
) tel ON i.num_grupo = tel.num_grupo
LEFT JOIN (
    SELECT num_grupo, COUNT(*) AS qtd_unidades_compartilhadas
    FROM gessimples.gei_c115_tel_unidade_compartilhado
    GROUP BY num_grupo
) uni ON i.num_grupo = uni.num_grupo
GROUP BY i.num_grupo,
         id.qtd_identificadores_compartilhados,
         tel.qtd_telefones_compartilhados,
         uni.qtd_unidades_compartilhadas;

COMPUTE STATS gessimples.gei_c115_metricas_grupos;

-- =====================================================
-- 7. GEI_C115_RANKING_RISCO_GRUPO_ECONOMICO
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_c115_ranking_risco_grupo_economico;

CREATE TABLE gessimples.gei_c115_ranking_risco_grupo_economico AS
SELECT
    num_grupo,
    score_risco_c115,
    qtd_identificadores_compartilhados,
    qtd_telefones_compartilhados,
    qtd_unidades_compartilhadas,
    CASE
        WHEN score_risco_c115 >= 10 THEN 'CRÍTICO'
        WHEN score_risco_c115 >= 5 THEN 'ALTO'
        WHEN score_risco_c115 >= 2 THEN 'MÉDIO'
        ELSE 'BAIXO'
    END AS nivel_risco_grupo_economico,
    ROW_NUMBER() OVER (ORDER BY score_risco_c115 DESC) AS ranking
FROM gessimples.gei_c115_metricas_grupos
WHERE score_risco_c115 > 0;

COMPUTE STATS gessimples.gei_c115_ranking_risco_grupo_economico;
