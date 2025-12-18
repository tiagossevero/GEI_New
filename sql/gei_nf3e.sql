-- =====================================================
-- GEI - SISTEMA DE DETECAO DE GRUPOS ECONOMICOS
-- TABELA GEI_NF3E - Notas Fiscais de Energia Eletrica
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-18
-- Descricao: Agregacao de consumo de energia eletrica por CNPJ
-- Fonte: nf3e.nf3e (84 milhoes de registros)
-- =====================================================

-- =====================================================
-- TABELA 1: gei_nf3e - Valores mensais por CNPJ destinatario
-- =====================================================
DROP TABLE IF EXISTS gessimples.gei_nf3e;
SET REQUEST_POOL = 'medium';

CREATE TABLE gessimples.gei_nf3e AS
WITH base AS (
  SELECT
    REGEXP_REPLACE(TRIM(COALESCE(
      procnf3e.nf3e.infnf3e.dest.cnpj,
      procnf3e.nf3e.infnf3e.dest.cpf
    )), '[^0-9]', '') AS cnpj_dest,
    CAST(ano_emissao AS INT) * 100 + CAST(mes_emissao AS INT) AS nu_per_ref,
    COALESCE(CAST(procnf3e.nf3e.infnf3e.total.vnf AS DOUBLE), 0) AS vl_nota
  FROM nf3e.nf3e
  WHERE ano_emissao >= 2020
    AND ano_emissao <= 2025
    AND (procnf3e.nf3e.infnf3e.dest.cnpj IS NOT NULL
         OR procnf3e.nf3e.infnf3e.dest.cpf IS NOT NULL)
),
agregado AS (
  SELECT
    cnpj_dest,
    nu_per_ref,
    SUM(vl_nota) AS vl_energia_mensal,
    COUNT(*) AS qt_notas
  FROM base
  WHERE LENGTH(cnpj_dest) >= 11
  GROUP BY cnpj_dest, nu_per_ref
),
acumulado AS (
  SELECT
    cnpj_dest,
    nu_per_ref,
    vl_energia_mensal,
    qt_notas,
    SUM(vl_energia_mensal) OVER (
      PARTITION BY cnpj_dest
      ORDER BY nu_per_ref
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS vl_energia_12m
  FROM agregado
)
SELECT
  a.cnpj_dest AS cnpj,

  -- Periodo 2021
  MAX(CASE WHEN a.nu_per_ref = 202101 THEN a.vl_energia_12m ELSE 0 END) AS jan2021,
  MAX(CASE WHEN a.nu_per_ref = 202102 THEN a.vl_energia_12m ELSE 0 END) AS fev2021,
  MAX(CASE WHEN a.nu_per_ref = 202103 THEN a.vl_energia_12m ELSE 0 END) AS mar2021,
  MAX(CASE WHEN a.nu_per_ref = 202104 THEN a.vl_energia_12m ELSE 0 END) AS abr2021,
  MAX(CASE WHEN a.nu_per_ref = 202105 THEN a.vl_energia_12m ELSE 0 END) AS mai2021,
  MAX(CASE WHEN a.nu_per_ref = 202106 THEN a.vl_energia_12m ELSE 0 END) AS jun2021,
  MAX(CASE WHEN a.nu_per_ref = 202107 THEN a.vl_energia_12m ELSE 0 END) AS jul2021,
  MAX(CASE WHEN a.nu_per_ref = 202108 THEN a.vl_energia_12m ELSE 0 END) AS ago2021,
  MAX(CASE WHEN a.nu_per_ref = 202109 THEN a.vl_energia_12m ELSE 0 END) AS set2021,
  MAX(CASE WHEN a.nu_per_ref = 202110 THEN a.vl_energia_12m ELSE 0 END) AS out2021,
  MAX(CASE WHEN a.nu_per_ref = 202111 THEN a.vl_energia_12m ELSE 0 END) AS nov2021,
  MAX(CASE WHEN a.nu_per_ref = 202112 THEN a.vl_energia_12m ELSE 0 END) AS dez2021,

  -- Periodo 2022
  MAX(CASE WHEN a.nu_per_ref = 202201 THEN a.vl_energia_12m ELSE 0 END) AS jan2022,
  MAX(CASE WHEN a.nu_per_ref = 202202 THEN a.vl_energia_12m ELSE 0 END) AS fev2022,
  MAX(CASE WHEN a.nu_per_ref = 202203 THEN a.vl_energia_12m ELSE 0 END) AS mar2022,
  MAX(CASE WHEN a.nu_per_ref = 202204 THEN a.vl_energia_12m ELSE 0 END) AS abr2022,
  MAX(CASE WHEN a.nu_per_ref = 202205 THEN a.vl_energia_12m ELSE 0 END) AS mai2022,
  MAX(CASE WHEN a.nu_per_ref = 202206 THEN a.vl_energia_12m ELSE 0 END) AS jun2022,
  MAX(CASE WHEN a.nu_per_ref = 202207 THEN a.vl_energia_12m ELSE 0 END) AS jul2022,
  MAX(CASE WHEN a.nu_per_ref = 202208 THEN a.vl_energia_12m ELSE 0 END) AS ago2022,
  MAX(CASE WHEN a.nu_per_ref = 202209 THEN a.vl_energia_12m ELSE 0 END) AS set2022,
  MAX(CASE WHEN a.nu_per_ref = 202210 THEN a.vl_energia_12m ELSE 0 END) AS out2022,
  MAX(CASE WHEN a.nu_per_ref = 202211 THEN a.vl_energia_12m ELSE 0 END) AS nov2022,
  MAX(CASE WHEN a.nu_per_ref = 202212 THEN a.vl_energia_12m ELSE 0 END) AS dez2022,

  -- Periodo 2023
  MAX(CASE WHEN a.nu_per_ref = 202301 THEN a.vl_energia_12m ELSE 0 END) AS jan2023,
  MAX(CASE WHEN a.nu_per_ref = 202302 THEN a.vl_energia_12m ELSE 0 END) AS fev2023,
  MAX(CASE WHEN a.nu_per_ref = 202303 THEN a.vl_energia_12m ELSE 0 END) AS mar2023,
  MAX(CASE WHEN a.nu_per_ref = 202304 THEN a.vl_energia_12m ELSE 0 END) AS abr2023,
  MAX(CASE WHEN a.nu_per_ref = 202305 THEN a.vl_energia_12m ELSE 0 END) AS mai2023,
  MAX(CASE WHEN a.nu_per_ref = 202306 THEN a.vl_energia_12m ELSE 0 END) AS jun2023,
  MAX(CASE WHEN a.nu_per_ref = 202307 THEN a.vl_energia_12m ELSE 0 END) AS jul2023,
  MAX(CASE WHEN a.nu_per_ref = 202308 THEN a.vl_energia_12m ELSE 0 END) AS ago2023,
  MAX(CASE WHEN a.nu_per_ref = 202309 THEN a.vl_energia_12m ELSE 0 END) AS set2023,
  MAX(CASE WHEN a.nu_per_ref = 202310 THEN a.vl_energia_12m ELSE 0 END) AS out2023,
  MAX(CASE WHEN a.nu_per_ref = 202311 THEN a.vl_energia_12m ELSE 0 END) AS nov2023,
  MAX(CASE WHEN a.nu_per_ref = 202312 THEN a.vl_energia_12m ELSE 0 END) AS dez2023,

  -- Periodo 2024
  MAX(CASE WHEN a.nu_per_ref = 202401 THEN a.vl_energia_12m ELSE 0 END) AS jan2024,
  MAX(CASE WHEN a.nu_per_ref = 202402 THEN a.vl_energia_12m ELSE 0 END) AS fev2024,
  MAX(CASE WHEN a.nu_per_ref = 202403 THEN a.vl_energia_12m ELSE 0 END) AS mar2024,
  MAX(CASE WHEN a.nu_per_ref = 202404 THEN a.vl_energia_12m ELSE 0 END) AS abr2024,
  MAX(CASE WHEN a.nu_per_ref = 202405 THEN a.vl_energia_12m ELSE 0 END) AS mai2024,
  MAX(CASE WHEN a.nu_per_ref = 202406 THEN a.vl_energia_12m ELSE 0 END) AS jun2024,
  MAX(CASE WHEN a.nu_per_ref = 202407 THEN a.vl_energia_12m ELSE 0 END) AS jul2024,
  MAX(CASE WHEN a.nu_per_ref = 202408 THEN a.vl_energia_12m ELSE 0 END) AS ago2024,
  MAX(CASE WHEN a.nu_per_ref = 202409 THEN a.vl_energia_12m ELSE 0 END) AS set2024,
  MAX(CASE WHEN a.nu_per_ref = 202410 THEN a.vl_energia_12m ELSE 0 END) AS out2024,
  MAX(CASE WHEN a.nu_per_ref = 202411 THEN a.vl_energia_12m ELSE 0 END) AS nov2024,
  MAX(CASE WHEN a.nu_per_ref = 202412 THEN a.vl_energia_12m ELSE 0 END) AS dez2024,

  -- Periodo 2025
  MAX(CASE WHEN a.nu_per_ref = 202501 THEN a.vl_energia_12m ELSE 0 END) AS jan2025,
  MAX(CASE WHEN a.nu_per_ref = 202502 THEN a.vl_energia_12m ELSE 0 END) AS fev2025,
  MAX(CASE WHEN a.nu_per_ref = 202503 THEN a.vl_energia_12m ELSE 0 END) AS mar2025,
  MAX(CASE WHEN a.nu_per_ref = 202504 THEN a.vl_energia_12m ELSE 0 END) AS abr2025,
  MAX(CASE WHEN a.nu_per_ref = 202505 THEN a.vl_energia_12m ELSE 0 END) AS mai2025,
  MAX(CASE WHEN a.nu_per_ref = 202506 THEN a.vl_energia_12m ELSE 0 END) AS jun2025,
  MAX(CASE WHEN a.nu_per_ref = 202507 THEN a.vl_energia_12m ELSE 0 END) AS jul2025,
  MAX(CASE WHEN a.nu_per_ref = 202508 THEN a.vl_energia_12m ELSE 0 END) AS ago2025,
  MAX(CASE WHEN a.nu_per_ref = 202509 THEN a.vl_energia_12m ELSE 0 END) AS set2025

FROM acumulado a
JOIN gessimples.gei_cnpj gc ON a.cnpj_dest = gc.cnpj
WHERE a.vl_energia_12m IS NOT NULL
GROUP BY a.cnpj_dest;

COMPUTE STATS gessimples.gei_nf3e;


-- =====================================================
-- TABELA 2: gei_nf3e_metricas_grupo - Metricas agregadas por grupo economico
-- =====================================================
DROP TABLE IF EXISTS gessimples.gei_nf3e_metricas_grupo;

CREATE TABLE gessimples.gei_nf3e_metricas_grupo AS
WITH base AS (
  SELECT
    REGEXP_REPLACE(TRIM(COALESCE(
      procnf3e.nf3e.infnf3e.dest.cnpj,
      procnf3e.nf3e.infnf3e.dest.cpf
    )), '[^0-9]', '') AS cnpj_dest,
    REGEXP_REPLACE(TRIM(procnf3e.nf3e.infnf3e.emit.cnpj), '[^0-9]', '') AS cnpj_emit,
    CAST(ano_emissao AS INT) * 100 + CAST(mes_emissao AS INT) AS nu_per_ref,
    COALESCE(CAST(procnf3e.nf3e.infnf3e.total.vnf AS DOUBLE), 0) AS vl_nota
  FROM nf3e.nf3e
  WHERE ano_emissao >= 2023
    AND (procnf3e.nf3e.infnf3e.dest.cnpj IS NOT NULL
         OR procnf3e.nf3e.infnf3e.dest.cpf IS NOT NULL)
),
por_cnpj AS (
  SELECT
    b.cnpj_dest,
    gc.num_grupo,
    SUM(b.vl_nota) AS vl_total_energia,
    COUNT(*) AS qt_notas,
    COUNT(DISTINCT b.cnpj_emit) AS qt_fornecedores,
    COUNT(DISTINCT b.nu_per_ref) AS qt_meses_consumo,
    AVG(b.vl_nota) AS vl_medio_nota,
    MAX(b.vl_nota) AS vl_max_nota,
    MIN(CASE WHEN b.vl_nota > 0 THEN b.vl_nota END) AS vl_min_nota
  FROM base b
  JOIN gessimples.gei_cnpj gc ON b.cnpj_dest = gc.cnpj
  WHERE LENGTH(b.cnpj_dest) >= 11
  GROUP BY b.cnpj_dest, gc.num_grupo
)
SELECT
  num_grupo,
  COUNT(DISTINCT cnpj_dest) AS qt_empresas_consumidoras,
  SUM(vl_total_energia) AS vl_energia_grupo,
  SUM(qt_notas) AS qt_notas_grupo,
  SUM(qt_fornecedores) AS qt_fornecedores_grupo,
  AVG(vl_medio_nota) AS vl_medio_nota_grupo,
  MAX(vl_max_nota) AS vl_max_nota_grupo,
  AVG(qt_meses_consumo) AS media_meses_consumo
FROM por_cnpj
GROUP BY num_grupo;

COMPUTE STATS gessimples.gei_nf3e_metricas_grupo;


-- =====================================================
-- TABELA 3: gei_nf3e_detalhado - Detalhamento mensal por empresa
-- =====================================================
DROP TABLE IF EXISTS gessimples.gei_nf3e_detalhado;

CREATE TABLE gessimples.gei_nf3e_detalhado AS
WITH base AS (
  SELECT
    REGEXP_REPLACE(TRIM(COALESCE(
      procnf3e.nf3e.infnf3e.dest.cnpj,
      procnf3e.nf3e.infnf3e.dest.cpf
    )), '[^0-9]', '') AS cnpj_dest,
    REGEXP_REPLACE(TRIM(procnf3e.nf3e.infnf3e.emit.cnpj), '[^0-9]', '') AS cnpj_emit,
    ano_emissao,
    mes_emissao,
    COALESCE(CAST(procnf3e.nf3e.infnf3e.total.vnf AS DOUBLE), 0) AS vl_nota
  FROM nf3e.nf3e
  WHERE ano_emissao >= 2023
    AND (procnf3e.nf3e.infnf3e.dest.cnpj IS NOT NULL
         OR procnf3e.nf3e.infnf3e.dest.cpf IS NOT NULL)
)
SELECT
  b.cnpj_dest AS cnpj,
  gc.num_grupo,
  b.ano_emissao,
  b.mes_emissao,
  SUM(b.vl_nota) AS vl_energia_mensal,
  COUNT(*) AS qt_notas,
  COUNT(DISTINCT b.cnpj_emit) AS qt_fornecedores
FROM base b
JOIN gessimples.gei_cnpj gc ON b.cnpj_dest = gc.cnpj
WHERE LENGTH(b.cnpj_dest) >= 11
GROUP BY b.cnpj_dest, gc.num_grupo, b.ano_emissao, b.mes_emissao;

COMPUTE STATS gessimples.gei_nf3e_detalhado;
