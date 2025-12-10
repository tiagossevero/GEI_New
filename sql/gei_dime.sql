-- =====================================================
-- GEI - SISTEMA DE DETECAO DE GRUPOS ECONOMICOS
-- TABELA GEI_DIME - Faturamento Regime Normal
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descricao: Receita bruta acumulada 12 meses - Regime Normal (DIME)
-- Estrutura identica a gei_pgdas para facilitar consolidacao
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_dime;
SET REQUEST_POOL = 'medium';

CREATE TABLE gessimples.gei_dime AS
WITH base AS (
  SELECT
    REGEXP_REPLACE(TRIM(CAST(NU_CNPJ AS STRING)), '[^0-9]', '') AS nu_cnpj,
    nu_per_ref,
    SUM(COALESCE(VL_FATURAMENTO, 0)) OVER (
      PARTITION BY REGEXP_REPLACE(TRIM(CAST(NU_CNPJ AS STRING)), '[^0-9]', '')
      ORDER BY nu_per_ref
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS vl_rec_bruta_12m
  FROM usr_sat_ods.ods_decl_dime_raw
  WHERE nu_per_ref BETWEEN 202001 AND 202509
    AND NU_CNPJ IS NOT NULL
)
SELECT
  b.nu_cnpj AS cnpj,

  -- Periodo 2021
  MAX(CASE WHEN b.nu_per_ref = 202101 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jan2021,
  MAX(CASE WHEN b.nu_per_ref = 202102 THEN b.vl_rec_bruta_12m ELSE 0 END) AS fev2021,
  MAX(CASE WHEN b.nu_per_ref = 202103 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mar2021,
  MAX(CASE WHEN b.nu_per_ref = 202104 THEN b.vl_rec_bruta_12m ELSE 0 END) AS abr2021,
  MAX(CASE WHEN b.nu_per_ref = 202105 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mai2021,
  MAX(CASE WHEN b.nu_per_ref = 202106 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jun2021,
  MAX(CASE WHEN b.nu_per_ref = 202107 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jul2021,
  MAX(CASE WHEN b.nu_per_ref = 202108 THEN b.vl_rec_bruta_12m ELSE 0 END) AS ago2021,
  MAX(CASE WHEN b.nu_per_ref = 202109 THEN b.vl_rec_bruta_12m ELSE 0 END) AS set2021,
  MAX(CASE WHEN b.nu_per_ref = 202110 THEN b.vl_rec_bruta_12m ELSE 0 END) AS out2021,
  MAX(CASE WHEN b.nu_per_ref = 202111 THEN b.vl_rec_bruta_12m ELSE 0 END) AS nov2021,
  MAX(CASE WHEN b.nu_per_ref = 202112 THEN b.vl_rec_bruta_12m ELSE 0 END) AS dez2021,

  -- Periodo 2022
  MAX(CASE WHEN b.nu_per_ref = 202201 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jan2022,
  MAX(CASE WHEN b.nu_per_ref = 202202 THEN b.vl_rec_bruta_12m ELSE 0 END) AS fev2022,
  MAX(CASE WHEN b.nu_per_ref = 202203 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mar2022,
  MAX(CASE WHEN b.nu_per_ref = 202204 THEN b.vl_rec_bruta_12m ELSE 0 END) AS abr2022,
  MAX(CASE WHEN b.nu_per_ref = 202205 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mai2022,
  MAX(CASE WHEN b.nu_per_ref = 202206 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jun2022,
  MAX(CASE WHEN b.nu_per_ref = 202207 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jul2022,
  MAX(CASE WHEN b.nu_per_ref = 202208 THEN b.vl_rec_bruta_12m ELSE 0 END) AS ago2022,
  MAX(CASE WHEN b.nu_per_ref = 202209 THEN b.vl_rec_bruta_12m ELSE 0 END) AS set2022,
  MAX(CASE WHEN b.nu_per_ref = 202210 THEN b.vl_rec_bruta_12m ELSE 0 END) AS out2022,
  MAX(CASE WHEN b.nu_per_ref = 202211 THEN b.vl_rec_bruta_12m ELSE 0 END) AS nov2022,
  MAX(CASE WHEN b.nu_per_ref = 202212 THEN b.vl_rec_bruta_12m ELSE 0 END) AS dez2022,

  -- Periodo 2023
  MAX(CASE WHEN b.nu_per_ref = 202301 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jan2023,
  MAX(CASE WHEN b.nu_per_ref = 202302 THEN b.vl_rec_bruta_12m ELSE 0 END) AS fev2023,
  MAX(CASE WHEN b.nu_per_ref = 202303 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mar2023,
  MAX(CASE WHEN b.nu_per_ref = 202304 THEN b.vl_rec_bruta_12m ELSE 0 END) AS abr2023,
  MAX(CASE WHEN b.nu_per_ref = 202305 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mai2023,
  MAX(CASE WHEN b.nu_per_ref = 202306 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jun2023,
  MAX(CASE WHEN b.nu_per_ref = 202307 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jul2023,
  MAX(CASE WHEN b.nu_per_ref = 202308 THEN b.vl_rec_bruta_12m ELSE 0 END) AS ago2023,
  MAX(CASE WHEN b.nu_per_ref = 202309 THEN b.vl_rec_bruta_12m ELSE 0 END) AS set2023,
  MAX(CASE WHEN b.nu_per_ref = 202310 THEN b.vl_rec_bruta_12m ELSE 0 END) AS out2023,
  MAX(CASE WHEN b.nu_per_ref = 202311 THEN b.vl_rec_bruta_12m ELSE 0 END) AS nov2023,
  MAX(CASE WHEN b.nu_per_ref = 202312 THEN b.vl_rec_bruta_12m ELSE 0 END) AS dez2023,

  -- Periodo 2024
  MAX(CASE WHEN b.nu_per_ref = 202401 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jan2024,
  MAX(CASE WHEN b.nu_per_ref = 202402 THEN b.vl_rec_bruta_12m ELSE 0 END) AS fev2024,
  MAX(CASE WHEN b.nu_per_ref = 202403 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mar2024,
  MAX(CASE WHEN b.nu_per_ref = 202404 THEN b.vl_rec_bruta_12m ELSE 0 END) AS abr2024,
  MAX(CASE WHEN b.nu_per_ref = 202405 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mai2024,
  MAX(CASE WHEN b.nu_per_ref = 202406 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jun2024,
  MAX(CASE WHEN b.nu_per_ref = 202407 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jul2024,
  MAX(CASE WHEN b.nu_per_ref = 202408 THEN b.vl_rec_bruta_12m ELSE 0 END) AS ago2024,
  MAX(CASE WHEN b.nu_per_ref = 202409 THEN b.vl_rec_bruta_12m ELSE 0 END) AS set2024,
  MAX(CASE WHEN b.nu_per_ref = 202410 THEN b.vl_rec_bruta_12m ELSE 0 END) AS out2024,
  MAX(CASE WHEN b.nu_per_ref = 202411 THEN b.vl_rec_bruta_12m ELSE 0 END) AS nov2024,
  MAX(CASE WHEN b.nu_per_ref = 202412 THEN b.vl_rec_bruta_12m ELSE 0 END) AS dez2024,

  -- Periodo 2025
  MAX(CASE WHEN b.nu_per_ref = 202501 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jan2025,
  MAX(CASE WHEN b.nu_per_ref = 202502 THEN b.vl_rec_bruta_12m ELSE 0 END) AS fev2025,
  MAX(CASE WHEN b.nu_per_ref = 202503 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mar2025,
  MAX(CASE WHEN b.nu_per_ref = 202504 THEN b.vl_rec_bruta_12m ELSE 0 END) AS abr2025,
  MAX(CASE WHEN b.nu_per_ref = 202505 THEN b.vl_rec_bruta_12m ELSE 0 END) AS mai2025,
  MAX(CASE WHEN b.nu_per_ref = 202506 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jun2025,
  MAX(CASE WHEN b.nu_per_ref = 202507 THEN b.vl_rec_bruta_12m ELSE 0 END) AS jul2025,
  MAX(CASE WHEN b.nu_per_ref = 202508 THEN b.vl_rec_bruta_12m ELSE 0 END) AS ago2025,
  MAX(CASE WHEN b.nu_per_ref = 202509 THEN b.vl_rec_bruta_12m ELSE 0 END) AS set2025
FROM base b
JOIN gessimples.gei_cnpj gc ON b.nu_cnpj = gc.cnpj
WHERE b.vl_rec_bruta_12m IS NOT NULL
GROUP BY b.nu_cnpj;

COMPUTE STATS gessimples.gei_dime;
