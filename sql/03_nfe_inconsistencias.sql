-- =====================================================
-- GEI - SISTEMA DE DETECÇÃO DE GRUPOS ECONÔMICOS
-- 03 - NFe E VIEWS DE INCONSISTÊNCIAS
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-10
-- Descrição: View de NFe unificada e detecção de inconsistências
-- =====================================================

-- =====================================================
-- 1. GEI_NFE - View unificada de NFe + NFCe
-- =====================================================

DROP VIEW IF EXISTS gessimples.gei_nfe;

CREATE VIEW gessimples.gei_nfe AS
-- NFe (Notas Fiscais Eletrônicas)
SELECT
    a.chave AS nfe_nu_chave_acesso,
    (a.ano_emissao * 100 + a.mes_emissao) AS nfe_per_ref,
    a.dhemi_orig AS nfe_dt_emissao,
    a.ip_transmissor AS nfe_ip_transmissao,
    a.situacao AS nfe_situacao,
    gc_emit.num_grupo AS grupo_emit,
    a.procnfe.nfe.infnfe.emit.cnpj AS nfe_cnpj_cpf_emit,
    a.procnfe.nfe.infnfe.emit.ie AS nfe_emit_ie,
    a.procnfe.nfe.infnfe.emit.enderemit.fone AS nfe_emit_telefone,
    a.procnfe.nfe.infnfe.emit.enderemit.xlgr AS nfe_emit_end_logradouro,
    a.procnfe.nfe.infnfe.emit.enderemit.nro AS nfe_emit_end_numero,
    a.procnfe.nfe.infnfe.emit.enderemit.xcpl AS nfe_emit_end_complemento,
    a.procnfe.nfe.infnfe.emit.enderemit.xbairro AS nfe_emit_end_bairro,
    a.procnfe.nfe.infnfe.emit.enderemit.xmun AS nfe_emit_end_municipio,
    a.procnfe.nfe.infnfe.emit.xfant AS nfe_emit_nm_fantasia,
    a.procnfe.nfe.infnfe.emit.xnome AS nfe_emit_razao_social,
    gc_dest.num_grupo AS grupo_dest,
    a.procnfe.nfe.infnfe.dest.cnpj AS nfe_cnpj_cpf_dest,
    a.procnfe.nfe.infnfe.dest.ie AS nfe_dest_ie,
    a.procnfe.nfe.infnfe.dest.email AS nfe_dest_email,
    a.procnfe.nfe.infnfe.dest.enderdest.fone AS nfe_dest_telefone,
    a.procnfe.nfe.infnfe.dest.enderdest.xlgr AS nfe_dest_end_logradouro,
    a.procnfe.nfe.infnfe.dest.enderdest.nro AS nfe_dest_end_numero,
    a.procnfe.nfe.infnfe.dest.enderdest.xcpl AS nfe_dest_end_complemento,
    a.procnfe.nfe.infnfe.dest.enderdest.xbairro AS nfe_dest_end_bairro,
    a.procnfe.nfe.infnfe.dest.enderdest.xmun AS nfe_dest_end_municipio,
    a.procnfe.nfe.infnfe.dest.xnome AS nfe_dest_razao_social,
    b.prod.cprod AS nfe_cd_produto,
    b.prod.xprod AS nfe_de_produto,
    CONCAT(
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.xlgr, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.nro, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.xcpl, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.xbairro, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.xmun, '')
    ) AS nfe_emit_end_completo,
    CONCAT(
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.xlgr, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.nro, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.xcpl, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.xbairro, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.xmun, '')
    ) AS nfe_dest_end_completo,
    'NFe' AS origem
FROM nfe.nfe a, a.procnfe.nfe.infnfe.det b
LEFT JOIN gessimples.gei_cnpj gc_emit
    ON gc_emit.cnpj = REGEXP_REPLACE(TRIM(CAST(a.procnfe.nfe.infnfe.emit.cnpj AS STRING)), '[^0-9]', '')
LEFT JOIN gessimples.gei_cnpj gc_dest
    ON gc_dest.cnpj = REGEXP_REPLACE(TRIM(CAST(a.procnfe.nfe.infnfe.dest.cnpj AS STRING)), '[^0-9]', '')
WHERE a.situacao = 1
  AND (gc_emit.num_grupo IS NOT NULL OR gc_dest.num_grupo IS NOT NULL)
  AND CAST((a.ano_emissao * 100 + a.mes_emissao) AS STRING) LIKE '2025%'

UNION ALL

-- NFCe (Notas Fiscais de Consumidor Eletrônicas)
SELECT
    a.chave AS nfe_nu_chave_acesso,
    (a.ano_emissao * 100 + a.mes_emissao) AS nfe_per_ref,
    a.dhemi_orig AS nfe_dt_emissao,
    a.ip_transmissor AS nfe_ip_transmissao,
    1 AS nfe_situacao,
    gc_emit.num_grupo AS grupo_emit,
    a.procnfe.nfe.infnfe.emit.cnpj AS nfe_cnpj_cpf_emit,
    NULL AS nfe_emit_ie,
    NULL AS nfe_emit_telefone,
    a.procnfe.nfe.infnfe.emit.enderemit.xlgr AS nfe_emit_end_logradouro,
    a.procnfe.nfe.infnfe.emit.enderemit.nro AS nfe_emit_end_numero,
    a.procnfe.nfe.infnfe.emit.enderemit.xcpl AS nfe_emit_end_complemento,
    a.procnfe.nfe.infnfe.emit.enderemit.xbairro AS nfe_emit_end_bairro,
    a.procnfe.nfe.infnfe.emit.enderemit.xmun AS nfe_emit_end_municipio,
    a.procnfe.nfe.infnfe.emit.xfant AS nfe_emit_nm_fantasia,
    a.procnfe.nfe.infnfe.emit.xnome AS nfe_emit_razao_social,
    gc_dest.num_grupo AS grupo_dest,
    a.procnfe.nfe.infnfe.dest.cnpj AS nfe_cnpj_cpf_dest,
    NULL AS nfe_dest_ie,
    a.procnfe.nfe.infnfe.dest.email AS nfe_dest_email,
    NULL AS nfe_dest_telefone,
    a.procnfe.nfe.infnfe.dest.enderdest.xlgr AS nfe_dest_end_logradouro,
    a.procnfe.nfe.infnfe.dest.enderdest.nro AS nfe_dest_end_numero,
    a.procnfe.nfe.infnfe.dest.enderdest.xcpl AS nfe_dest_end_complemento,
    a.procnfe.nfe.infnfe.dest.enderdest.xbairro AS nfe_dest_end_bairro,
    a.procnfe.nfe.infnfe.dest.enderdest.xmun AS nfe_dest_end_municipio,
    a.procnfe.nfe.infnfe.dest.xnome AS nfe_dest_razao_social,
    d.item.prod.cprod AS nfe_cd_produto,
    d.item.prod.xprod AS nfe_de_produto,
    CONCAT(
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.xlgr, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.nro, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.xcpl, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.xbairro, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.emit.enderemit.xmun, '')
    ) AS nfe_emit_end_completo,
    CONCAT(
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.xlgr, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.nro, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.xcpl, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.xbairro, ''), ' ',
        COALESCE(a.procnfe.nfe.infnfe.dest.enderdest.xmun, '')
    ) AS nfe_dest_end_completo,
    'NFCe' AS origem
FROM nfce.nfce a, a.procnfe.nfe.infnfe.det d
LEFT JOIN gessimples.gei_cnpj gc_emit
    ON gc_emit.cnpj = REGEXP_REPLACE(TRIM(CAST(a.procnfe.nfe.infnfe.emit.cnpj AS STRING)), '[^0-9]', '')
LEFT JOIN gessimples.gei_cnpj gc_dest
    ON gc_dest.cnpj = REGEXP_REPLACE(TRIM(CAST(a.procnfe.nfe.infnfe.dest.cnpj AS STRING)), '[^0-9]', '')
WHERE (gc_emit.num_grupo IS NOT NULL OR gc_dest.num_grupo IS NOT NULL)
  AND CAST((a.ano_emissao * 100 + a.mes_emissao) AS STRING) LIKE '2025%';

-- =====================================================
-- 2. VIEWS DE INCONSISTÊNCIAS - Detectam compartilhamentos
-- =====================================================

-- 2.1 IP de transmissão compartilhado
DROP VIEW IF EXISTS gessimples.gei_ip_incons;

CREATE VIEW gessimples.gei_ip_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_ip_transmissao AS ip_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_ip_transmissao IS NOT NULL
  AND TRIM(nfe_ip_transmissao) != ''
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_ip_transmissao
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- 2.2 Email de destinatário compartilhado
DROP VIEW IF EXISTS gessimples.gei_email_incons;

CREATE VIEW gessimples.gei_email_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_dest_email AS email_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_dest_email IS NOT NULL
  AND TRIM(nfe_dest_email) != ''
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_dest_email
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- 2.3 Telefone do emitente compartilhado
DROP VIEW IF EXISTS gessimples.gei_tel_emit_incons;

CREATE VIEW gessimples.gei_tel_emit_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_emit_telefone AS telefone_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_emit_telefone IS NOT NULL
  AND TRIM(nfe_emit_telefone) != ''
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_emit_telefone
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- 2.4 Telefone do destinatário compartilhado
DROP VIEW IF EXISTS gessimples.gei_tel_dest_incons;

CREATE VIEW gessimples.gei_tel_dest_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_dest_telefone AS telefone_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_dest_telefone IS NOT NULL
  AND TRIM(nfe_dest_telefone) != ''
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_dest_telefone
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- 2.5 Endereço do emitente compartilhado
DROP VIEW IF EXISTS gessimples.gei_end_emit_incons;

CREATE VIEW gessimples.gei_end_emit_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_emit_end_completo AS endereco_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_emit_end_completo IS NOT NULL
  AND TRIM(nfe_emit_end_completo) != ''
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_emit_end_completo
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- 2.6 Endereço do destinatário compartilhado
DROP VIEW IF EXISTS gessimples.gei_end_dest_incons;

CREATE VIEW gessimples.gei_end_dest_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_dest_end_completo AS endereco_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_dest_end_completo IS NOT NULL
  AND TRIM(nfe_dest_end_completo) != ''
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_dest_end_completo
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- 2.7 Cliente (CNPJ destinatário) compartilhado
DROP VIEW IF EXISTS gessimples.gei_clientes_incons;

CREATE VIEW gessimples.gei_clientes_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_cnpj_cpf_dest AS cliente_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_cnpj_cpf_dest IS NOT NULL
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_cnpj_cpf_dest
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- 2.8 Fornecedor (CNPJ emitente) compartilhado
DROP VIEW IF EXISTS gessimples.gei_fornecedores_incons;

CREATE VIEW gessimples.gei_fornecedores_incons AS
SELECT
    grupo_dest AS num_grupo,
    nfe_cnpj_cpf_emit AS fornecedor_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_dest) AS qtd_destinatarios,
    COLLECT_SET(nfe_cnpj_cpf_dest) AS cnpjs_destinatarios
FROM gessimples.gei_nfe
WHERE nfe_cnpj_cpf_emit IS NOT NULL
  AND grupo_dest IS NOT NULL
GROUP BY grupo_dest, nfe_cnpj_cpf_emit
HAVING COUNT(DISTINCT nfe_cnpj_cpf_dest) > 1;

-- 2.9 Código de produto compartilhado
DROP VIEW IF EXISTS gessimples.gei_codigos_incons;

CREATE VIEW gessimples.gei_codigos_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_cd_produto AS codigo_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_cd_produto IS NOT NULL
  AND TRIM(nfe_cd_produto) != ''
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_cd_produto
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- 2.10 Descrição de produto compartilhada
DROP VIEW IF EXISTS gessimples.gei_produtos_incons;

CREATE VIEW gessimples.gei_produtos_incons AS
SELECT
    grupo_emit AS num_grupo,
    nfe_de_produto AS produto_compartilhado,
    COUNT(DISTINCT nfe_cnpj_cpf_emit) AS qtd_emitentes,
    COLLECT_SET(nfe_cnpj_cpf_emit) AS cnpjs_emitentes
FROM gessimples.gei_nfe
WHERE nfe_de_produto IS NOT NULL
  AND TRIM(nfe_de_produto) != ''
  AND grupo_emit IS NOT NULL
GROUP BY grupo_emit, nfe_de_produto
HAVING COUNT(DISTINCT nfe_cnpj_cpf_emit) > 1;

-- =====================================================
-- 3. GEI_NFE_COMPLETO - NFe com flags de inconsistência
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_nfe_completo;

CREATE TABLE gessimples.gei_nfe_completo AS
SELECT
    n.*,
    CASE WHEN ip.ip_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_ip_compartilhado,
    CASE WHEN em.email_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_email_compartilhado,
    CASE WHEN te.telefone_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_tel_emit_compartilhado,
    CASE WHEN td.telefone_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_tel_dest_compartilhado,
    CASE WHEN ee.endereco_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_end_emit_compartilhado,
    CASE WHEN ed.endereco_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_end_dest_compartilhado,
    CASE WHEN cl.cliente_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_cliente_compartilhado,
    CASE WHEN fo.fornecedor_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_fornecedor_compartilhado,
    CASE WHEN cd.codigo_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_codigo_compartilhado,
    CASE WHEN pr.produto_compartilhado IS NOT NULL THEN 1 ELSE 0 END AS sn_produto_compartilhado,
    -- Score total de inconsistências
    (CASE WHEN ip.ip_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN em.email_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN te.telefone_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN td.telefone_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN ee.endereco_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN ed.endereco_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN cl.cliente_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN fo.fornecedor_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN cd.codigo_compartilhado IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN pr.produto_compartilhado IS NOT NULL THEN 1 ELSE 0 END
    ) AS score_inconsistencias
FROM gessimples.gei_nfe n
LEFT JOIN gessimples.gei_ip_incons ip ON n.grupo_emit = ip.num_grupo AND n.nfe_ip_transmissao = ip.ip_compartilhado
LEFT JOIN gessimples.gei_email_incons em ON n.grupo_emit = em.num_grupo AND n.nfe_dest_email = em.email_compartilhado
LEFT JOIN gessimples.gei_tel_emit_incons te ON n.grupo_emit = te.num_grupo AND n.nfe_emit_telefone = te.telefone_compartilhado
LEFT JOIN gessimples.gei_tel_dest_incons td ON n.grupo_emit = td.num_grupo AND n.nfe_dest_telefone = td.telefone_compartilhado
LEFT JOIN gessimples.gei_end_emit_incons ee ON n.grupo_emit = ee.num_grupo AND n.nfe_emit_end_completo = ee.endereco_compartilhado
LEFT JOIN gessimples.gei_end_dest_incons ed ON n.grupo_emit = ed.num_grupo AND n.nfe_dest_end_completo = ed.endereco_compartilhado
LEFT JOIN gessimples.gei_clientes_incons cl ON n.grupo_emit = cl.num_grupo AND n.nfe_cnpj_cpf_dest = cl.cliente_compartilhado
LEFT JOIN gessimples.gei_fornecedores_incons fo ON n.grupo_dest = fo.num_grupo AND n.nfe_cnpj_cpf_emit = fo.fornecedor_compartilhado
LEFT JOIN gessimples.gei_codigos_incons cd ON n.grupo_emit = cd.num_grupo AND n.nfe_cd_produto = cd.codigo_compartilhado
LEFT JOIN gessimples.gei_produtos_incons pr ON n.grupo_emit = pr.num_grupo AND n.nfe_de_produto = pr.produto_compartilhado;

COMPUTE STATS gessimples.gei_nfe_completo;
