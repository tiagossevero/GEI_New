-- =====================================================
-- GEI - SISTEMA DE DETECAO DE GRUPOS ECONOMICOS
-- ATUALIZACAO GEI_PERCENT com NF3e e NFCom
-- =====================================================
-- Autor: Sistema GEI
-- Data: 2025-12-18
-- Descricao: Adiciona dados de energia eletrica (NF3e) e
--            telecomunicacoes (NFCom) ao gei_percent
-- =====================================================

-- =====================================================
-- Parte 2 ATUALIZADA: Processamento dos dados com NF3e/NFCom
-- =====================================================
-- IMPORTANTE: Esta query substitui a query original do PERCENT 2
-- Incluindo os novos campos de energia e telecomunicacoes
-- =====================================================

DROP TABLE IF EXISTS gessimples.gei_percent;
SET REQUEST_POOL = 'default';
SET MEM_LIMIT = 20G;

CREATE TABLE gessimples.gei_percent AS
WITH faturamento_consolidado AS (
    -- ============================================================================
    -- >> CONSOLIDACAO PGDAS + DIME
    -- ============================================================================
    -- Dados do PGDAS (Simples Nacional)
    SELECT
        gc.num_grupo,
        gc.cnpj,
        COALESCE(pg.jan2021, 0) AS jan2021, COALESCE(pg.fev2021, 0) AS fev2021, COALESCE(pg.mar2021, 0) AS mar2021,
        COALESCE(pg.abr2021, 0) AS abr2021, COALESCE(pg.mai2021, 0) AS mai2021, COALESCE(pg.jun2021, 0) AS jun2021,
        COALESCE(pg.jul2021, 0) AS jul2021, COALESCE(pg.ago2021, 0) AS ago2021, COALESCE(pg.set2021, 0) AS set2021,
        COALESCE(pg.out2021, 0) AS out2021, COALESCE(pg.nov2021, 0) AS nov2021, COALESCE(pg.dez2021, 0) AS dez2021,
        COALESCE(pg.jan2022, 0) AS jan2022, COALESCE(pg.fev2022, 0) AS fev2022, COALESCE(pg.mar2022, 0) AS mar2022,
        COALESCE(pg.abr2022, 0) AS abr2022, COALESCE(pg.mai2022, 0) AS mai2022, COALESCE(pg.jun2022, 0) AS jun2022,
        COALESCE(pg.jul2022, 0) AS jul2022, COALESCE(pg.ago2022, 0) AS ago2022, COALESCE(pg.set2022, 0) AS set2022,
        COALESCE(pg.out2022, 0) AS out2022, COALESCE(pg.nov2022, 0) AS nov2022, COALESCE(pg.dez2022, 0) AS dez2022,
        COALESCE(pg.jan2023, 0) AS jan2023, COALESCE(pg.fev2023, 0) AS fev2023, COALESCE(pg.mar2023, 0) AS mar2023,
        COALESCE(pg.abr2023, 0) AS abr2023, COALESCE(pg.mai2023, 0) AS mai2023, COALESCE(pg.jun2023, 0) AS jun2023,
        COALESCE(pg.jul2023, 0) AS jul2023, COALESCE(pg.ago2023, 0) AS ago2023, COALESCE(pg.set2023, 0) AS set2023,
        COALESCE(pg.out2023, 0) AS out2023, COALESCE(pg.nov2023, 0) AS nov2023, COALESCE(pg.dez2023, 0) AS dez2023,
        COALESCE(pg.jan2024, 0) AS jan2024, COALESCE(pg.fev2024, 0) AS fev2024, COALESCE(pg.mar2024, 0) AS mar2024,
        COALESCE(pg.abr2024, 0) AS abr2024, COALESCE(pg.mai2024, 0) AS mai2024, COALESCE(pg.jun2024, 0) AS jun2024,
        COALESCE(pg.jul2024, 0) AS jul2024, COALESCE(pg.ago2024, 0) AS ago2024, COALESCE(pg.set2024, 0) AS set2024,
        COALESCE(pg.out2024, 0) AS out2024, COALESCE(pg.nov2024, 0) AS nov2024, COALESCE(pg.dez2024, 0) AS dez2024,
        COALESCE(pg.jan2025, 0) AS jan2025, COALESCE(pg.fev2025, 0) AS fev2025, COALESCE(pg.mar2025, 0) AS mar2025,
        COALESCE(pg.abr2025, 0) AS abr2025, COALESCE(pg.mai2025, 0) AS mai2025, COALESCE(pg.jun2025, 0) AS jun2025,
        COALESCE(pg.jul2025, 0) AS jul2025, COALESCE(pg.ago2025, 0) AS ago2025, COALESCE(pg.set2025, 0) AS set2025
    FROM gessimples.gei_cnpj gc
    JOIN gessimples.gei_pgdas pg ON gc.cnpj = pg.cnpj

    UNION ALL

    -- Dados da DIME (Regime Normal)
    SELECT
        gc.num_grupo,
        gc.cnpj,
        COALESCE(dm.jan2021, 0) AS jan2021, COALESCE(dm.fev2021, 0) AS fev2021, COALESCE(dm.mar2021, 0) AS mar2021,
        COALESCE(dm.abr2021, 0) AS abr2021, COALESCE(dm.mai2021, 0) AS mai2021, COALESCE(dm.jun2021, 0) AS jun2021,
        COALESCE(dm.jul2021, 0) AS jul2021, COALESCE(dm.ago2021, 0) AS ago2021, COALESCE(dm.set2021, 0) AS set2021,
        COALESCE(dm.out2021, 0) AS out2021, COALESCE(dm.nov2021, 0) AS nov2021, COALESCE(dm.dez2021, 0) AS dez2021,
        COALESCE(dm.jan2022, 0) AS jan2022, COALESCE(dm.fev2022, 0) AS fev2022, COALESCE(dm.mar2022, 0) AS mar2022,
        COALESCE(dm.abr2022, 0) AS abr2022, COALESCE(dm.mai2022, 0) AS mai2022, COALESCE(dm.jun2022, 0) AS jun2022,
        COALESCE(dm.jul2022, 0) AS jul2022, COALESCE(dm.ago2022, 0) AS ago2022, COALESCE(dm.set2022, 0) AS set2022,
        COALESCE(dm.out2022, 0) AS out2022, COALESCE(dm.nov2022, 0) AS nov2022, COALESCE(dm.dez2022, 0) AS dez2022,
        COALESCE(dm.jan2023, 0) AS jan2023, COALESCE(dm.fev2023, 0) AS fev2023, COALESCE(dm.mar2023, 0) AS mar2023,
        COALESCE(dm.abr2023, 0) AS abr2023, COALESCE(dm.mai2023, 0) AS mai2023, COALESCE(dm.jun2023, 0) AS jun2023,
        COALESCE(dm.jul2023, 0) AS jul2023, COALESCE(dm.ago2023, 0) AS ago2023, COALESCE(dm.set2023, 0) AS set2023,
        COALESCE(dm.out2023, 0) AS out2023, COALESCE(dm.nov2023, 0) AS nov2023, COALESCE(dm.dez2023, 0) AS dez2023,
        COALESCE(dm.jan2024, 0) AS jan2024, COALESCE(dm.fev2024, 0) AS fev2024, COALESCE(dm.mar2024, 0) AS mar2024,
        COALESCE(dm.abr2024, 0) AS abr2024, COALESCE(dm.mai2024, 0) AS mai2024, COALESCE(dm.jun2024, 0) AS jun2024,
        COALESCE(dm.jul2024, 0) AS jul2024, COALESCE(dm.ago2024, 0) AS ago2024, COALESCE(dm.set2024, 0) AS set2024,
        COALESCE(dm.out2024, 0) AS out2024, COALESCE(dm.nov2024, 0) AS nov2024, COALESCE(dm.dez2024, 0) AS dez2024,
        COALESCE(dm.jan2025, 0) AS jan2025, COALESCE(dm.fev2025, 0) AS fev2025, COALESCE(dm.mar2025, 0) AS mar2025,
        COALESCE(dm.abr2025, 0) AS abr2025, COALESCE(dm.mai2025, 0) AS mai2025, COALESCE(dm.jun2025, 0) AS jun2025,
        COALESCE(dm.jul2025, 0) AS jul2025, COALESCE(dm.ago2025, 0) AS ago2025, COALESCE(dm.set2025, 0) AS set2025
    FROM gessimples.gei_cnpj gc
    JOIN gessimples.gei_dime dm ON gc.cnpj = dm.cnpj
),
grupo AS (
    -- Agora soma os valores consolidados (PGDAS + DIME) por grupo
    SELECT
        num_grupo,
        SUM(jan2021) AS jan2021, SUM(fev2021) AS fev2021, SUM(mar2021) AS mar2021,
        SUM(abr2021) AS abr2021, SUM(mai2021) AS mai2021, SUM(jun2021) AS jun2021,
        SUM(jul2021) AS jul2021, SUM(ago2021) AS ago2021, SUM(set2021) AS set2021,
        SUM(out2021) AS out2021, SUM(nov2021) AS nov2021, SUM(dez2021) AS dez2021,
        SUM(jan2022) AS jan2022, SUM(fev2022) AS fev2022, SUM(mar2022) AS mar2022,
        SUM(abr2022) AS abr2022, SUM(mai2022) AS mai2022, SUM(jun2022) AS jun2022,
        SUM(jul2022) AS jul2022, SUM(ago2022) AS ago2022, SUM(set2022) AS set2022,
        SUM(out2022) AS out2022, SUM(nov2022) AS nov2022, SUM(dez2022) AS dez2022,
        SUM(jan2023) AS jan2023, SUM(fev2023) AS fev2023, SUM(mar2023) AS mar2023,
        SUM(abr2023) AS abr2023, SUM(mai2023) AS mai2023, SUM(jun2023) AS jun2023,
        SUM(jul2023) AS jul2023, SUM(ago2023) AS ago2023, SUM(set2023) AS set2023,
        SUM(out2023) AS out2023, SUM(nov2023) AS nov2023, SUM(dez2023) AS dez2023,
        SUM(jan2024) AS jan2024, SUM(fev2024) AS fev2024, SUM(mar2024) AS mar2024,
        SUM(abr2024) AS abr2024, SUM(mai2024) AS mai2024, SUM(jun2024) AS jun2024,
        SUM(jul2024) AS jul2024, SUM(ago2024) AS ago2024, SUM(set2024) AS set2024,
        SUM(out2024) AS out2024, SUM(nov2024) AS nov2024, SUM(dez2024) AS dez2024,
        SUM(jan2025) AS jan2025, SUM(fev2025) AS fev2025, SUM(mar2025) AS mar2025,
        SUM(abr2025) AS abr2025, SUM(mai2025) AS mai2025, SUM(jun2025) AS jun2025,
        SUM(jul2025) AS jul2025, SUM(ago2025) AS ago2025, SUM(set2025) AS set2025
    FROM faturamento_consolidado
    GROUP BY num_grupo
),
unpivoted AS (
    SELECT num_grupo, 'jan2021' AS periodo, jan2021 AS valor, 1 AS ord FROM grupo
    UNION ALL SELECT num_grupo, 'fev2021', fev2021, 2 FROM grupo
    UNION ALL SELECT num_grupo, 'mar2021', mar2021, 3 FROM grupo
    UNION ALL SELECT num_grupo, 'abr2021', abr2021, 4 FROM grupo
    UNION ALL SELECT num_grupo, 'mai2021', mai2021, 5 FROM grupo
    UNION ALL SELECT num_grupo, 'jun2021', jun2021, 6 FROM grupo
    UNION ALL SELECT num_grupo, 'jul2021', jul2021, 7 FROM grupo
    UNION ALL SELECT num_grupo, 'ago2021', ago2021, 8 FROM grupo
    UNION ALL SELECT num_grupo, 'set2021', set2021, 9 FROM grupo
    UNION ALL SELECT num_grupo, 'out2021', out2021, 10 FROM grupo
    UNION ALL SELECT num_grupo, 'nov2021', nov2021, 11 FROM grupo
    UNION ALL SELECT num_grupo, 'dez2021', dez2021, 12 FROM grupo
    UNION ALL SELECT num_grupo, 'jan2022', jan2022, 13 FROM grupo
    UNION ALL SELECT num_grupo, 'fev2022', fev2022, 14 FROM grupo
    UNION ALL SELECT num_grupo, 'mar2022', mar2022, 15 FROM grupo
    UNION ALL SELECT num_grupo, 'abr2022', abr2022, 16 FROM grupo
    UNION ALL SELECT num_grupo, 'mai2022', mai2022, 17 FROM grupo
    UNION ALL SELECT num_grupo, 'jun2022', jun2022, 18 FROM grupo
    UNION ALL SELECT num_grupo, 'jul2022', jul2022, 19 FROM grupo
    UNION ALL SELECT num_grupo, 'ago2022', ago2022, 20 FROM grupo
    UNION ALL SELECT num_grupo, 'set2022', set2022, 21 FROM grupo
    UNION ALL SELECT num_grupo, 'out2022', out2022, 22 FROM grupo
    UNION ALL SELECT num_grupo, 'nov2022', nov2022, 23 FROM grupo
    UNION ALL SELECT num_grupo, 'dez2022', dez2022, 24 FROM grupo
    UNION ALL SELECT num_grupo, 'jan2023', jan2023, 25 FROM grupo
    UNION ALL SELECT num_grupo, 'fev2023', fev2023, 26 FROM grupo
    UNION ALL SELECT num_grupo, 'mar2023', mar2023, 27 FROM grupo
    UNION ALL SELECT num_grupo, 'abr2023', abr2023, 28 FROM grupo
    UNION ALL SELECT num_grupo, 'mai2023', mai2023, 29 FROM grupo
    UNION ALL SELECT num_grupo, 'jun2023', jun2023, 30 FROM grupo
    UNION ALL SELECT num_grupo, 'jul2023', jul2023, 31 FROM grupo
    UNION ALL SELECT num_grupo, 'ago2023', ago2023, 32 FROM grupo
    UNION ALL SELECT num_grupo, 'set2023', set2023, 33 FROM grupo
    UNION ALL SELECT num_grupo, 'out2023', out2023, 34 FROM grupo
    UNION ALL SELECT num_grupo, 'nov2023', nov2023, 35 FROM grupo
    UNION ALL SELECT num_grupo, 'dez2023', dez2023, 36 FROM grupo
    UNION ALL SELECT num_grupo, 'jan2024', jan2024, 37 FROM grupo
    UNION ALL SELECT num_grupo, 'fev2024', fev2024, 38 FROM grupo
    UNION ALL SELECT num_grupo, 'mar2024', mar2024, 39 FROM grupo
    UNION ALL SELECT num_grupo, 'abr2024', abr2024, 40 FROM grupo
    UNION ALL SELECT num_grupo, 'mai2024', mai2024, 41 FROM grupo
    UNION ALL SELECT num_grupo, 'jun2024', jun2024, 42 FROM grupo
    UNION ALL SELECT num_grupo, 'jul2024', jul2024, 43 FROM grupo
    UNION ALL SELECT num_grupo, 'ago2024', ago2024, 44 FROM grupo
    UNION ALL SELECT num_grupo, 'set2024', set2024, 45 FROM grupo
    UNION ALL SELECT num_grupo, 'out2024', out2024, 46 FROM grupo
    UNION ALL SELECT num_grupo, 'nov2024', nov2024, 47 FROM grupo
    UNION ALL SELECT num_grupo, 'dez2024', dez2024, 48 FROM grupo
    UNION ALL SELECT num_grupo, 'jan2025', jan2025, 49 FROM grupo
    UNION ALL SELECT num_grupo, 'fev2025', fev2025, 50 FROM grupo
    UNION ALL SELECT num_grupo, 'mar2025', mar2025, 51 FROM grupo
    UNION ALL SELECT num_grupo, 'abr2025', abr2025, 52 FROM grupo
    UNION ALL SELECT num_grupo, 'mai2025', mai2025, 53 FROM grupo
    UNION ALL SELECT num_grupo, 'jun2025', jun2025, 54 FROM grupo
    UNION ALL SELECT num_grupo, 'jul2025', jul2025, 55 FROM grupo
    UNION ALL SELECT num_grupo, 'ago2025', ago2025, 56 FROM grupo
    UNION ALL SELECT num_grupo, 'set2025', set2025, 57 FROM grupo
),
primeiro_excesso AS (
    SELECT num_grupo, periodo FROM (
        SELECT num_grupo, periodo, valor, ord, ROW_NUMBER() OVER (PARTITION BY num_grupo ORDER BY ord) AS rn
        FROM unpivoted WHERE valor >= 4800000
    ) t WHERE rn = 1
),
maximos AS (
    SELECT num_grupo, valor AS valor_max, periodo AS periodo_max FROM (
        SELECT num_grupo, periodo, valor, ROW_NUMBER() OVER (PARTITION BY num_grupo ORDER BY valor DESC) AS rn
        FROM unpivoted
    ) t WHERE rn = 1
),
c115_ranking AS (
    SELECT * FROM gessimples.gei_c115_ranking_risco_grupo_economico
),
c115_metricas AS (
    SELECT * FROM gessimples.gei_c115_metricas_grupos
)
-- Consulta final unindo todas as fontes de dados
SELECT
    -- COLUNAS ORIGINAIS
    i.num_grupo,
    i.qntd_cnpj,
    i.perc_cliente, i.perc_email, i.perc_tel_dest, i.perc_tel_emit,
    i.perc_codigo_produto, i.perc_fornecedor, i.perc_end_emit, i.perc_end_dest,
    i.perc_descricao_produto, i.perc_ip_transmissao,
    i.total_incons AS total,
    i.distinct_nfe,
    i.total_itens,
    i.nm_razao_social, i.nm_fantasia, i.cd_cnae, i.nm_contador, i.endereco,
    i.qntd_s, i.qntd_normal, i.qntd_sn,
    i.total_cadastro,
    primeiro_excesso.periodo AS periodo,
    maximos.valor_max,
    maximos.periodo_max,
    i.qtd_socios_compartilhados, i.max_empresas_por_socio, i.qtd_cnpjs_com_socios_compartilhados,
    i.qtd_pares_cnpjs_relacionados, i.perc_cnpjs_com_socios, i.indice_interconexao,

    -- Campos do C115
    rk.ranking_risco, rk.qtd_cnpjs_relacionados, rk.perc_cnpjs_relacionados,
    rk.max_tipos_identificadores_comuns, rk.max_total_identificadores_comuns,
    rk.pares_com_tres_tipos_comum, rk.pares_com_dois_ou_mais_tipos_comum,
    rk.nivel_risco_grupo_economico,
    mg.total_tomadores, mg.tomadores_com_compartilhamento, mg.tomadores_com_multiplos_compartilhamentos,
    mg.total_compartilhamentos, mg.perc_tomadores_com_compartilhamento, mg.indice_risco_grupo_economico,

    -- Campos de Indicios
    ind.qtd_total_indicios,
    ind.qtd_tipos_indicios_distintos,
    ind.qtd_cnpjs_com_indicios,
    ind.perc_cnpjs_com_indicios,
    ind.indice_risco_indicios,

    -- Campos de Pagamentos e Funcionarios
    COALESCE(pag.valor_meios_pagamento_empresas, 0) AS valor_meios_pagamento_empresas,
    COALESCE(pag.valor_meios_pagamento_socios, 0) AS valor_meios_pagamento_socios,
    COALESCE(func.total_funcionarios, 0) AS total_funcionarios,
    CASE
        WHEN COALESCE(pag.valor_meios_pagamento_empresas, 0) > 0
        THEN ROUND(COALESCE(pag.valor_meios_pagamento_socios, 0) / pag.valor_meios_pagamento_empresas, 4)
        ELSE 0
    END AS indice_risco_pagamentos,
    CASE
        WHEN COALESCE(maximos.valor_max, 0) > 0 AND (COALESCE(func.total_funcionarios, 0) + 1) > 0
        THEN (1 - (1 / (COALESCE(maximos.valor_max, 0) / (COALESCE(func.total_funcionarios, 0) + 1) / 100000)))
        ELSE 0
    END AS indice_risco_fat_func,

    -- ============================================================================
    -- >> CAMPOS CCS
    -- ============================================================================
    COALESCE(ccs.total_contas_unicas, 0) AS ccs_total_contas_unicas,
    COALESCE(ccs.qtd_contas_compartilhadas, 0) AS ccs_qtd_contas_compartilhadas,
    COALESCE(ccs.perc_contas_compartilhadas, 0) AS ccs_perc_contas_compartilhadas,
    COALESCE(ccs.max_cnpjs_por_conta, 0) AS ccs_max_cnpjs_por_conta,
    COALESCE(ccs.qtd_sobreposicoes_responsaveis, 0) AS ccs_qtd_sobreposicoes_responsaveis,
    COALESCE(ccs.media_dias_sobreposicao, 0) AS ccs_media_dias_sobreposicao,
    COALESCE(ccs.qtd_datas_abertura_coordenada, 0) AS ccs_qtd_datas_abertura_coordenada,
    COALESCE(ccs.qtd_datas_encerramento_coordenado, 0) AS ccs_qtd_datas_encerramento_coordenado,
    COALESCE(ccs.indice_risco_ccs, 0) AS indice_risco_ccs,
    COALESCE(ccs_rk.nivel_risco_ccs, 'INEXISTENTE') AS nivel_risco_ccs,

    -- ============================================================================
    -- >> CAMPOS NF3E - ENERGIA ELETRICA (NOVOS)
    -- ============================================================================
    COALESCE(nf3e.qt_empresas_consumidoras, 0) AS nf3e_qt_empresas,
    COALESCE(nf3e.vl_energia_grupo, 0) AS nf3e_vl_total,
    COALESCE(nf3e.qt_notas_grupo, 0) AS nf3e_qt_notas,
    COALESCE(nf3e.qt_fornecedores_grupo, 0) AS nf3e_qt_fornecedores,
    COALESCE(nf3e.vl_medio_nota_grupo, 0) AS nf3e_vl_medio_nota,
    COALESCE(nf3e.vl_max_nota_grupo, 0) AS nf3e_vl_max_nota,
    COALESCE(nf3e.media_meses_consumo, 0) AS nf3e_media_meses,
    -- Indice de risco baseado no consumo de energia vs faturamento
    CASE
        WHEN COALESCE(maximos.valor_max, 0) > 0 AND COALESCE(nf3e.vl_energia_grupo, 0) > 0
        THEN ROUND(nf3e.vl_energia_grupo / maximos.valor_max * 100, 2)
        ELSE 0
    END AS nf3e_perc_energia_faturamento,

    -- ============================================================================
    -- >> CAMPOS NFCOM - TELECOMUNICACOES (NOVOS)
    -- ============================================================================
    COALESCE(nfcom.qt_empresas_consumidoras, 0) AS nfcom_qt_empresas,
    COALESCE(nfcom.vl_telecom_grupo, 0) AS nfcom_vl_total,
    COALESCE(nfcom.qt_notas_grupo, 0) AS nfcom_qt_notas,
    COALESCE(nfcom.qt_operadoras_grupo, 0) AS nfcom_qt_operadoras,
    COALESCE(nfcom.vl_medio_nota_grupo, 0) AS nfcom_vl_medio_nota,
    COALESCE(nfcom.vl_max_nota_grupo, 0) AS nfcom_vl_max_nota,
    COALESCE(nfcom.media_meses_consumo, 0) AS nfcom_media_meses,
    -- Indice de risco baseado no consumo de telecom vs faturamento
    CASE
        WHEN COALESCE(maximos.valor_max, 0) > 0 AND COALESCE(nfcom.vl_telecom_grupo, 0) > 0
        THEN ROUND(nfcom.vl_telecom_grupo / maximos.valor_max * 100, 2)
        ELSE 0
    END AS nfcom_perc_telecom_faturamento,

    -- ============================================================================
    -- >> METRICAS COMBINADAS ENERGIA + TELECOM
    -- ============================================================================
    COALESCE(nf3e.vl_energia_grupo, 0) + COALESCE(nfcom.vl_telecom_grupo, 0) AS vl_utilidades_total,
    CASE
        WHEN COALESCE(maximos.valor_max, 0) > 0
        THEN ROUND((COALESCE(nf3e.vl_energia_grupo, 0) + COALESCE(nfcom.vl_telecom_grupo, 0)) / maximos.valor_max * 100, 2)
        ELSE 0
    END AS perc_utilidades_faturamento,

    -- Scores antigos (mantidos para referencia)
    ROUND((COALESCE(i.total_incons, 0) * 0.4) + (COALESCE(i.total_cadastro, 0) * 0.2) + (COALESCE(i.indice_interconexao * 10, 0) * 0.4), 2) AS score_combinado,
    CASE WHEN mg.indice_risco_grupo_economico IS NOT NULL THEN ROUND((COALESCE(i.total_incons, 0) * 0.3) + (COALESCE(i.total_cadastro, 0) * 0.2) + (COALESCE(i.indice_interconexao * 10, 0) * 0.3) + (COALESCE(mg.indice_risco_grupo_economico, 0) * 0.2), 2) ELSE ROUND((COALESCE(i.total_incons, 0) * 0.4) + (COALESCE(i.total_cadastro, 0) * 0.2) + (COALESCE(i.indice_interconexao * 10, 0) * 0.4), 2) END AS score_combinado_c115,

    -- Score final original (com indicios)
    CASE
        WHEN mg.indice_risco_grupo_economico IS NOT NULL THEN
            ROUND(
                (COALESCE(i.total_incons, 0) * 0.25) +
                (COALESCE(i.total_cadastro, 0) * 0.15) +
                (COALESCE(i.indice_interconexao, 0) * 10 * 0.25) +
                (COALESCE(mg.indice_risco_grupo_economico, 0) * 0.15) +
                (COALESCE(ind.indice_risco_indicios, 0) * 20 * 0.20)
            , 2)
        ELSE
            ROUND(
                (COALESCE(i.total_incons, 0) * 0.35) +
                (COALESCE(i.total_cadastro, 0) * 0.15) +
                (COALESCE(i.indice_interconexao, 0) * 10 * 0.30) +
                (COALESCE(ind.indice_risco_indicios, 0) * 20 * 0.20)
            , 2)
    END AS score_final_completo,

    -- Score com pagamentos e funcionarios
    CASE
        WHEN mg.indice_risco_grupo_economico IS NOT NULL THEN
            ROUND(
                (COALESCE(i.total_incons, 0) * 0.20) +
                (COALESCE(i.total_cadastro, 0) * 0.10) +
                (COALESCE(i.indice_interconexao, 0) * 10 * 0.20) +
                (COALESCE(mg.indice_risco_grupo_economico, 0) * 0.10) +
                (COALESCE(ind.indice_risco_indicios, 0) * 20 * 0.15) +
                (CASE WHEN COALESCE(pag.valor_meios_pagamento_empresas, 0) > 0 THEN ROUND(COALESCE(pag.valor_meios_pagamento_socios, 0) / pag.valor_meios_pagamento_empresas, 4) ELSE 0 END * 0.15) +
                (CASE WHEN COALESCE(maximos.valor_max, 0) > 0 AND (COALESCE(func.total_funcionarios, 0) + 1) > 0 THEN (1 - (1 / (COALESCE(maximos.valor_max,0) / (COALESCE(func.total_funcionarios, 0) + 1) / 100000))) ELSE 0 END * 0.10)
            , 2)
        ELSE
            ROUND(
                (COALESCE(i.total_incons, 0) * 0.25) +
                (COALESCE(i.total_cadastro, 0) * 0.10) +
                (COALESCE(i.indice_interconexao, 0) * 10 * 0.25) +
                (COALESCE(ind.indice_risco_indicios, 0) * 20 * 0.15) +
                (CASE WHEN COALESCE(pag.valor_meios_pagamento_empresas, 0) > 0 THEN ROUND(COALESCE(pag.valor_meios_pagamento_socios, 0) / pag.valor_meios_pagamento_empresas, 4) ELSE 0 END * 0.15) +
                (CASE WHEN COALESCE(maximos.valor_max, 0) > 0 AND (COALESCE(func.total_funcionarios, 0) + 1) > 0 THEN (1 - (1 / (COALESCE(maximos.valor_max,0) / (COALESCE(func.total_funcionarios, 0) + 1) / 100000))) ELSE 0 END * 0.10)
            , 2)
    END AS score_final_avancado,

    -- ============================================================================
    -- >> SCORE FINAL COM CCS (15% DE PESO)
    -- ============================================================================
    CASE
        WHEN mg.indice_risco_grupo_economico IS NOT NULL THEN
            ROUND(
                (COALESCE(i.total_incons, 0) * 0.17) +
                (COALESCE(i.total_cadastro, 0) * 0.08) +
                (COALESCE(i.indice_interconexao, 0) * 10 * 0.17) +
                (COALESCE(mg.indice_risco_grupo_economico, 0) * 0.08) +
                (COALESCE(ind.indice_risco_indicios, 0) * 20 * 0.13) +
                (CASE WHEN COALESCE(pag.valor_meios_pagamento_empresas, 0) > 0
                  THEN ROUND(COALESCE(pag.valor_meios_pagamento_socios, 0) / pag.valor_meios_pagamento_empresas, 4)
                  ELSE 0 END * 0.12) +
                (CASE WHEN COALESCE(maximos.valor_max, 0) > 0 AND (COALESCE(func.total_funcionarios, 0) + 1) > 0
                  THEN (1 - (1 / (COALESCE(maximos.valor_max,0) / (COALESCE(func.total_funcionarios, 0) + 1) / 100000)))
                  ELSE 0 END * 0.10) +
                (COALESCE(ccs.indice_risco_ccs, 0) * 0.15)
            , 2)
        ELSE
            ROUND(
                (COALESCE(i.total_incons, 0) * 0.22) +
                (COALESCE(i.total_cadastro, 0) * 0.08) +
                (COALESCE(i.indice_interconexao, 0) * 10 * 0.22) +
                (COALESCE(ind.indice_risco_indicios, 0) * 20 * 0.13) +
                (CASE WHEN COALESCE(pag.valor_meios_pagamento_empresas, 0) > 0
                  THEN ROUND(COALESCE(pag.valor_meios_pagamento_socios, 0) / pag.valor_meios_pagamento_empresas, 4)
                  ELSE 0 END * 0.12) +
                (CASE WHEN COALESCE(maximos.valor_max, 0) > 0 AND (COALESCE(func.total_funcionarios, 0) + 1) > 0
                  THEN (1 - (1 / (COALESCE(maximos.valor_max,0) / (COALESCE(func.total_funcionarios, 0) + 1) / 100000)))
                  ELSE 0 END * 0.08) +
                (COALESCE(ccs.indice_risco_ccs, 0) * 0.15)
            , 2)
    END AS score_final_ccs

FROM gessimples.gei_percent_intermediario i
LEFT JOIN primeiro_excesso ON i.num_grupo = primeiro_excesso.num_grupo
LEFT JOIN maximos ON i.num_grupo = maximos.num_grupo
LEFT JOIN c115_ranking rk ON i.num_grupo = rk.num_grupo
LEFT JOIN c115_metricas mg ON i.num_grupo = mg.num_grupo
LEFT JOIN gessimples.gei_indicios_metricas_grupo ind ON i.num_grupo = ind.num_grupo
LEFT JOIN gessimples.gei_pagamentos_metricas_grupo pag ON i.num_grupo = pag.num_grupo
LEFT JOIN gessimples.gei_funcionarios_metricas_grupo func ON i.num_grupo = func.num_grupo
-- JOINS COM CCS
LEFT JOIN gessimples.gei_ccs_metricas_grupo ccs ON i.num_grupo = ccs.num_grupo
LEFT JOIN gessimples.gei_ccs_ranking_risco ccs_rk ON i.num_grupo = ccs_rk.num_grupo
-- ============================================================================
-- >> NOVOS JOINS COM NF3E E NFCOM
-- ============================================================================
LEFT JOIN gessimples.gei_nf3e_metricas_grupo nf3e ON i.num_grupo = nf3e.num_grupo
LEFT JOIN gessimples.gei_nfcom_metricas_grupo nfcom ON i.num_grupo = nfcom.num_grupo

ORDER BY
    score_final_ccs DESC,
    score_final_avancado DESC,
    score_final_completo DESC
LIMIT 10000;

COMPUTE STATS gessimples.gei_percent;
