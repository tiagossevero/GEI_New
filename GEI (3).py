# ============================================================
# COLE ESTE CÓDIGO NO INÍCIO DE CADA ARQUIVO .PY
# ============================================================
import streamlit as st
import hashlib

# DEFINA A SENHA AQUI
SENHA = "tsevero654"  # ← TROQUE para cada projeto

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.markdown("<div style='text-align: center; padding: 50px;'><h1>🔐 Acesso Restrito</h1></div>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            senha_input = st.text_input("Digite a senha:", type="password", key="pwd_input")
            if st.button("Entrar", use_container_width=True):
                if senha_input == SENHA:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("❌ Senha incorreta")
        st.stop()

check_password()

"""
Sistema GEI - Dashboard de Monitoramento Fiscal v3.0
Versão Completa com Análises Avançadas
Receita Estadual de Santa Catarina
"""

# =============================================================================
# IMPORTS E CONFIGURAÇÕES INICIAIS
# =============================================================================

import pandas as pd
import numpy as np
import plotly.express as px
import folium
from streamlit_folium import st_folium
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
from datetime import datetime
from sqlalchemy import create_engine
import warnings
import ssl
import openpyxl
from io import BytesIO
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
import os
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.ensemble import IsolationForest
import numpy as np

os.environ['PYTHONWARNINGS'] = 'ignore::DeprecationWarning'

warnings.filterwarnings('ignore')

# Configuração SSL - CORRIGIDO
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# Configuração Streamlit
st.set_page_config(
    page_title="GEI - Monitoramento Fiscal",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS Customizado
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }

    /* ESTILO DOS GRÁFICOS (PLOTLY) */
    div[data-testid="stPlotlyChart"] {
        border: 2px solid #e0e0e0;       /* Borda: 2px, sólida, cor cinza-claro */
        border-radius: 10px;             /* Cantos arredondados (mesmo dos KPIs) */
        padding: 10px;                   /* Espaçamento interno (ajuste conforme gosto) */
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); /* Sombra suave */
        background-color: #ffffff;       /* Fundo branco (opcional) */
    }
    
    /* ESTILO DOS KPIs - BORDA PRETA */
    div[data-testid="stMetric"] {
        background-color: #ffffff;        /* Fundo branco */
        border: 2px solid #2c3e50;        /* Borda: 2px de largura, sólida, cor cinza-escuro */
        border-radius: 10px;              /* Cantos arredondados (10 pixels de raio) */
        padding: 15px;                    /* Espaçamento interno (15px em todos os lados) */
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);  /* Sombra: horizontal=0, vertical=2px, blur=4px, cor preta 10% opacidade */
    }
    
    /* Título do métrica */
    div[data-testid="stMetric"] > label {
        font-weight: 600;                 /* Negrito médio */
        color: #2c3e50;                   /* Cor do texto */
    }
    
    /* Valor do métrica */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;                /* Tamanho da fonte do valor */
        font-weight: bold;                /* Negrito */
        color: #1f77b4;                   /* Cor azul */
    }
    
    /* Delta (variação) */
    div[data-testid="stMetricDelta"] {
        font-size: 0.9rem;                /* Tamanho menor para delta */
    }
    
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }

    /* =========================================================================
       SIDEBAR SEMPRE COLAPSADO - EXPANDE AO PASSAR O MOUSE
       ========================================================================= */

    /* Sidebar sempre colapsado por padrão */
    section[data-testid="stSidebar"] {
        width: 0px !important;
        min-width: 0px !important;
        transform: translateX(-100%);
        transition: transform 0.3s ease-in-out, width 0.3s ease-in-out;
    }

    /* Expande ao passar o mouse ou focar */
    section[data-testid="stSidebar"]:hover,
    section[data-testid="stSidebar"]:focus-within {
        width: 300px !important;
        min-width: 300px !important;
        transform: translateX(0);
    }

    /* Indicador visual para expandir (hambúrguer) */
    section[data-testid="stSidebar"]::before {
        content: "☰";
        position: absolute;
        right: -30px;
        top: 50%;
        transform: translateY(-50%);
        font-size: 24px;
        color: #1565C0;
        cursor: pointer;
        z-index: 1000;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# CONFIGURAÇÕES DE CONEXÃO
# =============================================================================

IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'gessimples'

try:
    IMPALA_USER = st.secrets["impala_credentials"]["user"]
    IMPALA_PASSWORD = st.secrets["impala_credentials"]["password"]
except:
    st.error("Configure as credenciais no arquivo .streamlit/secrets.toml")
    st.stop()

# =============================================================================
# FUNÇÕES DE CONEXÃO E CARREGAMENTO
# =============================================================================

@st.cache_resource
def get_impala_engine():
    """Cria engine de conexão com Impala"""
    try:
        engine = create_engine(
            f'impala://{IMPALA_HOST}:{IMPALA_PORT}/{DATABASE}',
            connect_args={
                'user': IMPALA_USER,
                'password': IMPALA_PASSWORD,
                'auth_mechanism': 'LDAP',
                'use_ssl': True
            }
        )
        connection = engine.connect()
        connection.close()
        return engine
    except Exception as e:
        st.error(f"Erro ao conectar ao Impala: {e}")
        return None

@st.cache_data(ttl=3600, show_spinner="Carregando dados principais...")
def carregar_todos_os_dados(_engine):
    """Carrega datasets principais do Sistema GEI"""
    dados = {}
    
    if _engine is None:
        return dados
    
    tabelas_principais = {
        'percent': ('gei_percent', None),
        'cnpj': ('gei_cnpj', 50000),
        'cadastro': ('gei_cadastro', 50000),
        'contador': ('gei_contador', None),
        'socios_compartilhados': ('gei_socios_compartilhados', 30000),
        'c115_ranking': ('gei_c115_ranking_risco_grupo_economico', None),
        'funcionarios_metricas': ('gei_funcionarios_metricas_grupo', None),
        'pagamentos_metricas': ('gei_pagamentos_metricas_grupo', None),
        'c115_metricas': ('gei_c115_metricas_grupos', None),
        'ccs_metricas': ('gei_ccs_metricas_grupo', None),
        'ccs_ranking': ('gei_ccs_ranking_risco', None)
    }
    
    st.sidebar.write("**Status do Carregamento:**")
    
    for key, (tablename, limit) in tabelas_principais.items():
        try:
            st.sidebar.write(f"⏳ {tablename}...")
            
            if limit:
                query = f"SELECT * FROM {DATABASE}.{tablename} LIMIT {limit}"
            else:
                query = f"SELECT * FROM {DATABASE}.{tablename}"
            
            df = pd.read_sql(query, _engine)
            df.columns = [col.lower() for col in df.columns]
            dados[key] = df
            st.sidebar.success(f"✔️ {tablename} ({len(df):,})")
        except Exception as e:
            st.sidebar.warning(f"⚠️ {tablename}: {str(e)[:50]}")
            dados[key] = pd.DataFrame()
    
    return dados

@st.cache_data(ttl=3600)
def executar_query_analise(_engine, query_name, query_sql):
    """Executa uma query de análise e retorna o resultado"""
    try:
        df = pd.read_sql(query_sql, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Erro ao executar {query_name}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def carregar_dossie_completo(_engine, num_grupo):
    """Carrega todos os dados de um grupo para o dossiê - VERSÃO CORRIGIDA"""
    dossie = {}
    
    # Garantir que num_grupo seja string para as comparações
    num_grupo_str = str(num_grupo)
    
    # =========================================================================
    # 1. DADOS PRINCIPAIS DO GRUPO (gei_percent)
    # =========================================================================
    try:
        query_principal = f"""
        SELECT *
        FROM {DATABASE}.gei_percent
        WHERE num_grupo = '{num_grupo_str}'
        """
        dossie['principal'] = pd.read_sql(query_principal, _engine)
    except Exception as e:
        print(f"Erro ao carregar dados principais: {e}")
        dossie['principal'] = pd.DataFrame()
    
    # =========================================================================
    # 2. CNPJs DO GRUPO
    # =========================================================================
    try:
        query_cnpjs = f"""
        SELECT 
            g.cnpj,
            c.nm_razao_social,
            c.nm_fantasia,
            c.cd_cnae,
            c.nm_reg_apuracao,
            c.dt_constituicao_empresa,
            c.nm_munic as nm_municipio,
            c.nm_contador
        FROM {DATABASE}.gei_cnpj g
        LEFT JOIN usr_sat_ods.vw_ods_contrib c ON g.cnpj = c.nu_cnpj
        WHERE g.num_grupo = '{num_grupo_str}'
        """
        dossie['cnpjs'] = pd.read_sql(query_cnpjs, _engine)
        
        if dossie['cnpjs'].empty:
            # Fallback: buscar apenas CNPJs sem JOIN
            query_cnpjs_simples = f"""
            SELECT cnpj
            FROM {DATABASE}.gei_cnpj
            WHERE num_grupo = '{num_grupo_str}'
            """
            dossie['cnpjs'] = pd.read_sql(query_cnpjs_simples, _engine)
    except Exception as e:
        print(f"Erro ao carregar CNPJs: {e}")
        dossie['cnpjs'] = pd.DataFrame()
    
    # =========================================================================
    # 3. SÓCIOS COMPARTILHADOS
    # =========================================================================
    try:
        query_socios = f"""
        SELECT 
            cpf_socio,
            qtd_empresas
        FROM {DATABASE}.gei_socios_compartilhados
        WHERE num_grupo = '{num_grupo_str}'
        ORDER BY qtd_empresas DESC
        """
        dossie['socios'] = pd.read_sql(query_socios, _engine)
    except Exception as e:
        print(f"Erro ao carregar sócios: {e}")
        dossie['socios'] = pd.DataFrame()
    
    # =========================================================================
    # 4. INDÍCIOS FISCAIS
    # =========================================================================
    try:
        query_indicios = f"""
        SELECT 
            tx_descricao_indicio,
            cnpj,
            tx_descricao_complemento
        FROM {DATABASE}.gei_indicios
        WHERE num_grupo = '{num_grupo_str}'
        """
        dossie['indicios'] = pd.read_sql(query_indicios, _engine)
    except Exception as e:
        print(f"Erro ao carregar indícios: {e}")
        dossie['indicios'] = pd.DataFrame()
    
    # =========================================================================
    # 5. FUNCIONÁRIOS - BUSCAR DE gei_funcionarios_metricas_grupo
    # =========================================================================
    try:
        query_func = f"""
        SELECT 
            num_grupo,
            total_funcionarios,
            cnpjs_com_funcionarios
        FROM {DATABASE}.gei_funcionarios_metricas_grupo
        WHERE num_grupo = '{num_grupo_str}'
        """
        dossie['funcionarios'] = pd.read_sql(query_func, _engine)
    except Exception as e:
        print(f"Erro ao carregar funcionários: {e}")
        dossie['funcionarios'] = pd.DataFrame()
    
    # =========================================================================
    # 6. MEIOS DE PAGAMENTO - BUSCAR DE gei_pagamentos_metricas_grupo
    # =========================================================================
    try:
        query_pag = f"""
        SELECT 
            num_grupo,
            valor_meios_pagamento_empresas,
            valor_meios_pagamento_socios
        FROM {DATABASE}.gei_pagamentos_metricas_grupo
        WHERE num_grupo = '{num_grupo_str}'
        """
        dossie['pagamentos'] = pd.read_sql(query_pag, _engine)
    except Exception as e:
        print(f"Erro ao carregar pagamentos: {e}")
        dossie['pagamentos'] = pd.DataFrame()
    
    # =========================================================================
    # 7. CONVÊNIO 115 - BUSCAR DE gei_c115_ranking_risco_grupo_economico
    # =========================================================================
    try:
        query_c115 = f"""
        SELECT 
            num_grupo,
            ranking_risco,
            nivel_risco_grupo_economico,
            indice_risco_grupo_economico,
            qtd_cnpjs_relacionados,
            perc_cnpjs_relacionados,
            total_tomadores,
            tomadores_com_compartilhamento,
            total_compartilhamentos
        FROM {DATABASE}.gei_c115_ranking_risco_grupo_economico
        WHERE num_grupo = '{num_grupo_str}'
        """
        dossie['c115'] = pd.read_sql(query_c115, _engine)
    except Exception as e:
        print(f"Erro ao carregar C115: {e}")
        dossie['c115'] = pd.DataFrame()
    
    # =========================================================================
    # 8. CCS - CONTAS COMPARTILHADAS
    # =========================================================================
    try:
        query_ccs = f"""
        SELECT 
            nr_cpf,
            nm_banco,
            cd_agencia,
            nr_conta,
            qtd_cnpjs_usando_conta,
            qtd_vinculos_ativos,
            status_conta
        FROM {DATABASE}.gei_ccs_cpf_compartilhado
        WHERE num_grupo = '{num_grupo_str}'
        ORDER BY qtd_cnpjs_usando_conta DESC
        LIMIT 50
        """
        dossie['ccs_compartilhadas'] = pd.read_sql(query_ccs, _engine)
    except Exception as e:
        print(f"Erro ao carregar CCS compartilhadas: {e}")
        dossie['ccs_compartilhadas'] = pd.DataFrame()
    
    # =========================================================================
    # 9. CCS - SOBREPOSIÇÕES DE RESPONSÁVEIS
    # =========================================================================
    try:
        query_sobreposicoes = f"""
        SELECT 
            nr_cpf,
            cnpj1,
            cnpj2,
            nm_responsavel,
            inicio1,
            fim1,
            inicio2,
            fim2,
            dias_sobreposicao
        FROM {DATABASE}.gei_ccs_sobreposicao_responsaveis
        WHERE num_grupo = '{num_grupo_str}'
        ORDER BY dias_sobreposicao DESC
        LIMIT 50
        """
        dossie['ccs_sobreposicoes'] = pd.read_sql(query_sobreposicoes, _engine)
    except Exception as e:
        print(f"Erro ao carregar sobreposições: {e}")
        dossie['ccs_sobreposicoes'] = pd.DataFrame()
    
    # =========================================================================
    # 10. CCS - PADRÕES COORDENADOS
    # =========================================================================
    try:
        query_padroes = f"""
        SELECT 
            tipo_evento,
            dt_evento,
            qtd_cnpjs,
            qtd_contas,
            qtd_cpfs_distintos
        FROM {DATABASE}.gei_ccs_padroes_coordenados
        WHERE num_grupo = '{num_grupo_str}'
        ORDER BY dt_evento DESC
        LIMIT 50
        """
        dossie['ccs_padroes'] = pd.read_sql(query_padroes, _engine)
    except Exception as e:
        print(f"Erro ao carregar padrões coordenados: {e}")
        dossie['ccs_padroes'] = pd.DataFrame()
    
    # =========================================================================
    # 11. INCONSISTÊNCIAS NFE
    # =========================================================================
    try:
        query_incons = f"""
        SELECT
            nfe_nu_chave_acesso,
            nfe_dt_emissao,
            nfe_cnpj_cpf_emit,
            nfe_cnpj_cpf_dest,
            nfe_dest_email,
            nfe_dest_telefone,
            nfe_emit_telefone,
            nfe_cd_produto,
            nfe_de_produto,
            nfe_emit_end_completo,
            nfe_dest_end_completo,
            nfe_ip_transmissao,
            cliente_incons,
            email_incons,
            tel_dest_incons,
            tel_emit_incons,
            codigo_produto_incons,
            fornecedor_incons,
            end_emit_incons,
            end_dest_incons,
            descricao_produto_incons,
            ip_transmissao_incons
        FROM {DATABASE}.gei_nfe_completo
        WHERE grupo_emit = '{num_grupo_str}' OR grupo_dest = '{num_grupo_str}'
        LIMIT 1000
        """
        dossie['inconsistencias'] = pd.read_sql(query_incons, _engine)
    except Exception as e:
        print(f"Erro ao carregar inconsistências NFe: {e}")
        dossie['inconsistencias'] = pd.DataFrame()

    # =========================================================================
    # 12. FATURAMENTO (PGDAS + DIME CONSOLIDADO)
    # =========================================================================
    try:
        # Buscar CNPJs do grupo primeiro
        cnpjs_grupo = dossie['cnpjs']['cnpj'].tolist() if not dossie['cnpjs'].empty else []

        if cnpjs_grupo:
            cnpjs_str = "', '".join([str(c) for c in cnpjs_grupo])

            # Query PGDAS
            query_pgdas = f"""
            SELECT
                cnpj,
                jan2025, fev2025, mar2025, abr2025, mai2025, jun2025,
                jul2025, ago2025, set2025,
                'PGDAS' as fonte
            FROM gessimples.gei_pgdas
            WHERE cnpj IN ('{cnpjs_str}')
            """
            df_pgdas = pd.read_sql(query_pgdas, _engine)

            # Query DIME
            query_dime = f"""
            SELECT
                cnpj,
                jan2025, fev2025, mar2025, abr2025, mai2025, jun2025,
                jul2025, ago2025, set2025,
                'DIME' as fonte
            FROM gessimples.gei_dime
            WHERE cnpj IN ('{cnpjs_str}')
            """
            try:
                df_dime = pd.read_sql(query_dime, _engine)
            except:
                df_dime = pd.DataFrame()

            # Consolidar
            if not df_pgdas.empty or not df_dime.empty:
                dossie['faturamento'] = pd.concat([df_pgdas, df_dime], ignore_index=True)
            else:
                dossie['faturamento'] = pd.DataFrame()
        else:
            dossie['faturamento'] = pd.DataFrame()
    except Exception as e:
        print(f"Erro ao carregar faturamento: {e}")
        dossie['faturamento'] = pd.DataFrame()

    # =========================================================================
    # 13. ENERGIA ELÉTRICA (NF3e)
    # =========================================================================
    try:
        # Buscar CNPJs do grupo
        cnpjs_grupo = dossie['cnpjs']['cnpj'].tolist() if not dossie['cnpjs'].empty else []

        if cnpjs_grupo:
            cnpjs_str = "', '".join([str(c) for c in cnpjs_grupo])

            # Query NF3e - Consumo de energia elétrica
            query_nf3e = f"""
            SELECT
                cnpj,
                jan2024, fev2024, mar2024, abr2024, mai2024, jun2024,
                jul2024, ago2024, set2024, out2024, nov2024, dez2024,
                jan2025, fev2025, mar2025, abr2025, mai2025, jun2025,
                jul2025, ago2025, set2025
            FROM gessimples.gei_nf3e
            WHERE cnpj IN ('{cnpjs_str}')
            """
            dossie['nf3e'] = pd.read_sql(query_nf3e, _engine)

            # Query métricas do grupo
            query_nf3e_metricas = f"""
            SELECT *
            FROM gessimples.gei_nf3e_metricas_grupo
            WHERE num_grupo = '{num_grupo_str}'
            """
            try:
                dossie['nf3e_metricas'] = pd.read_sql(query_nf3e_metricas, _engine)
            except:
                dossie['nf3e_metricas'] = pd.DataFrame()

            # Query detalhado mensal
            query_nf3e_det = f"""
            SELECT *
            FROM gessimples.gei_nf3e_detalhado
            WHERE num_grupo = '{num_grupo_str}'
            ORDER BY ano_emissao DESC, mes_emissao DESC
            """
            try:
                dossie['nf3e_detalhado'] = pd.read_sql(query_nf3e_det, _engine)
            except:
                dossie['nf3e_detalhado'] = pd.DataFrame()
        else:
            dossie['nf3e'] = pd.DataFrame()
            dossie['nf3e_metricas'] = pd.DataFrame()
            dossie['nf3e_detalhado'] = pd.DataFrame()
    except Exception as e:
        print(f"Erro ao carregar NF3e (energia): {e}")
        dossie['nf3e'] = pd.DataFrame()
        dossie['nf3e_metricas'] = pd.DataFrame()
        dossie['nf3e_detalhado'] = pd.DataFrame()

    # =========================================================================
    # 14. TELECOMUNICAÇÕES (NFCom)
    # =========================================================================
    try:
        # Buscar CNPJs do grupo
        cnpjs_grupo = dossie['cnpjs']['cnpj'].tolist() if not dossie['cnpjs'].empty else []

        if cnpjs_grupo:
            cnpjs_str = "', '".join([str(c) for c in cnpjs_grupo])

            # Query NFCom - Consumo de telecomunicações
            query_nfcom = f"""
            SELECT
                cnpj,
                jan2024, fev2024, mar2024, abr2024, mai2024, jun2024,
                jul2024, ago2024, set2024, out2024, nov2024, dez2024,
                jan2025, fev2025, mar2025, abr2025, mai2025, jun2025,
                jul2025, ago2025, set2025
            FROM gessimples.gei_nfcom
            WHERE cnpj IN ('{cnpjs_str}')
            """
            dossie['nfcom'] = pd.read_sql(query_nfcom, _engine)

            # Query métricas do grupo
            query_nfcom_metricas = f"""
            SELECT *
            FROM gessimples.gei_nfcom_metricas_grupo
            WHERE num_grupo = '{num_grupo_str}'
            """
            try:
                dossie['nfcom_metricas'] = pd.read_sql(query_nfcom_metricas, _engine)
            except:
                dossie['nfcom_metricas'] = pd.DataFrame()

            # Query detalhado mensal
            query_nfcom_det = f"""
            SELECT *
            FROM gessimples.gei_nfcom_detalhado
            WHERE num_grupo = '{num_grupo_str}'
            ORDER BY ano_emissao DESC, mes_emissao DESC
            """
            try:
                dossie['nfcom_detalhado'] = pd.read_sql(query_nfcom_det, _engine)
            except:
                dossie['nfcom_detalhado'] = pd.DataFrame()

            # Query por operadora
            query_nfcom_op = f"""
            SELECT *
            FROM gessimples.gei_nfcom_por_operadora
            WHERE num_grupo = '{num_grupo_str}'
            ORDER BY vl_total DESC
            """
            try:
                dossie['nfcom_operadoras'] = pd.read_sql(query_nfcom_op, _engine)
            except:
                dossie['nfcom_operadoras'] = pd.DataFrame()
        else:
            dossie['nfcom'] = pd.DataFrame()
            dossie['nfcom_metricas'] = pd.DataFrame()
            dossie['nfcom_detalhado'] = pd.DataFrame()
            dossie['nfcom_operadoras'] = pd.DataFrame()
    except Exception as e:
        print(f"Erro ao carregar NFCom (telecom): {e}")
        dossie['nfcom'] = pd.DataFrame()
        dossie['nfcom_metricas'] = pd.DataFrame()
        dossie['nfcom_detalhado'] = pd.DataFrame()
        dossie['nfcom_operadoras'] = pd.DataFrame()

    return dossie

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def aplicar_filtros(df, filtros):
    """Aplica filtros aos dados"""
    if df.empty:
        return df
    
    df_filtrado = df.copy()
    
    if 'score_final_ccs' in df_filtrado.columns:
        df_filtrado = df_filtrado[
            (df_filtrado['score_final_ccs'] >= filtros['score_min']) &
            (df_filtrado['score_final_ccs'] <= filtros['score_max'])
        ]
    elif 'score_final_avancado' in df_filtrado.columns:
        df_filtrado = df_filtrado[
            (df_filtrado['score_final_avancado'] >= filtros['score_min']) &
            (df_filtrado['score_final_avancado'] <= filtros['score_max'])
        ]
    
    if 'qntd_cnpj' in df_filtrado.columns:
        df_filtrado = df_filtrado[
            (df_filtrado['qntd_cnpj'] >= filtros['cnpj_min']) &
            (df_filtrado['qntd_cnpj'] <= filtros['cnpj_max'])
        ]
    
    if filtros['com_indicios'] and 'qtd_total_indicios' in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado['qtd_total_indicios'] > 0]
    
    return df_filtrado

def formatar_moeda(valor):
    """Formata valores monetários"""
    if pd.isna(valor):
        return "N/A"
    if valor >= 1e9:
        return f"R$ {valor/1e9:.1f}B"
    elif valor >= 1e6:
        return f"R$ {valor/1e6:.1f}M"
    elif valor >= 1e3:
        return f"R$ {valor/1e3:.1f}K"
    else:
        return f"R$ {valor:.2f}"

def analise_machine_learning(engine, dados, filtros):
    """Análise de Machine Learning para identificação de grupos econômicos"""
    
    st.markdown("<h1 class='main-header'>🤖 Machine Learning - Identificação de Grupos Econômicos</h1>", unsafe_allow_html=True)
    
    st.info("""
    Este módulo utiliza algoritmos de aprendizado não supervisionado para identificar automaticamente
    padrões que caracterizam grupos econômicos com base nos scores e métricas já calculados pelo sistema GEI.
    """)
    
    # SEÇÃO 1: CONFIGURAÇÃO DO MODELO
    st.header("1. Configuração do Modelo")
    
    # ADICIONAR OPÇÃO DE MODO
    modo_analise = st.radio(
        "Modo de Análise:",
        ["Individual (escolher algoritmo)", "Consenso (executar todos os 3 modelos)"],
        help="Individual: executa apenas 1 algoritmo | Consenso: executa os 3 e compara resultados"
    )
    
    if modo_analise == "Individual (escolher algoritmo)":
        col1, col2, col3 = st.columns(3)
        
        with col1:
            algoritmo = st.selectbox(
                "Algoritmo de Clustering:",
                ["K-Means", "DBSCAN", "Isolation Forest"],
                help="K-Means: Rápido e eficiente | DBSCAN: Detecta outliers | Isolation Forest: Identifica anomalias"
            )
        
        with col2:
            if algoritmo == "K-Means":
                n_clusters = st.slider("Número de Clusters", 2, 5, 2)
            elif algoritmo == "DBSCAN":
                eps = st.slider("Epsilon (eps)", 0.1, 2.0, 0.5, 0.1)
                min_samples = st.slider("Min Samples", 2, 10, 3)
        
        with col3:
            usar_pca = st.checkbox("Usar PCA (Redução de Dimensionalidade)", value=True)
            if usar_pca:
                n_components_pca = st.slider("Componentes PCA", 2, 10, 3)
    else:
        # Modo consenso - configurações fixas otimizadas
        st.info("""
        **Modo Consenso Ativado:**
        - Executará K-Means (2 clusters), DBSCAN e Isolation Forest
        - Comparará os resultados dos 3 algoritmos
        - Grupos identificados por múltiplos modelos têm maior confiança
        """)
        usar_pca = True
        n_components_pca = 3
    
    # Botão para carregar dados
    if st.button("🔄 Carregar Dados dos Grupos", type="primary"):
        with st.spinner("Carregando dados..."):
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            status_text.text("Carregando dados agregados da tabela gei_percent...")
            progress_bar.progress(30)
            
            query_grupos = """
            SELECT 
                num_grupo,
                qntd_cnpj as qtd_cnpjs,
                
                -- Scores já calculados
                COALESCE(score_final_ccs, 0) as score_final_ccs,
                COALESCE(score_final_avancado, 0) as score_final_avancado,
                COALESCE(total, 0) as score_inconsistencias_nfe,
                
                -- Métricas Cadastrais
                CASE WHEN nm_razao_social = 'S' THEN 1 ELSE 0 END as razao_social_identica,
                CASE WHEN nm_fantasia = 'S' THEN 1 ELSE 0 END as fantasia_identica,
                CASE WHEN cd_cnae = 'S' THEN 1 ELSE 0 END as cnae_identico,
                CASE WHEN nm_contador = 'S' THEN 1 ELSE 0 END as contador_identico,
                CASE WHEN endereco = 'S' THEN 1 ELSE 0 END as endereco_identico,
                qntd_sn + qntd_normal + qntd_s as total_regimes,
                
                -- Métricas Financeiras
                COALESCE(valor_max, 0) as receita_maxima,
                CASE WHEN valor_max > 4800000 THEN 1 ELSE 0 END as acima_limite_sn,
                
                -- Vínculos Societários
                COALESCE(qtd_socios_compartilhados, 0) as socios_compartilhados,
                COALESCE(max_empresas_por_socio, 0) as max_empresas_socio,
                COALESCE(indice_interconexao, 0) as indice_interconexao,
                COALESCE(perc_cnpjs_com_socios, 0) as perc_cnpjs_com_socios,
                
                -- Convênio 115
                COALESCE(indice_risco_grupo_economico, 0) as indice_risco_c115,
                COALESCE(perc_cnpjs_relacionados, 0) as perc_cnpjs_relacionados_c115,
                COALESCE(total_compartilhamentos, 0) as total_compartilhamentos_c115,
                CASE 
                    WHEN nivel_risco_grupo_economico = 'CRÍTICO' THEN 3
                    WHEN nivel_risco_grupo_economico = 'ALTO' THEN 2
                    WHEN nivel_risco_grupo_economico = 'MÉDIO' THEN 1
                    ELSE 0
                END as nivel_risco_c115_num,
                
                -- Indícios Fiscais
                COALESCE(qtd_total_indicios, 0) as total_indicios,
                COALESCE(qtd_tipos_indicios_distintos, 0) as tipos_indicios_distintos,
                COALESCE(perc_cnpjs_com_indicios, 0) as perc_cnpjs_com_indicios,
                COALESCE(indice_risco_indicios, 0) as indice_risco_indicios,
                
                -- Meios de Pagamento
                COALESCE(valor_meios_pagamento_empresas, 0) as pagamentos_empresas,
                COALESCE(valor_meios_pagamento_socios, 0) as pagamentos_socios,
                COALESCE(indice_risco_pagamentos, 0) as indice_risco_pagamentos,
                
                -- Funcionários
                COALESCE(total_funcionarios, 0) as total_funcionarios,
                COALESCE(indice_risco_fat_func, 0) as indice_risco_fat_func,
                
                -- CCS (Contas Bancárias)
                COALESCE(ccs_qtd_contas_compartilhadas, 0) as contas_compartilhadas,
                COALESCE(ccs_perc_contas_compartilhadas, 0) as perc_contas_compartilhadas,
                COALESCE(ccs_max_cnpjs_por_conta, 0) as max_cnpjs_por_conta,
                COALESCE(ccs_qtd_sobreposicoes_responsaveis, 0) as sobreposicoes_responsaveis,
                COALESCE(indice_risco_ccs, 0) as indice_risco_ccs,
                CASE 
                    WHEN nivel_risco_ccs = 'CRÍTICO' THEN 3
                    WHEN nivel_risco_ccs = 'ALTO' THEN 2
                    WHEN nivel_risco_ccs = 'MÉDIO' THEN 1
                    ELSE 0
                END as nivel_risco_ccs_num,
                
                -- Inconsistências NFe (detalhadas)
                COALESCE(perc_cliente, 0) as perc_cliente_incons,
                COALESCE(perc_email, 0) as perc_email_incons,
                COALESCE(perc_tel_dest, 0) as perc_tel_dest_incons,
                COALESCE(perc_tel_emit, 0) as perc_tel_emit_incons,
                COALESCE(perc_codigo_produto, 0) as perc_codigo_produto_incons,
                COALESCE(perc_fornecedor, 0) as perc_fornecedor_incons,
                COALESCE(perc_end_emit, 0) as perc_end_emit_incons,
                COALESCE(perc_end_dest, 0) as perc_end_dest_incons,
                COALESCE(perc_descricao_produto, 0) as perc_descricao_produto_incons,
                COALESCE(perc_ip_transmissao, 0) as perc_ip_transmissao_incons,
                COALESCE(distinct_nfe, 0) as total_nfe_analisadas
                
            FROM gessimples.gei_percent
            WHERE qntd_cnpj > 1
            ORDER BY score_final_ccs DESC
            LIMIT 10000
            """
            
            progress_bar.progress(60)
            df_grupos = pd.read_sql(query_grupos, engine)
            
            if df_grupos.empty:
                st.error("Nenhum grupo encontrado com múltiplos CNPJs.")
                progress_bar.empty()
                status_text.empty()
                return
            
            progress_bar.progress(90)
            status_text.text("Processando dados...")
            
            # Preencher NaN com 0
            df_grupos = df_grupos.fillna(0)
            
            # Calcular score percentual customizado
            status_text.text("Calculando scores customizados...")
            
            scores_customizados = []
            for _, grupo in df_grupos.iterrows():
                score = 0
                max_score = 0
                
                # 1. Cadastro (peso 10 pontos)
                max_score += 10
                score += grupo['razao_social_identica'] * 2
                score += grupo['fantasia_identica'] * 1
                score += grupo['cnae_identico'] * 1
                score += grupo['contador_identico'] * 3
                score += grupo['endereco_identico'] * 3
                
                # 2. Sócios (peso 8 pontos)
                max_score += 8
                if grupo['socios_compartilhados'] > 0:
                    score += min(5, grupo['socios_compartilhados'] * 0.5)
                score += min(3, grupo['indice_interconexao'] * 3)
                
                # 3. Financeiro (peso 7 pontos)
                max_score += 7
                score += grupo['acima_limite_sn'] * 5
                if grupo['receita_maxima'] > 4800000 and grupo['qtd_cnpjs'] > 1:
                    excesso_normalizado = min(2, (grupo['receita_maxima'] - 4800000) / 4800000)
                    score += excesso_normalizado
                
                # 4. C115 (peso 5 pontos)
                max_score += 5
                score += min(3, grupo['indice_risco_c115'] / 10)
                score += min(2, grupo['nivel_risco_c115_num'] * 0.67)
                
                # 5. Indícios (peso 5 pontos)
                max_score += 5
                if grupo['total_indicios'] > 0:
                    score += min(5, grupo['total_indicios'] * 0.2)
                
                # 6. CCS (peso 5 pontos)
                max_score += 5
                if grupo['contas_compartilhadas'] > 0:
                    score += min(3, grupo['contas_compartilhadas'] * 0.5)
                score += min(2, grupo['nivel_risco_ccs_num'] * 0.67)
                
                # 7. NFe (peso 5 pontos)
                max_score += 5
                score_nfe = (grupo['perc_cliente_incons'] + grupo['perc_email_incons'] + 
                           grupo['perc_tel_dest_incons'] + grupo['perc_tel_emit_incons'] +
                           grupo['perc_ip_transmissao_incons']) / 5
                score += min(5, score_nfe * 5)
                
                # 8. Pagamentos (peso 3 pontos)
                max_score += 3
                if grupo['pagamentos_socios'] > 0:
                    score += min(3, grupo['indice_risco_pagamentos'] * 100)
                
                # 9. Funcionários (peso 2 pontos)
                max_score += 2
                if grupo['total_funcionarios'] > 0 and grupo['receita_maxima'] > 0:
                    receita_por_func = grupo['receita_maxima'] / (grupo['total_funcionarios'] + 1)
                    if receita_por_func > 500000:
                        score += 2
                    elif receita_por_func > 300000:
                        score += 1
                
                percentual = (score / max_score * 100) if max_score > 0 else 0
                
                scores_customizados.append({
                    'score_ml_absoluto': score,
                    'score_ml_maximo': max_score,
                    'score_ml_percentual': percentual
                })
            
            df_scores = pd.DataFrame(scores_customizados)
            df_grupos = pd.concat([df_grupos, df_scores], axis=1)
            
            progress_bar.progress(100)
            status_text.text("Concluído!")
            
            import time
            time.sleep(0.5)
            progress_bar.empty()
            status_text.empty()
            
            st.session_state['df_grupos_ml'] = df_grupos
            st.success(f"✅ {len(df_grupos)} grupos carregados com sucesso!")
    
    # Verificar se dados foram carregados
    if 'df_grupos_ml' not in st.session_state:
        st.warning("⚠️ Clique em 'Carregar Dados dos Grupos' para começar.")
        return
    
    df_grupos = st.session_state['df_grupos_ml']
    
    # SEÇÃO 2: ANÁLISE EXPLORATÓRIA
    st.header("2. Análise Exploratória dos Dados")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de Grupos", len(df_grupos))
    with col2:
        st.metric("Score Médio (GEI)", f"{df_grupos['score_final_ccs'].mean():.1f}")
    with col3:
        st.metric("Score Médio (ML)", f"{df_grupos['score_ml_percentual'].mean():.1f}%")
    with col4:
        st.metric("Acima Limite SN", int(df_grupos['acima_limite_sn'].sum()))
    
    # Distribuição de scores
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.histogram(df_grupos, x='score_ml_percentual', nbins=20,
                          title="Distribuição de Scores ML",
                          labels={'score_ml_percentual': 'Score ML (%)', 'count': 'Frequência'},
                          template=filtros['tema'])
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.scatter(df_grupos, x='score_final_ccs', y='score_ml_percentual',
                        hover_data=['num_grupo', 'qtd_cnpjs'],
                        title="Score GEI vs Score ML",
                        labels={'score_final_ccs': 'Score GEI', 'score_ml_percentual': 'Score ML (%)'},
                        template=filtros['tema'])
        st.plotly_chart(fig, use_container_width=True)
    
    # Correlação entre features
    st.subheader("Matriz de Correlação das Features Principais")
    
    features_correlacao = [
        'score_ml_percentual', 'score_final_ccs', 'qtd_cnpjs',
        'socios_compartilhados', 'indice_interconexao', 'receita_maxima',
        'indice_risco_c115', 'total_indicios', 'indice_risco_ccs',
        'contas_compartilhadas', 'score_inconsistencias_nfe'
    ]
    
    corr_matrix = df_grupos[features_correlacao].corr()
    
    fig = px.imshow(corr_matrix,
                   text_auto='.2f',
                   aspect="auto",
                   title="Correlação entre Features Principais",
                   template=filtros['tema'],
                   color_continuous_scale='RdBu_r')
    st.plotly_chart(fig, use_container_width=True)
    
    # SEÇÃO 3: TREINAMENTO DO MODELO
    st.header("3. Treinamento do Modelo")
    
    # Definir features para o modelo
    features_para_modelo = [
        'qtd_cnpjs',
        'razao_social_identica', 'fantasia_identica', 'cnae_identico',
        'contador_identico', 'endereco_identico',
        'socios_compartilhados', 'indice_interconexao', 'perc_cnpjs_com_socios',
        'receita_maxima', 'acima_limite_sn',
        'indice_risco_c115', 'nivel_risco_c115_num',
        'total_indicios', 'indice_risco_indicios',
        'contas_compartilhadas', 'indice_risco_ccs', 'nivel_risco_ccs_num',
        'score_inconsistencias_nfe',
        'indice_risco_pagamentos', 'indice_risco_fat_func'
    ]
    
    if modo_analise == "Individual (escolher algoritmo)":
        # MODO INDIVIDUAL
        if st.button("🚀 Treinar Modelo", type="primary"):
            
            with st.spinner("Treinando modelo..."):
                
                X = df_grupos[features_para_modelo].fillna(0)
                
                # Normalização
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                # PCA (opcional)
                if usar_pca:
                    pca = PCA(n_components=n_components_pca)
                    X_transformed = pca.fit_transform(X_scaled)
                    
                    variancia_explicada = pca.explained_variance_ratio_.sum() * 100
                    st.info(f"✅ PCA aplicado: {n_components_pca} componentes explicam {variancia_explicada:.1f}% da variância")
                    
                    # Gráfico de variância explicada e features
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        fig = px.bar(
                            x=[f'PC{i+1}' for i in range(len(pca.explained_variance_ratio_))],
                            y=pca.explained_variance_ratio_,
                            title="Variância Explicada por Componente PCA",
                            labels={'x': 'Componente', 'y': 'Variância Explicada'},
                            template=filtros['tema']
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        # Mostrar quais features mais influenciam cada componente
                        st.write("**Top 3 Features por Componente:**")
                        
                        for i in range(min(3, pca.n_components)):
                            loadings = pca.components_[i]
                            top_indices = np.argsort(np.abs(loadings))[-3:][::-1]
                            
                            st.write(f"**PC{i+1}** ({pca.explained_variance_ratio_[i]*100:.1f}%):")
                            for idx in top_indices:
                                feature_name = features_para_modelo[idx]
                                peso = loadings[idx]
                                st.write(f"  • {feature_name}: {peso:.3f}")
                else:
                    X_transformed = X_scaled
                
                # Aplicar algoritmo escolhido
                if algoritmo == "K-Means":
                    modelo = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                    labels = modelo.fit_predict(X_transformed)
                    
                elif algoritmo == "DBSCAN":
                    modelo = DBSCAN(eps=eps, min_samples=min_samples)
                    labels = modelo.fit_predict(X_transformed)
                    
                elif algoritmo == "Isolation Forest":
                    modelo = IsolationForest(contamination=0.3, random_state=42)
                    predictions = modelo.fit_predict(X_transformed)
                    labels = (predictions == -1).astype(int)
                
                # Adicionar labels ao dataframe
                df_grupos['cluster'] = labels
                
                # Determinar qual cluster representa grupos econômicos
                if algoritmo in ["K-Means", "DBSCAN"]:
                    score_por_cluster = df_grupos.groupby('cluster')['score_ml_percentual'].mean()
                    cluster_grupo_economico = score_por_cluster.idxmax()
                    
                    df_grupos['eh_grupo_economico'] = df_grupos['cluster'].apply(
                        lambda x: 'Grupo Econômico' if x == cluster_grupo_economico else 'Não é Grupo'
                    )
                else:
                    df_grupos['eh_grupo_economico'] = df_grupos['cluster'].apply(
                        lambda x: 'Grupo Econômico' if x == 1 else 'Não é Grupo'
                    )
                
                st.session_state['df_grupos_ml'] = df_grupos
                st.session_state['modelo_ml'] = modelo
                st.session_state['scaler_ml'] = scaler
                if usar_pca:
                    st.session_state['pca_ml'] = pca
                
                st.success("✅ Modelo treinado com sucesso!")
    
    else:
        # MODO CONSENSO - EXECUTAR OS 3 MODELOS
        if st.button("🚀 Executar Análise de Consenso (3 Modelos)", type="primary"):
            
            with st.spinner("Executando análise com 3 algoritmos..."):
                
                X = df_grupos[features_para_modelo].fillna(0)
                
                # Normalização
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                # PCA
                pca = PCA(n_components=n_components_pca)
                X_transformed = pca.fit_transform(X_scaled)
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # ============================================================
                # MODELO 1: K-MEANS
                # ============================================================
                status_text.text("1/3 - Treinando K-Means...")
                progress_bar.progress(10)
                
                modelo_kmeans = KMeans(n_clusters=2, random_state=42, n_init=10)
                labels_kmeans = modelo_kmeans.fit_predict(X_transformed)
                
                # Determinar qual cluster é "Grupo Econômico"
                score_por_cluster_km = df_grupos.groupby(labels_kmeans)['score_ml_percentual'].mean()
                cluster_ge_km = score_por_cluster_km.idxmax()
                df_grupos['kmeans_eh_grupo'] = (labels_kmeans == cluster_ge_km).astype(int)
                
                progress_bar.progress(35)
                
                # ============================================================
                # MODELO 2: DBSCAN
                # ============================================================
                status_text.text("2/3 - Treinando DBSCAN...")
                progress_bar.progress(40)
                
                modelo_dbscan = DBSCAN(eps=0.5, min_samples=3)
                labels_dbscan = modelo_dbscan.fit_predict(X_transformed)
                
                # DBSCAN: -1 são outliers, determinar qual cluster tem maior score
                if len(np.unique(labels_dbscan[labels_dbscan != -1])) > 0:
                    df_temp = df_grupos.copy()
                    df_temp['cluster_dbscan'] = labels_dbscan
                    
                    # Considerar apenas clusters válidos (não outliers)
                    clusters_validos = df_temp[df_temp['cluster_dbscan'] != -1]
                    if not clusters_validos.empty:
                        score_por_cluster_db = clusters_validos.groupby('cluster_dbscan')['score_ml_percentual'].mean()
                        cluster_ge_db = score_por_cluster_db.idxmax()
                        df_grupos['dbscan_eh_grupo'] = (labels_dbscan == cluster_ge_db).astype(int)
                    else:
                        df_grupos['dbscan_eh_grupo'] = 0
                else:
                    df_grupos['dbscan_eh_grupo'] = 0
                
                progress_bar.progress(65)
                
                # ============================================================
                # MODELO 3: ISOLATION FOREST
                # ============================================================
                status_text.text("3/3 - Treinando Isolation Forest...")
                progress_bar.progress(70)
                
                modelo_iforest = IsolationForest(contamination=0.3, random_state=42)
                predictions_if = modelo_iforest.fit_predict(X_transformed)
                
                # -1 = anomalia (grupo econômico suspeito), 1 = normal
                df_grupos['iforest_eh_grupo'] = (predictions_if == -1).astype(int)
                
                progress_bar.progress(90)
                
                # ============================================================
                # CALCULAR CONSENSO
                # ============================================================
                status_text.text("Calculando consenso...")
                
                df_grupos['votos_eh_grupo'] = (
                    df_grupos['kmeans_eh_grupo'] + 
                    df_grupos['dbscan_eh_grupo'] + 
                    df_grupos['iforest_eh_grupo']
                )
                
                # Classificação por consenso
                df_grupos['consenso_classificacao'] = df_grupos['votos_eh_grupo'].apply(
                    lambda x: 'CONSENSO FORTE (3/3)' if x == 3 else
                             'CONSENSO MODERADO (2/3)' if x == 2 else
                             'CONSENSO FRACO (1/3)' if x == 1 else
                             'NÃO É GRUPO (0/3)'
                )
                
                # Nível de confiança
                df_grupos['nivel_confianca'] = df_grupos['votos_eh_grupo'].apply(
                    lambda x: 'Muito Alto' if x == 3 else
                             'Alto' if x == 2 else
                             'Moderado' if x == 1 else
                             'Baixo'
                )
                
                progress_bar.progress(100)
                status_text.text("Concluído!")
                
                import time
                time.sleep(0.5)
                progress_bar.empty()
                status_text.empty()
                
                # Salvar modelos
                st.session_state['df_grupos_ml'] = df_grupos
                st.session_state['modelos_consenso'] = {
                    'kmeans': modelo_kmeans,
                    'dbscan': modelo_dbscan,
                    'iforest': modelo_iforest,
                    'scaler': scaler,
                    'pca': pca
                }
                
                st.success("✅ Análise de Consenso concluída com sucesso!")
    
    # ================================================================
    # SEÇÃO 4: RESULTADOS
    # ================================================================
    
    # VERIFICAR SE É MODO CONSENSO
    if 'votos_eh_grupo' in df_grupos.columns:
        
        st.header("4. Resultados da Análise de Consenso")
        
        st.info("""
        **Como interpretar os resultados:**
        - **3/3 votos**: Os 3 algoritmos concordam que é Grupo Econômico → **Confiança Muito Alta**
        - **2/3 votos**: 2 algoritmos indicam Grupo Econômico → **Confiança Alta**
        - **1/3 votos**: Apenas 1 algoritmo indica Grupo Econômico → **Confiança Moderada**
        - **0/3 votos**: Nenhum algoritmo indica Grupo Econômico → **Não é Grupo**
        """)
        
        # Métricas de consenso
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            consenso_forte = len(df_grupos[df_grupos['votos_eh_grupo'] == 3])
            st.metric("Consenso Forte (3/3)", consenso_forte, 
                     help="3 algoritmos concordam")
        
        with col2:
            consenso_moderado = len(df_grupos[df_grupos['votos_eh_grupo'] == 2])
            st.metric("Consenso Moderado (2/3)", consenso_moderado,
                     help="2 algoritmos concordam")
        
        with col3:
            consenso_fraco = len(df_grupos[df_grupos['votos_eh_grupo'] == 1])
            st.metric("Consenso Fraco (1/3)", consenso_fraco,
                     help="Apenas 1 algoritmo indica")
        
        with col4:
            nao_grupo = len(df_grupos[df_grupos['votos_eh_grupo'] == 0])
            st.metric("Não é Grupo (0/3)", nao_grupo,
                     help="Nenhum algoritmo indica")
        
        # Gráfico de distribuição de votos
        st.subheader("Distribuição de Consenso")
        
        col1, col2 = st.columns(2)
        
        with col1:
            dist_votos = df_grupos['votos_eh_grupo'].value_counts().sort_index()
            
            fig = px.bar(
                x=['0/3 votos', '1/3 votos', '2/3 votos', '3/3 votos'],
                y=[dist_votos.get(i, 0) for i in range(4)],
                title="Distribuição de Votos dos Algoritmos",
                labels={'x': 'Votos', 'y': 'Quantidade de Grupos'},
                template=filtros['tema'],
                color=[dist_votos.get(i, 0) for i in range(4)],
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.pie(
                df_grupos,
                names='consenso_classificacao',
                title="Proporção por Nível de Consenso",
                template=filtros['tema'],
                color_discrete_sequence=['#00CC96', '#FFA15A', '#EF553B', '#AB63FA']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Comparação Score ML vs Votos
        st.subheader("Análise: Score ML vs Consenso dos Algoritmos")
        
        fig = px.box(
            df_grupos,
            x='votos_eh_grupo',
            y='score_ml_percentual',
            color='votos_eh_grupo',
            title="Distribuição de Score ML por Número de Votos",
            labels={'votos_eh_grupo': 'Votos (Grupo Econômico)', 'score_ml_percentual': 'Score ML (%)'},
            template=filtros['tema']
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabela de concordância entre modelos
        st.subheader("Matriz de Concordância entre Algoritmos")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            concordancia_km_db = (df_grupos['kmeans_eh_grupo'] == df_grupos['dbscan_eh_grupo']).sum()
            perc_km_db = concordancia_km_db / len(df_grupos) * 100
            st.metric("K-Means ↔ DBSCAN", f"{perc_km_db:.1f}%",
                     help=f"{concordancia_km_db} grupos com classificação idêntica")
        
        with col2:
            concordancia_km_if = (df_grupos['kmeans_eh_grupo'] == df_grupos['iforest_eh_grupo']).sum()
            perc_km_if = concordancia_km_if / len(df_grupos) * 100
            st.metric("K-Means ↔ Isolation Forest", f"{perc_km_if:.1f}%",
                     help=f"{concordancia_km_if} grupos com classificação idêntica")
        
        with col3:
            concordancia_db_if = (df_grupos['dbscan_eh_grupo'] == df_grupos['iforest_eh_grupo']).sum()
            perc_db_if = concordancia_db_if / len(df_grupos) * 100
            st.metric("DBSCAN ↔ Isolation Forest", f"{perc_db_if:.1f}%",
                     help=f"{concordancia_db_if} grupos com classificação idêntica")
        
        # ================================================================
        # SEÇÃO 5: ANÁLISE DETALHADA POR NÍVEL DE CONSENSO
        # ================================================================
        st.header("5. Análise Detalhada por Nível de Consenso")
        
        tab1, tab2, tab3, tab4 = st.tabs([
            "🔴 Consenso Forte (3/3)",
            "🟡 Consenso Moderado (2/3)",
            "🟠 Consenso Fraco (1/3)",
            "🟢 Não é Grupo (0/3)"
        ])
        
        with tab1:
            grupos_3votos = df_grupos[df_grupos['votos_eh_grupo'] == 3].sort_values('score_ml_percentual', ascending=False)
            
            if not grupos_3votos.empty:
                st.success(f"**{len(grupos_3votos)} grupos com CONSENSO FORTE** - Todos os 3 algoritmos concordam")
                
                st.write("**Características destes grupos:**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Score ML Médio", f"{grupos_3votos['score_ml_percentual'].mean():.1f}%")
                with col2:
                    st.metric("Score GEI Médio", f"{grupos_3votos['score_final_ccs'].mean():.1f}")
                with col3:
                    acima_limite = (grupos_3votos['acima_limite_sn'] == 1).sum()
                    st.metric("Acima Limite SN", f"{acima_limite} ({acima_limite/len(grupos_3votos)*100:.1f}%)")
                
                # Tabela top 50
                colunas_exibir = [
                    'num_grupo', 'score_ml_percentual', 'score_final_ccs', 'qtd_cnpjs',
                    'socios_compartilhados', 'receita_maxima', 'total_indicios',
                    'contas_compartilhadas', 'kmeans_eh_grupo', 'dbscan_eh_grupo', 'iforest_eh_grupo'
                ]
                
                df_display = grupos_3votos[colunas_exibir].copy()
                df_display['receita_maxima'] = df_display['receita_maxima'].apply(formatar_moeda)
                df_display = df_display.rename(columns={
                    'kmeans_eh_grupo': 'K-Means',
                    'dbscan_eh_grupo': 'DBSCAN',
                    'iforest_eh_grupo': 'I.Forest'
                })
                
                st.dataframe(df_display.head(50), width='stretch', hide_index=True)
                
                # Seletor de grupo
                grupo_sel = st.selectbox(
                    "Selecione um grupo para análise detalhada:",
                    grupos_3votos['num_grupo'].tolist(),
                    format_func=lambda x: f"Grupo {x} - Score: {grupos_3votos[grupos_3votos['num_grupo']==x]['score_ml_percentual'].iloc[0]:.1f}%",
                    key="grupo_3votos"
                )
                
                if grupo_sel:
                    st.divider()
                    mostrar_detalhes_grupo(engine, grupo_sel, df_grupos, filtros)
            else:
                st.info("Nenhum grupo com consenso forte (3/3).")
        
        with tab2:
            grupos_2votos = df_grupos[df_grupos['votos_eh_grupo'] == 2].sort_values('score_ml_percentual', ascending=False)
            
            if not grupos_2votos.empty:
                st.warning(f"**{len(grupos_2votos)} grupos com CONSENSO MODERADO** - 2 algoritmos concordam")
                
                # Mostrar quais combinações de algoritmos
                st.write("**Combinações de Algoritmos:**")
                
                comb_km_db = len(grupos_2votos[
                    (grupos_2votos['kmeans_eh_grupo'] == 1) & 
                    (grupos_2votos['dbscan_eh_grupo'] == 1) & 
                    (grupos_2votos['iforest_eh_grupo'] == 0)
                ])
                
                comb_km_if = len(grupos_2votos[
                    (grupos_2votos['kmeans_eh_grupo'] == 1) & 
                    (grupos_2votos['dbscan_eh_grupo'] == 0) & 
                    (grupos_2votos['iforest_eh_grupo'] == 1)
                ])
                
                comb_db_if = len(grupos_2votos[
                    (grupos_2votos['kmeans_eh_grupo'] == 0) & 
                    (grupos_2votos['dbscan_eh_grupo'] == 1) & 
                    (grupos_2votos['iforest_eh_grupo'] == 1)
                ])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("K-Means + DBSCAN", comb_km_db)
                with col2:
                    st.metric("K-Means + I.Forest", comb_km_if)
                with col3:
                    st.metric("DBSCAN + I.Forest", comb_db_if)
                
                # Tabela
                colunas_exibir = [
                    'num_grupo', 'score_ml_percentual', 'score_final_ccs', 'qtd_cnpjs',
                    'kmeans_eh_grupo', 'dbscan_eh_grupo', 'iforest_eh_grupo'
                ]
                
                df_display = grupos_2votos[colunas_exibir].copy()
                df_display = df_display.rename(columns={
                    'kmeans_eh_grupo': 'K-Means',
                    'dbscan_eh_grupo': 'DBSCAN',
                    'iforest_eh_grupo': 'I.Forest'
                })
                
                st.dataframe(df_display.head(50), width='stretch', hide_index=True)
            else:
                st.info("Nenhum grupo com consenso moderado (2/3).")
        
        with tab3:
            grupos_1voto = df_grupos[df_grupos['votos_eh_grupo'] == 1].sort_values('score_ml_percentual', ascending=False)
            
            if not grupos_1voto.empty:
                st.info(f"**{len(grupos_1voto)} grupos com CONSENSO FRACO** - Apenas 1 algoritmo indica")
                
                # Mostrar qual algoritmo votou
                st.write("**Algoritmo que Indicou:**")
                
                apenas_km = len(grupos_1voto[
                    (grupos_1voto['kmeans_eh_grupo'] == 1) & 
                    (grupos_1voto['dbscan_eh_grupo'] == 0) & 
                    (grupos_1voto['iforest_eh_grupo'] == 0)
                ])
                
                apenas_db = len(grupos_1voto[
                    (grupos_1voto['kmeans_eh_grupo'] == 0) & 
                    (grupos_1voto['dbscan_eh_grupo'] == 1) & 
                    (grupos_1voto['iforest_eh_grupo'] == 0)
                ])
                
                apenas_if = len(grupos_1voto[
                    (grupos_1voto['kmeans_eh_grupo'] == 0) & 
                    (grupos_1voto['dbscan_eh_grupo'] == 0) & 
                    (grupos_1voto['iforest_eh_grupo'] == 1)
                ])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Apenas K-Means", apenas_km)
                with col2:
                    st.metric("Apenas DBSCAN", apenas_db)
                with col3:
                    st.metric("Apenas I.Forest", apenas_if)
                
                # Tabela resumida
                colunas_exibir = [
                    'num_grupo', 'score_ml_percentual', 'qtd_cnpjs',
                    'kmeans_eh_grupo', 'dbscan_eh_grupo', 'iforest_eh_grupo'
                ]
                
                df_display = grupos_1voto[colunas_exibir].copy()
                df_display = df_display.rename(columns={
                    'kmeans_eh_grupo': 'K-Means',
                    'dbscan_eh_grupo': 'DBSCAN',
                    'iforest_eh_grupo': 'I.Forest'
                })
                
                st.dataframe(df_display.head(50), width='stretch', hide_index=True)
            else:
                st.info("Nenhum grupo com consenso fraco (1/3).")
        
        with tab4:
            grupos_0votos = df_grupos[df_grupos['votos_eh_grupo'] == 0].sort_values('score_ml_percentual', ascending=False)
            
            if not grupos_0votos.empty:
                st.success(f"**{len(grupos_0votos)} grupos classificados como NÃO É GRUPO** - Nenhum algoritmo indicou")
                
                st.write("**Características destes grupos:**")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Score ML Médio", f"{grupos_0votos['score_ml_percentual'].mean():.1f}%")
                with col2:
                    st.metric("Score GEI Médio", f"{grupos_0votos['score_final_ccs'].mean():.1f}")
                
                # Tabela resumida
                colunas_exibir = ['num_grupo', 'score_ml_percentual', 'score_final_ccs', 'qtd_cnpjs']
                st.dataframe(grupos_0votos[colunas_exibir].head(50), width='stretch', hide_index=True)
            else:
                st.info("Todos os grupos foram identificados como Grupo Econômico por pelo menos 1 algoritmo.")
        
        # ================================================================
        # SEÇÃO 6: EXPORTAR RESULTADOS
        # ================================================================
        st.header("6. Exportar Resultados")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV com resultados de consenso
            colunas_export = [
                'num_grupo', 'qtd_cnpjs', 'score_ml_percentual', 'score_final_ccs',
                'votos_eh_grupo', 'consenso_classificacao', 'nivel_confianca',
                'kmeans_eh_grupo', 'dbscan_eh_grupo', 'iforest_eh_grupo',
                'socios_compartilhados', 'receita_maxima', 'total_indicios', 
                'contas_compartilhadas', 'acima_limite_sn'
            ]
            
            csv_resultados = df_grupos[colunas_export].to_csv(index=False)
            
            st.download_button(
                label="📥 Download Resultados Consenso (CSV)",
                data=csv_resultados,
                file_name=f"grupos_ml_consenso_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv"
            )
        
        with col2:
            # Excel com resultados detalhados por nível de consenso
            try:
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_grupos.to_excel(writer, sheet_name='Todos os Grupos', index=False)
                    grupos_3votos.to_excel(writer, sheet_name='Consenso Forte (3-3)', index=False)
                    grupos_2votos.to_excel(writer, sheet_name='Consenso Moderado (2-3)', index=False)
                    grupos_1voto.to_excel(writer, sheet_name='Consenso Fraco (1-3)', index=False)
                    grupos_0votos.to_excel(writer, sheet_name='Não é Grupo (0-3)', index=False)
                
                st.download_button(
                    label="📊 Download Completo (Excel)",
                    data=output.getvalue(),
                    file_name=f"grupos_ml_consenso_completo_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except ImportError:
                st.warning("⚠️ Biblioteca openpyxl não disponível. Use o download CSV.")
    
    elif 'cluster' in df_grupos.columns:
        # MODO INDIVIDUAL - RESULTADOS
        st.header("4. Resultados da Classificação")
        
        # Métricas de avaliação
        col1, col2, col3 = st.columns(3)
        
        with col1:
            grupos_economicos = (df_grupos['eh_grupo_economico'] == 'Grupo Econômico').sum()
            st.metric("Grupos Econômicos Identificados", grupos_economicos)
        
        with col2:
            nao_grupos = (df_grupos['eh_grupo_economico'] == 'Não é Grupo').sum()
            st.metric("Não é Grupo Econômico", nao_grupos)
        
        with col3:
            if 'modelo_ml' in st.session_state and 'scaler_ml' in st.session_state:
                if algoritmo != "Isolation Forest" and len(df_grupos['cluster'].unique()) > 1:
                    X = df_grupos[features_para_modelo].fillna(0)
                    scaler = st.session_state['scaler_ml']
                    X_scaled = scaler.transform(X)
                    
                    if usar_pca and 'pca_ml' in st.session_state:
                        pca = st.session_state['pca_ml']
                        X_transformed = pca.transform(X_scaled)
                    else:
                        X_transformed = X_scaled
                    
                    try:
                        silhouette = silhouette_score(X_transformed, df_grupos['cluster'])
                        st.metric("Silhouette Score", f"{silhouette:.3f}")
                    except:
                        st.metric("Silhouette Score", "N/A")
        
        # Comparação de scores
        fig = px.box(df_grupos, x='eh_grupo_economico', y='score_ml_percentual',
                    color='eh_grupo_economico',
                    title="Distribuição de Scores ML por Classificação",
                    labels={'score_ml_percentual': 'Score ML (%)', 'eh_grupo_economico': 'Classificação'},
                    template=filtros['tema'],
                    color_discrete_map={
                        'Grupo Econômico': '#EF553B',
                        'Não é Grupo': '#00CC96'
                    })
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabelas
        st.header("5. Grupos Identificados")
        
        tab1, tab2 = st.tabs(["✅ Grupos Econômicos", "❌ Não é Grupo"])
        
        with tab1:
            grupos_eco = df_grupos[df_grupos['eh_grupo_economico'] == 'Grupo Econômico'].sort_values('score_ml_percentual', ascending=False)
            
            if not grupos_eco.empty:
                st.write(f"**{len(grupos_eco)} grupos identificados como Grupo Econômico**")
                
                colunas_exibir = [
                    'num_grupo', 'score_ml_percentual', 'score_final_ccs', 'qtd_cnpjs',
                    'socios_compartilhados', 'receita_maxima', 'total_indicios',
                    'contas_compartilhadas'
                ]
                
                df_display = grupos_eco[colunas_exibir].copy()
                df_display['receita_maxima'] = df_display['receita_maxima'].apply(formatar_moeda)
                
                st.dataframe(df_display.head(50), width='stretch', hide_index=True)
            else:
                st.info("Nenhum grupo classificado como Grupo Econômico.")
        
        with tab2:
            nao_grupos_df = df_grupos[df_grupos['eh_grupo_economico'] == 'Não é Grupo'].sort_values('score_ml_percentual', ascending=False)
            
            if not nao_grupos_df.empty:
                st.write(f"**{len(nao_grupos_df)} grupos classificados como Não é Grupo**")
                
                colunas_exibir = [
                    'num_grupo', 'score_ml_percentual', 'score_final_ccs', 'qtd_cnpjs'
                ]
                
                st.dataframe(nao_grupos_df[colunas_exibir].head(50), width='stretch', hide_index=True)
            else:
                st.info("Todos grupos classificados como Grupo Econômico.")
        
        # Exportação
        st.header("6. Exportar Resultados")
        
        col1, col2 = st.columns(2)
        
        with col1:
            colunas_export = [
                'num_grupo', 'qtd_cnpjs', 'score_ml_percentual', 'score_final_ccs',
                'cluster', 'eh_grupo_economico', 'socios_compartilhados', 'receita_maxima',
                'total_indicios', 'contas_compartilhadas', 'acima_limite_sn'
            ]
            
            csv_resultados = df_grupos[colunas_export].to_csv(index=False)
            
            st.download_button(
                label="📥 Download Resultados (CSV)",
                data=csv_resultados,
                file_name=f"grupos_ml_classificacao_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv"
            )
        
        with col2:
            try:
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_grupos.to_excel(writer, sheet_name='Todos os Grupos', index=False)
                    grupos_eco.to_excel(writer, sheet_name='Grupos Econômicos', index=False)
                    nao_grupos_df.to_excel(writer, sheet_name='Não é Grupo', index=False)
                
                st.download_button(
                    label="📊 Download Completo (Excel)",
                    data=output.getvalue(),
                    file_name=f"grupos_ml_analise_completa_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except ImportError:
                st.warning("⚠️ Biblioteca openpyxl não disponível. Use o download CSV.")

def mostrar_detalhes_grupo(engine, num_grupo, df_grupos, filtros):
    """Mostra detalhes completos de um grupo específico"""
    
    grupo_info = df_grupos[df_grupos['num_grupo'] == num_grupo].iloc[0]
    
    st.subheader(f"📋 Detalhes do Grupo {num_grupo}")
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Score ML", f"{grupo_info['score_ml_percentual']:.1f}%")
    with col2:
        st.metric("CNPJs", int(grupo_info['qtd_cnpjs']))
    with col3:
        st.metric("Score GEI", f"{grupo_info['score_final_ccs']:.1f}")
    with col4:
        st.metric("Classificação", grupo_info.get('eh_grupo_economico', 'N/A'))
    
    # Votação dos algoritmos
    st.write("**Votação dos Algoritmos:**")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("K-Means", "✅ SIM" if grupo_info['kmeans_eh_grupo'] == 1 else "❌ NÃO")
    with col2:
        st.metric("DBSCAN", "✅ SIM" if grupo_info['dbscan_eh_grupo'] == 1 else "❌ NÃO")
    with col3:
        st.metric("Isolation Forest", "✅ SIM" if grupo_info['iforest_eh_grupo'] == 1 else "❌ NÃO")
    with col4:
        votos = int(grupo_info['votos_eh_grupo'])
        consenso = "FORTE" if votos == 3 else "MODERADO" if votos == 2 else "FRACO" if votos == 1 else "NENHUM"
        st.metric("Consenso", f"{consenso} ({votos}/3)")
    
    st.divider()
    
    # Características do grupo
    st.write("**Características:**")
    
    # Converter todos os valores para string para evitar conflito de tipos
    caracteristicas = {
        'Razão Social Idêntica': 'Sim' if grupo_info['razao_social_identica'] == 1 else 'Não',
        'Nome Fantasia Idêntico': 'Sim' if grupo_info['fantasia_identica'] == 1 else 'Não',
        'CNAE Idêntico': 'Sim' if grupo_info['cnae_identico'] == 1 else 'Não',
        'Contador Idêntico': 'Sim' if grupo_info['contador_identico'] == 1 else 'Não',
        'Endereço Idêntico': 'Sim' if grupo_info['endereco_identico'] == 1 else 'Não',
        'Sócios Compartilhados': str(int(grupo_info['socios_compartilhados'])),
        'Índice Interconexão': f"{grupo_info['indice_interconexao']:.3f}",
        'Receita Máxima': formatar_moeda(grupo_info['receita_maxima']),
        'Acima Limite SN': 'Sim' if grupo_info['acima_limite_sn'] == 1 else 'Não',
        'Total de Indícios': str(int(grupo_info['total_indicios'])),
        'Tipos Indícios Distintos': str(int(grupo_info['tipos_indicios_distintos'])),
        'Contas Compartilhadas': str(int(grupo_info['contas_compartilhadas'])),
        'Índice Risco CCS': f"{grupo_info['indice_risco_ccs']:.4f}",
        'Índice Risco C115': f"{grupo_info['indice_risco_c115']:.4f}",
        'Total Funcionários': str(int(grupo_info['total_funcionarios'])),
        'Score Inconsistências NFe': f"{grupo_info['score_inconsistencias_nfe']:.2f}"
    }
    
    # Criar dataframe com tipos consistentes
    df_caract = pd.DataFrame({
        'Característica': list(caracteristicas.keys()),
        'Valor': list(caracteristicas.values())
    })
    
    # Garantir que ambas as colunas são string
    df_caract['Característica'] = df_caract['Característica'].astype(str)
    df_caract['Valor'] = df_caract['Valor'].astype(str)
    
    st.dataframe(df_caract, hide_index=True)
    
    st.divider()
    
    # CNPJs do grupo
    st.write("**CNPJs do Grupo:**")
    
    query_cnpjs = f"""
    SELECT 
        g.cnpj,
        c.nm_razao_social,
        c.nm_fantasia,
        c.cd_cnae,
        c.nm_reg_apuracao,
        c.nm_munic as municipio
    FROM gessimples.gei_cnpj g
    LEFT JOIN usr_sat_ods.vw_ods_contrib c ON g.cnpj = c.nu_cnpj
    WHERE CAST(g.num_grupo AS INT) = {num_grupo}
    """
    
    try:
        df_cnpjs_grupo = pd.read_sql(query_cnpjs, engine)
        if not df_cnpjs_grupo.empty:
            # Garantir que todas as colunas são string para evitar problemas com Arrow
            for col in df_cnpjs_grupo.columns:
                df_cnpjs_grupo[col] = df_cnpjs_grupo[col].astype(str)
            
            st.dataframe(df_cnpjs_grupo, hide_index=True)
        else:
            st.info("CNPJs não encontrados ou erro ao carregar dados cadastrais.")
    except Exception as e:
        st.warning(f"Erro ao carregar CNPJs: {e}")
    
    st.divider()
    
    # Sócios compartilhados - SEM nm_socio
    if grupo_info['socios_compartilhados'] > 0:
        st.write("**Sócios Compartilhados:**")
        try:
            query_socios = f"""
            SELECT 
                cpf_socio,
                qtd_empresas
            FROM gessimples.gei_socios_compartilhados
            WHERE CAST(num_grupo AS INT) = {num_grupo}
            ORDER BY qtd_empresas DESC
            """
            
            df_socios = pd.read_sql(query_socios, engine)
            
            if not df_socios.empty:
                # Converter para string
                for col in df_socios.columns:
                    df_socios[col] = df_socios[col].astype(str)
                st.dataframe(df_socios.head(20), hide_index=True)
            else:
                st.info("Detalhes de sócios não disponíveis.")
        except Exception as e:
            st.warning(f"Não foi possível carregar sócios: {e}")
    
    st.divider()
    
    # Indícios - SEM dt_referencia
    if grupo_info['total_indicios'] > 0:
        st.write("**Indícios Fiscais:**")
        try:
            query_indicios = f"""
            SELECT 
                tx_descricao_indicio,
                cnpj
            FROM gessimples.gei_indicios
            WHERE CAST(num_grupo AS INT) = {num_grupo}
            """
            
            df_indicios = pd.read_sql(query_indicios, engine)
            
            if not df_indicios.empty:
                # Resumo por tipo
                resumo_indicios = df_indicios['tx_descricao_indicio'].value_counts().reset_index()
                resumo_indicios.columns = ['Tipo de Indício', 'Quantidade']
                
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.dataframe(resumo_indicios, hide_index=True)
                with col2:
                    fig = px.bar(resumo_indicios, x='Quantidade', y='Tipo de Indício',
                               orientation='h', title="Distribuição de Indícios",
                               template=filtros.get('tema', 'plotly'))
                    st.plotly_chart(fig)
                
                # Mostrar lista completa
                st.write("**Lista de Indícios:**")
                st.dataframe(df_indicios.head(50), hide_index=True)
            else:
                st.info("Detalhes de indícios não disponíveis.")
        except Exception as e:
            st.warning(f"Não foi possível carregar indícios: {e}")
    
    st.divider()
    
    # Contas compartilhadas (CCS)
    if grupo_info['contas_compartilhadas'] > 0:
        st.write("**Contas Compartilhadas (CCS):**")
        try:
            query_ccs = f"""
            SELECT 
                nr_cpf,
                nm_banco,
                cd_agencia,
                nr_conta,
                qtd_cnpjs_usando_conta,
                qtd_vinculos_ativos
            FROM gessimples.gei_ccs_cpf_compartilhado
            WHERE CAST(num_grupo AS INT) = {num_grupo}
            ORDER BY qtd_cnpjs_usando_conta DESC
            """
            
            df_ccs = pd.read_sql(query_ccs, engine)
            
            if not df_ccs.empty:
                # Converter para string
                for col in df_ccs.columns:
                    df_ccs[col] = df_ccs[col].astype(str)
                st.dataframe(df_ccs.head(20), hide_index=True)
            else:
                st.info("Detalhes de contas compartilhadas não disponíveis.")
        except Exception as e:
            st.warning(f"Não foi possível carregar CCS: {e}")
    
def gerar_pdf_analise_pontual(cnpjs_validos, resultados):
    """Gera PDF completo da análise pontual"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    story = []
    
    # Título
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=20,
        textColor=colors.HexColor('#1f77b4'),
        spaceAfter=20,
        alignment=TA_CENTER
    )
    story.append(Paragraph("ANÁLISE PONTUAL DE CNPJs", title_style))
    story.append(Paragraph("Sistema GEI - Receita Estadual de Santa Catarina", styles['Normal']))
    story.append(Paragraph(f"Data de Geração: {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # SEÇÃO 1: RESUMO EXECUTIVO
    story.append(Paragraph("<b>1. RESUMO EXECUTIVO</b>", styles['Heading2']))
    
    dados_resumo = [
        ['Métrica', 'Valor'],
        ['CNPJs Analisados', str(len(cnpjs_validos))],
        ['CNPJs com Cadastro', str(len(resultados.get('cadastro', pd.DataFrame())))],
        ['Vínculos Societários', str(len(resultados.get('socios', pd.DataFrame())))],
        ['Sócios Compartilhados', str(len(resultados.get('socios_compartilhados', pd.Series())))],
        ['Notas Fiscais (2025)', str(len(resultados.get('nfe', pd.DataFrame())))],
        ['Indícios Fiscais', str(len(resultados.get('indicios', pd.DataFrame())))],
        ['Contas Bancárias', str(len(resultados.get('ccs', pd.DataFrame())))],
        ['Funcionários Encontrados', str(resultados.get('funcionarios', pd.DataFrame())['total_funcionarios'].sum() if not resultados.get('funcionarios', pd.DataFrame()).empty else 0)],
        ['Em Grupos GEI Existentes', str(len(resultados.get('grupos_existentes', pd.DataFrame())))]
    ]
    
    table = Table(dados_resumo, colWidths=[3*inch, 3*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.3*inch))
    
    # ALERTAS CRÍTICOS
    alertas = []
    
    if len(resultados.get('socios_compartilhados', pd.Series())) > 0:
        alertas.append("CRÍTICO: Sócios compartilhados detectados - possível grupo econômico")
    
    # Verificar receitas altas (PGDAS + DIME)
    tem_pgdas_alerta = not resultados.get('pgdas', pd.DataFrame()).empty
    tem_dime_alerta = not resultados.get('dime', pd.DataFrame()).empty

    cnpjs_acima_limite = set()
    if tem_pgdas_alerta:
        receitas_altas_pgdas = resultados['pgdas'][resultados['pgdas']['receita_12m'] > 4800000]
        cnpjs_acima_limite.update(receitas_altas_pgdas['cnpj'].unique())
    if tem_dime_alerta:
        receitas_altas_dime = resultados['dime'][resultados['dime']['receita_12m'] > 4800000]
        cnpjs_acima_limite.update(receitas_altas_dime['cnpj'].unique())

    if cnpjs_acima_limite:
        alertas.append(f"ATENÇÃO: {len(cnpjs_acima_limite)} CNPJs com faturamento acima do limite SN (PGDAS/DIME)")
    
    if not resultados.get('indicios', pd.DataFrame()).empty:
        alertas.append(f"CRÍTICO: {len(resultados['indicios'])} indícios fiscais identificados")
    
    if not resultados.get('ccs', pd.DataFrame()).empty:
        cpfs_contas = resultados['ccs'].groupby('nr_cpf')['cnpj'].nunique()
        if (cpfs_contas > 1).any():
            alertas.append("ATENÇÃO: Contas bancárias compartilhadas detectadas")
    
    if alertas:
        story.append(Paragraph("<b>ALERTAS:</b>", styles['Heading3']))
        for alerta in alertas:
            story.append(Paragraph(f"• {alerta}", styles['Normal']))
        story.append(Spacer(1, 0.2*inch))
    
    story.append(PageBreak())
    
    # SEÇÃO 2: CNPJs ANALISADOS E DADOS CADASTRAIS
    story.append(Paragraph(f"<b>2. CNPJs ANALISADOS ({len(cnpjs_validos)})</b>", styles['Heading2']))
    
    for cnpj in cnpjs_validos:
        story.append(Paragraph(f"<b>CNPJ: {cnpj}</b>", styles['Normal']))
        
        if not resultados.get('cadastro', pd.DataFrame()).empty:
            cadastro = resultados['cadastro'][resultados['cadastro']['cnpj'] == cnpj]
            if not cadastro.empty:
                info = cadastro.iloc[0]
                story.append(Paragraph(f"Razão Social: {info.get('nm_razao_social', 'N/A')}", styles['Normal']))
                if pd.notna(info.get('nm_fantasia')):
                    story.append(Paragraph(f"Nome Fantasia: {info.get('nm_fantasia')}", styles['Normal']))
                if pd.notna(info.get('cd_cnae')):
                    story.append(Paragraph(f"CNAE: {info.get('cd_cnae')}", styles['Normal']))
                if pd.notna(info.get('nm_reg_apuracao')):
                    story.append(Paragraph(f"Regime: {info.get('nm_reg_apuracao')}", styles['Normal']))
                if pd.notna(info.get('municipio')):
                    story.append(Paragraph(f"Município: {info.get('municipio')}", styles['Normal']))
                if pd.notna(info.get('nm_contador')):
                    story.append(Paragraph(f"Contador: {info.get('nm_contador')}", styles['Normal']))
        
        story.append(Spacer(1, 0.15*inch))
    
    story.append(PageBreak())
    
    # =====================================================================
    # SEÇÃO 3: ANÁLISE DE SIMILARIDADE - EVIDÊNCIAS DE GRUPO ECONÔMICO
    # =====================================================================
    story.append(Paragraph("<b>3. ANÁLISE DE SIMILARIDADE - EVIDÊNCIAS DE GRUPO ECONÔMICO</b>", styles['Heading2']))
    story.append(Spacer(1, 0.1*inch))
    
    # Calcular score de similaridade
    score_similaridade = 0
    max_score_possivel = 0
    evidencias_pdf = {}
    
    # 3.1 - ANÁLISE CADASTRAL
    story.append(Paragraph("<b>3.1. Consistência Cadastral</b>", styles['Heading3']))
    
    if not resultados['cadastro'].empty and len(resultados['cadastro']) > 1:
        cadastro_dados = []
        
        # Razão Social
        max_score_possivel += 2
        razoes = resultados['cadastro']['nm_razao_social'].dropna().unique()
        if len(razoes) == 1:
            cadastro_dados.append(['Razão Social', 'IDÊNTICA', '1', '+2.0', 'CRÍTICO'])
            evidencias_pdf['razao_social'] = True
            score_similaridade += 2
        else:
            cadastro_dados.append(['Razão Social', 'DIFERENTES', str(len(razoes)), '0.0', '-'])
        
        # Contador
        max_score_possivel += 2
        contadores = resultados['cadastro']['nm_contador'].dropna().unique()
        if len(contadores) == 1:
            cadastro_dados.append(['Contador', 'MESMO', '1', '+2.0', 'CRÍTICO'])
            evidencias_pdf['contador'] = True
            score_similaridade += 2
        else:
            cadastro_dados.append(['Contador', 'DIFERENTES', str(len(contadores)), '0.0', '-'])
        
        # Endereço
        max_score_possivel += 3
        enderecos = resultados['cadastro'].apply(
            lambda row: f"{row.get('nm_logradouro', '')} {row.get('nu_logradouro', '')} {row.get('nm_bairro', '')} {row.get('municipio', '')}".strip(),
            axis=1
        ).unique()
        if len(enderecos) == 1:
            cadastro_dados.append(['Endereço', 'IDÊNTICO', '1', '+3.0', 'CRÍTICO'])
            evidencias_pdf['endereco'] = True
            score_similaridade += 3
        else:
            cadastro_dados.append(['Endereço', 'DIFERENTES', str(len(enderecos)), '0.0', '-'])
        
        # CNAE
        max_score_possivel += 1
        cnaes = resultados['cadastro']['cd_cnae'].dropna().unique()
        if len(cnaes) == 1:
            cadastro_dados.append(['CNAE', 'IDÊNTICO', '1', '+1.0', 'Alto'])
            evidencias_pdf['cnae'] = True
            score_similaridade += 1
        else:
            cadastro_dados.append(['CNAE', 'DIFERENTES', str(len(cnaes)), '0.0', '-'])
        
        # Município
        max_score_possivel += 0.5
        municipios = resultados['cadastro']['municipio'].dropna().unique()
        if len(municipios) == 1:
            cadastro_dados.append(['Município', 'MESMO', '1', '+0.5', 'Leve'])
            score_similaridade += 0.5
        
        table = Table([['Atributo', 'Status', 'Qtd', 'Pontos', 'Nível']] + cadastro_dados,
                     colWidths=[1.5*inch, 1.3*inch, 0.8*inch, 0.9*inch, 1*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.2*inch))
    
    # 3.2 - VÍNCULOS SOCIETÁRIOS
    story.append(Paragraph("<b>3.2. Vínculos Societários</b>", styles['Heading3']))
    
    if not resultados['socios'].empty and len(cnpjs_validos) > 1:
        socios_dados = []
        
        max_score_possivel += 5
        socios_compartilhados = resultados.get('socios_compartilhados', pd.Series())
        
        if len(socios_compartilhados) > 0:
            pontos_socios = min(len(socios_compartilhados) * 2, 5)
            socios_dados.append(['Sócios Compartilhados', str(len(socios_compartilhados)), 
                                'DETECTADO', f'+{pontos_socios:.1f}', 'CRÍTICO'])
            evidencias_pdf['socios_compartilhados'] = True
            score_similaridade += pontos_socios
            
            # Listar sócios compartilhados
            story.append(Paragraph("Sócios que participam de múltiplos CNPJs:", styles['Normal']))
            for cpf, qtd in list(socios_compartilhados.items())[:10]:
                story.append(Paragraph(f"• CPF {cpf}: Presente em {qtd} CNPJs", 
                                      ParagraphStyle('Indent', parent=styles['Normal'], leftIndent=20)))
        else:
            socios_dados.append(['Sócios Compartilhados', '0', 'NÃO DETECTADO', '0.0', '-'])
        
        table = Table([['Indicador', 'Quantidade', 'Status', 'Pontos', 'Nível']] + socios_dados,
                     colWidths=[2*inch, 1*inch, 1.5*inch, 0.8*inch, 0.7*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.2*inch))
    
    # 3.3 - FATURAMENTO (PGDAS + DIME)
    story.append(Paragraph("<b>3.3. Análise de Faturamento (PGDAS / DIME)</b>", styles['Heading3']))

    # Consolidar dados de faturamento
    tem_pgdas_pdf = not resultados.get('pgdas', pd.DataFrame()).empty
    tem_dime_pdf = not resultados.get('dime', pd.DataFrame()).empty

    if (tem_pgdas_pdf or tem_dime_pdf) and len(cnpjs_validos) > 1:
        receitas_dados = []

        # Calcular receitas consolidadas
        receitas_pgdas = resultados['pgdas'].groupby('cnpj')['receita_12m'].max() if tem_pgdas_pdf else pd.Series(dtype=float)
        receitas_dime = resultados['dime'].groupby('cnpj')['receita_12m'].max() if tem_dime_pdf else pd.Series(dtype=float)

        # Combinar receitas (soma de PGDAS e DIME por CNPJ único)
        todos_cnpjs = set(receitas_pgdas.index.tolist()) | set(receitas_dime.index.tolist())
        receitas_por_cnpj = pd.Series(dtype=float)
        for cnpj in todos_cnpjs:
            valor_pgdas = receitas_pgdas.get(cnpj, 0)
            valor_dime = receitas_dime.get(cnpj, 0)
            receitas_por_cnpj[cnpj] = max(valor_pgdas, valor_dime)  # Usar o maior valor para evitar duplicação

        receita_total_grupo = receitas_por_cnpj.sum()

        # Info sobre fontes
        fontes_str = []
        if tem_pgdas_pdf:
            fontes_str.append(f"PGDAS: {len(receitas_pgdas)} CNPJs")
        if tem_dime_pdf:
            fontes_str.append(f"DIME: {len(receitas_dime)} CNPJs")
        story.append(Paragraph(f"Fontes: {', '.join(fontes_str)}", styles['Normal']))
        story.append(Spacer(1, 0.1*inch))

        max_score_possivel += 5
        if receita_total_grupo > 4800000:
            excesso = receita_total_grupo - 4800000
            receitas_dados.append(['Faturamento Total', formatar_moeda(receita_total_grupo),
                                   'ACIMA LIMITE', formatar_moeda(excesso), '+5.0', 'CRÍTICO'])
            evidencias_pdf['receita_excesso'] = True
            score_similaridade += 5
        else:
            receitas_dados.append(['Faturamento Total', formatar_moeda(receita_total_grupo),
                                   'DENTRO LIMITE', '-', '0.0', '-'])

        # Distribuição uniforme
        max_score_possivel += 2
        if len(receitas_por_cnpj) > 1:
            receita_media = receitas_por_cnpj.mean()
            desvio_padrao = receitas_por_cnpj.std()
            coef_variacao = (desvio_padrao / receita_media) if receita_media > 0 else 0

            if coef_variacao < 0.3:
                receitas_dados.append(['Distribuição', f'CV: {coef_variacao:.2f}',
                                       'MUITO UNIFORME', '-', '+2.0', 'Planejada'])
                evidencias_pdf['receita_uniforme'] = True
                score_similaridade += 2
            else:
                receitas_dados.append(['Distribuição', f'CV: {coef_variacao:.2f}',
                                       'VARIADA', '-', '0.0', '-'])

        # Análise de regimes mistos
        if tem_pgdas_pdf and tem_dime_pdf:
            receitas_dados.append(['Regimes Tributários', 'Misto (SN + Normal)',
                                   'ATENÇÃO', '-', '+1.0', 'Planejamento'])
            score_similaridade += 1

        table = Table([['Indicador', 'Valor', 'Status', 'Detalhe', 'Pontos', 'Nível']] + receitas_dados,
                     colWidths=[1.2*inch, 1.3*inch, 1.2*inch, 1*inch, 0.6*inch, 0.7*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 7),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.2*inch))
    
    # 3.4 - NOTAS FISCAIS
    story.append(Paragraph("<b>3.4. Compartilhamento em Notas Fiscais</b>", styles['Heading3']))
    
    if not resultados['nfe'].empty and len(cnpjs_validos) > 1:
        nfe_dados = []
        
        # IPs compartilhados
        max_score_possivel += 3
        if 'nfe_ip_transmissao' in resultados['nfe'].columns:
            ips_por_cnpj = {}
            for cnpj in cnpjs_validos:
                ips = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'] == cnpj]['nfe_ip_transmissao'].dropna().unique()
                if len(ips) > 0:
                    ips_por_cnpj[cnpj] = set(ips)
            
            if len(ips_por_cnpj) > 1:
                all_ips = set()
                for ips in ips_por_cnpj.values():
                    all_ips.update(ips)
                
                ips_compart = [ip for ip in all_ips if sum(1 for ips in ips_por_cnpj.values() if ip in ips) > 1]
                
                if len(ips_compart) > 0:
                    pontos_ip = min(len(ips_compart), 3)
                    nfe_dados.append(['IPs Transmissão', str(len(ips_compart)), 'COMPARTILHADOS', 
                                     f'+{pontos_ip:.1f}', 'CRÍTICO'])
                    evidencias_pdf['ip_compartilhado'] = True
                    score_similaridade += pontos_ip
                else:
                    nfe_dados.append(['IPs Transmissão', '0', 'NÃO COMPART.', '0.0', '-'])
        
        # Clientes compartilhados
        max_score_possivel += 2
        clientes_por_cnpj = {}
        for cnpj in cnpjs_validos:
            clientes = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'] == cnpj]['nfe_cnpj_cpf_dest'].dropna().unique()
            clientes_por_cnpj[cnpj] = set(clientes)
        
        if len(clientes_por_cnpj) > 1:
            clientes_compart = set.intersection(*clientes_por_cnpj.values()) if clientes_por_cnpj else set()
            
            if len(clientes_compart) > 0:
                pontos_cli = min(len(clientes_compart) / 10, 2)
                nfe_dados.append(['Clientes Comuns', str(len(clientes_compart)), 'DETECTADOS',
                                 f'+{pontos_cli:.1f}', 'Moderado'])
                evidencias_pdf['clientes_comuns'] = True
                score_similaridade += pontos_cli
        
        # Fornecedores compartilhados
        max_score_possivel += 2
        fornec_por_cnpj = {}
        for cnpj in cnpjs_validos:
            fornec = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_dest'] == cnpj]['nfe_cnpj_cpf_emit'].dropna().unique()
            fornec_por_cnpj[cnpj] = set(fornec)
        
        if len(fornec_por_cnpj) > 1:
            fornec_compart = set.intersection(*fornec_por_cnpj.values()) if fornec_por_cnpj else set()
            
            if len(fornec_compart) > 0:
                pontos_forn = min(len(fornec_compart) / 10, 2)
                nfe_dados.append(['Fornecedores Comuns', str(len(fornec_compart)), 'DETECTADOS',
                                 f'+{pontos_forn:.1f}', 'Moderado'])
                evidencias_pdf['fornecedores_comuns'] = True
                score_similaridade += pontos_forn
        
        # >>> ADICIONAR AQUI <
        # Endereços de emissão compartilhados
        max_score_possivel += 2
        if 'nfe_emit_end_completo' in resultados['nfe'].columns:
            enderecos_emit = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'].isin(cnpjs_validos)]['nfe_emit_end_completo'].dropna().unique()
            if len(enderecos_emit) == 1 and len(enderecos_emit[0]) > 10:
                nfe_dados.append(['Endereço Emissão', '1', 'MESMO ENDEREÇO',
                                 '+2.0', 'CRÍTICO'])
                evidencias_pdf['endereco_nfe_emit'] = True
                score_similaridade += 2
        
        # Endereços de destino compartilhados
        max_score_possivel += 2
        if 'nfe_dest_end_completo' in resultados['nfe'].columns:
            enderecos_dest = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_dest'].isin(cnpjs_validos)]['nfe_dest_end_completo'].dropna().unique()
            if len(enderecos_dest) == 1 and len(enderecos_dest[0]) > 10:
                nfe_dados.append(['Endereço Destino', '1', 'MESMO ENDEREÇO',
                                 '+2.0', 'CRÍTICO'])
                evidencias_pdf['endereco_nfe_dest'] = True
                score_similaridade += 2
        # >>> FIM DA ADIÇÃO <
        
        if nfe_dados:
            table = Table([['Indicador', 'Quantidade', 'Status', 'Pontos', 'Nível']] + nfe_dados,
                         colWidths=[1.8*inch, 1.2*inch, 1.3*inch, 0.8*inch, 0.9*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(table)
            story.append(Spacer(1, 0.2*inch))
    
    # 3.5 - CONVÊNIO 115
    story.append(Paragraph("<b>3.5. Convênio 115</b>", styles['Heading3']))
    
    if not resultados['c115'].empty and len(cnpjs_validos) > 1:
        c115_dados = []
        
        max_score_possivel += 3
        identificadores = resultados['c115'].groupby('nu_identificador_tomador')['cnpj_tomador'].nunique()
        ident_compart = identificadores[identificadores > 1]
        
        if len(ident_compart) > 0:
            pontos_id = min(len(ident_compart), 3)
            c115_dados.append(['Identificadores', str(len(ident_compart)), 'COMPARTILHADOS',
                              f'+{pontos_id:.1f}', 'CRÍTICO'])
            evidencias_pdf['c115_identificador'] = True
            score_similaridade += pontos_id
        else:
            c115_dados.append(['Identificadores', '0', 'NÃO COMPART.', '0.0', '-'])
        
        # Telefones
        max_score_possivel += 2
        if 'nu_tel_contato' in resultados['c115'].columns:
            telefones = resultados['c115'].groupby('nu_tel_contato')['cnpj_tomador'].nunique()
            tel_compart = telefones[telefones > 1]
            
            if len(tel_compart) > 0:
                pontos_tel = min(len(tel_compart), 2)
                c115_dados.append(['Telefones', str(len(tel_compart)), 'COMPARTILHADOS',
                                  f'+{pontos_tel:.1f}', 'Alto'])
                evidencias_pdf['c115_telefone'] = True
                score_similaridade += pontos_tel
        
        if c115_dados:
            table = Table([['Indicador', 'Quantidade', 'Status', 'Pontos', 'Nível']] + c115_dados,
                         colWidths=[1.8*inch, 1.2*inch, 1.3*inch, 0.8*inch, 0.9*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(table)
            story.append(Spacer(1, 0.2*inch))
    
    # 3.6 - CONTAS BANCÁRIAS (CCS)
    story.append(Paragraph("<b>3.6. Contas Bancárias - CCS</b>", styles['Heading3']))
    
    if not resultados['ccs'].empty and len(cnpjs_validos) > 1:
        ccs_dados = []
        
        max_score_possivel += 4
        cpfs_contas = resultados['ccs'].groupby('nr_cpf')['cnpj'].nunique()
        cpfs_compart = cpfs_contas[cpfs_contas > 1]
        
        if len(cpfs_compart) > 0:
            pontos_cpf = min(len(cpfs_compart) * 2, 4)
            ccs_dados.append(['CPFs Múltiplas Contas', str(len(cpfs_compart)), 'DETECTADOS',
                             f'+{pontos_cpf:.1f}', 'CRÍTICO'])
            evidencias_pdf['ccs_cpf_compartilhado'] = True
            score_similaridade += pontos_cpf
            
            # Listar CPFs compartilhados
            story.append(Paragraph("CPFs com acesso a múltiplas contas:", styles['Normal']))
            for cpf, qtd in list(cpfs_compart.items())[:5]:
                story.append(Paragraph(f"• CPF {cpf}: {qtd} CNPJs",
                                      ParagraphStyle('Indent', parent=styles['Normal'], leftIndent=20)))
        else:
            ccs_dados.append(['CPFs Múltiplas Contas', '0', 'NÃO DETECTADOS', '0.0', '-'])
        
        if ccs_dados:
            table = Table([['Indicador', 'Quantidade', 'Status', 'Pontos', 'Nível']] + ccs_dados,
                         colWidths=[1.8*inch, 1.2*inch, 1.3*inch, 0.8*inch, 0.9*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(table)
            story.append(Spacer(1, 0.2*inch))
    
    story.append(PageBreak())
    
    # 3.7 - SCORE FINAL DE SIMILARIDADE
    story.append(Paragraph("<b>3.7. Score Final de Similaridade</b>", styles['Heading3']))
    
    # Tabela de score
    percentual = (score_similaridade / max_score_possivel * 100) if max_score_possivel > 0 else 0
    total_evidencias = len([v for v in evidencias_pdf.values() if v])
    
    # Determinar nível de risco
    if score_similaridade >= 15:
        nivel_risco = "CRÍTICO"
        cor_nivel = colors.red
    elif score_similaridade >= 10:
        nivel_risco = "ALTO"
        cor_nivel = colors.orange
    elif score_similaridade >= 5:
        nivel_risco = "MODERADO"
        cor_nivel = colors.yellow
    else:
        nivel_risco = "BAIXO"
        cor_nivel = colors.green
    
    dados_score = [
        ['Métrica', 'Valor'],
        ['Score de Similaridade', f"{score_similaridade:.1f} pontos"],
        ['Score Máximo Possível', f"{max_score_possivel:.1f} pontos"],
        ['Percentual Atingido', f"{percentual:.1f}%"],
        ['Total de Evidências', str(total_evidencias)],
        ['Nível de Risco', nivel_risco]
    ]
    
    table = Table(dados_score, colWidths=[3*inch, 3*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('BACKGROUND', (0, -1), (-1, -1), cor_nivel)
    ]))
    story.append(table)
    story.append(Spacer(1, 0.2*inch))
    
    # Lista de evidências encontradas
    if evidencias_pdf:
        story.append(Paragraph("<b>Evidências Identificadas:</b>", styles['Heading3']))
        
        categorias_evidencias = {
                    'Cadastrais': ['razao_social', 'fantasia', 'cnae', 'contador', 'endereco'],
                    'Societárias': ['socios_compartilhados'],
                    'Fiscais': ['receita_excesso', 'receita_uniforme', 'receita_correlacao'],
                    'Operacionais': ['ip_compartilhado', 'clientes_comuns', 'fornecedores_comuns', 'produtos_comuns', 'desc_produtos_comuns', 'tel_emit_compartilhado', 'email_dest_compartilhado', 'endereco_nfe_emit', 'endereco_nfe_dest'],
                    'C115': ['c115_identificador', 'c115_telefone'],
                    'Financeiras': ['ccs_cpf_compartilhado', 'socios_meios_pagamento']
                }
        
        for categoria, chaves in categorias_evidencias.items():
            evidencias_cat = [k.replace('_', ' ').title() for k in chaves if evidencias_pdf.get(k, False)]
            if evidencias_cat:
                story.append(Paragraph(f"<b>{categoria}:</b> {', '.join(evidencias_cat)}", styles['Normal']))
        
        story.append(Spacer(1, 0.2*inch))
    
    # Conclusão baseada no score
    story.append(Paragraph("<b>Conclusão da Análise de Similaridade:</b>", styles['Heading3']))
    
    if score_similaridade >= 15:
        conclusao = """
        FORTE EVIDÊNCIA DE GRUPO ECONÔMICO - Os CNPJs analisados apresentam múltiplas e graves 
        evidências de pertencerem ao mesmo grupo econômico. As similaridades detectadas em dados 
        cadastrais, vínculos societários, padrões operacionais e indicadores fiscais sugerem 
        fortemente operação coordenada e gestão centralizada.
        
        RECOMENDAÇÃO URGENTE: Criação imediata de grupo GEI para monitoramento integrado, análise 
        aprofundada de possível planejamento tributário abusivo, verificação de fraude à lei 
        (fracionamento artificial), intimação dos contribuintes para esclarecimentos e considerar 
        procedimento fiscal conjunto.
        """
    elif score_similaridade >= 10:
        conclusao = """
        EVIDÊNCIA SIGNIFICATIVA DE GRUPO ECONÔMICO - Os CNPJs apresentam várias características 
        compatíveis com grupo econômico. As evidências encontradas justificam investigação mais 
        aprofundada.
        
        RECOMENDAÇÃO: Criação de grupo GEI para monitoramento, análise complementar com dados 
        adicionais, solicitar documentação adicional aos contribuintes, monitoramento reforçado 
        nos próximos períodos e verificar histórico de alterações cadastrais.
        """
    elif score_similaridade >= 5:
        conclusao = """
        INDÍCIOS MODERADOS DE GRUPO ECONÔMICO - Alguns indícios sugerem possível vinculação entre 
        os CNPJs, mas não são conclusivos. Recomenda-se monitoramento e coleta de evidências 
        adicionais.
        
        RECOMENDAÇÃO: Monitoramento periódico dos CNPJs, atenção a novos indícios que possam surgir, 
        cruzamento com outras bases de dados e acompanhar evolução das receitas.
        """
    else:
        conclusao = """
        BAIXA EVIDÊNCIA DE GRUPO ECONÔMICO - Com base nos dados analisados, não foram encontradas 
        evidências significativas de que os CNPJs pertençam ao mesmo grupo econômico. As 
        similaridades detectadas podem ser coincidências ou características comuns do setor.
        
        RECOMENDAÇÃO: Monitoramento de rotina conforme procedimentos padrão e atenção caso surjam 
        novos indícios futuramente.
        """
    
    story.append(Paragraph(conclusao, styles['Normal']))
    story.append(PageBreak())
    
    # DEMAIS SEÇÕES (mantidas do código original)
    # SEÇÃO 4: VÍNCULOS SOCIETÁRIOS (detalhamento)
    if not resultados.get('socios', pd.DataFrame()).empty:
        story.append(Paragraph(f"<b>4. VÍNCULOS SOCIETÁRIOS ({len(resultados['socios'])} vínculos)</b>", styles['Heading2']))
        
        dados_socios = [['CNPJ', 'CPF Sócio', 'Qualificação', 'Relação Ativa']]
        for _, row in resultados['socios'].head(50).iterrows():
            dados_socios.append([
                str(row.get('cnpj', '')),
                str(row.get('cpf_socio', '')),
                str(row.get('nm_qualificacao', ''))[:25],
                str(row.get('sn_relacao_ativa', ''))
            ])
        
        table = Table(dados_socios, colWidths=[1.5*inch, 1.5*inch, 2*inch, 1*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(PageBreak())
    
    # SEÇÃO 5: FATURAMENTO DECLARADO (PGDAS + DIME)
    tem_pgdas = not resultados.get('pgdas', pd.DataFrame()).empty
    tem_dime = not resultados.get('dime', pd.DataFrame()).empty
    tem_faturamento = not resultados.get('faturamento', pd.DataFrame()).empty

    if tem_pgdas or tem_dime or tem_faturamento:
        story.append(Paragraph("<b>5. FATURAMENTO DECLARADO (PGDAS / DIME)</b>", styles['Heading2']))

        # 5.1 PGDAS (Simples Nacional)
        if tem_pgdas:
            story.append(Paragraph("<b>5.1 PGDAS - Simples Nacional</b>", styles['Heading3']))

            receita_max_pgdas = resultados['pgdas'].groupby('cnpj')['receita_12m'].max().reset_index()

            dados_pgdas = [['CNPJ', 'Receita Máxima (12m)', 'Acima Limite SN']]
            for _, row in receita_max_pgdas.iterrows():
                receita = row['receita_12m']
                dados_pgdas.append([
                    str(row['cnpj']),
                    formatar_moeda(receita),
                    'SIM' if receita > 4800000 else 'NÃO'
                ])

            table_pgdas = Table(dados_pgdas, colWidths=[2*inch, 2.5*inch, 1.5*inch])
            table_pgdas.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(table_pgdas)
            story.append(Spacer(1, 0.2*inch))

        # 5.2 DIME (Regime Normal)
        if tem_dime:
            story.append(Paragraph("<b>5.2 DIME - Regime Normal</b>", styles['Heading3']))

            df_dime = resultados['dime']
            receita_max_dime = df_dime.groupby('cnpj')['receita_12m'].max().reset_index()

            dados_dime = [['CNPJ', 'Faturamento Máximo (12m)', 'Total Créditos', 'Total Débitos']]
            for _, row in receita_max_dime.iterrows():
                cnpj = str(row['cnpj'])
                faturamento = row['receita_12m']
                # Buscar totais do CNPJ
                dados_cnpj = df_dime[df_dime['cnpj'] == cnpj]
                total_creditos = dados_cnpj['total_creditos'].sum() if 'total_creditos' in dados_cnpj.columns else 0
                total_debitos = dados_cnpj['total_debitos'].sum() if 'total_debitos' in dados_cnpj.columns else 0
                dados_dime.append([
                    cnpj,
                    formatar_moeda(faturamento),
                    formatar_moeda(total_creditos),
                    formatar_moeda(total_debitos)
                ])

            table_dime = Table(dados_dime, colWidths=[1.8*inch, 1.8*inch, 1.5*inch, 1.5*inch])
            table_dime.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2196F3')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(table_dime)
            story.append(Spacer(1, 0.2*inch))

        # 5.3 Resumo Consolidado
        story.append(Paragraph("<b>5.3 Resumo Consolidado do Grupo</b>", styles['Heading3']))

        # Calcular totais
        receita_total_pgdas = resultados['pgdas'].groupby('cnpj')['receita_12m'].max().sum() if tem_pgdas else 0
        receita_total_dime = resultados['dime'].groupby('cnpj')['receita_12m'].max().sum() if tem_dime else 0
        receita_total_grupo = receita_total_pgdas + receita_total_dime

        dados_resumo = [
            ['Fonte', 'Qtd CNPJs', 'Faturamento Total'],
            ['PGDAS (Simples)', str(len(resultados['pgdas']['cnpj'].unique())) if tem_pgdas else '0', formatar_moeda(receita_total_pgdas)],
            ['DIME (Normal)', str(len(resultados['dime']['cnpj'].unique())) if tem_dime else '0', formatar_moeda(receita_total_dime)],
            ['TOTAL GRUPO', '-', formatar_moeda(receita_total_grupo)]
        ]

        table_resumo = Table(dados_resumo, colWidths=[2.5*inch, 1.5*inch, 2.5*inch])
        table_resumo.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('BACKGROUND', (0, 3), (-1, 3), colors.HexColor('#FFE0E0')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 3), (-1, 3), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table_resumo)

        # Alerta se ultrapassar limite
        if receita_total_grupo > 4800000:
            excesso = receita_total_grupo - 4800000
            story.append(Spacer(1, 0.1*inch))
            alerta_style = ParagraphStyle('Alerta', parent=styles['Normal'], textColor=colors.red, fontSize=10)
            story.append(Paragraph(
                f"<b>ALERTA: Faturamento total do grupo ({formatar_moeda(receita_total_grupo)}) excede o limite do Simples Nacional em {formatar_moeda(excesso)}</b>",
                alerta_style
            ))

        story.append(PageBreak())
    
    # SEÇÃO 6: INDÍCIOS FISCAIS
    if not resultados.get('indicios', pd.DataFrame()).empty:
        story.append(Paragraph(f"<b>6. INDÍCIOS FISCAIS ({len(resultados['indicios'])} indícios)</b>", styles['Heading2']))
        
        resumo_indicios = resultados['indicios']['tx_descricao_indicio'].value_counts().reset_index()
        resumo_indicios.columns = ['Tipo', 'Quantidade']
        
        dados_indicios_resumo = [['Tipo de Indício', 'Quantidade']]
        for _, row in resumo_indicios.iterrows():
            dados_indicios_resumo.append([
                str(row['Tipo'])[:50],
                str(row['Quantidade'])
            ])
        
        table = Table(dados_indicios_resumo, colWidths=[4.5*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(PageBreak())
    
    # Rodapé
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph(
        "Sistema GEI v3.0 - Receita Estadual de Santa Catarina",
        ParagraphStyle('Footer', parent=styles['Normal'], fontSize=10, alignment=TA_CENTER)
    ))
    story.append(Paragraph(
        "Documento de caráter sigiloso - Uso restrito à fiscalização tributária",
        ParagraphStyle('Footer2', parent=styles['Normal'], fontSize=9, alignment=TA_CENTER, textColor=colors.grey)
    ))
    
    doc.build(story)
    buffer.seek(0)
    return buffer
    
def gerar_pdf_dossie(dossie, num_grupo):
    """Gera PDF completo com todas as informações do grupo"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    story = []
    
    # Função auxiliar para valores seguros
    def safe_value(value, default='N/A'):
        """Retorna valor seguro ou default se None/NaN"""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        return value
    
    def safe_int(value, default=0):
        """Retorna int seguro ou default"""
        try:
            return int(value) if pd.notna(value) else default
        except (ValueError, TypeError):
            return default
    
    def safe_float(value, decimals=2, default=0.0):
        """Retorna float formatado ou default"""
        try:
            return f"{float(value):.{decimals}f}" if pd.notna(value) else f"{default:.{decimals}f}"
        except (ValueError, TypeError):
            return f"{default:.{decimals}f}"
    
    # Título
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=20,
        textColor=colors.HexColor('#1f77b4'),
        spaceAfter=20,
        alignment=TA_CENTER
    )
    story.append(Paragraph(f"DOSSIÊ COMPLETO - GRUPO ECONÔMICO {num_grupo}", title_style))
    story.append(Paragraph("Receita Estadual de Santa Catarina", styles['Normal']))
    story.append(Paragraph(f"Data de Geração: {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # SEÇÃO 1: INFORMAÇÕES PRINCIPAIS
    if not dossie['principal'].empty:
        info = dossie['principal'].iloc[0]
        
        story.append(Paragraph("<b>1. INFORMAÇÕES PRINCIPAIS DO GRUPO</b>", styles['Heading2']))
        
        dados_principais = [
            ['Métrica', 'Valor'],
            ['Número do Grupo', str(num_grupo)],
            ['Score Final CCS', safe_float(info.get('score_final_ccs'), 2)],
            ['Score Final Avançado', safe_float(info.get('score_final_avancado'), 2)],
            ['Quantidade de CNPJs', str(safe_int(info.get('qntd_cnpj')))],
            ['Receita Máxima (12 meses)', formatar_moeda(safe_value(info.get('valor_max'), 0))],
            ['Total de Funcionários', str(safe_int(info.get('total_funcionarios')))],
            ['Nível de Risco C115', str(safe_value(info.get('nivel_risco_grupo_economico')))],
            ['Nível de Risco CCS', str(safe_value(info.get('nivel_risco_ccs')))],
            ['Total de Indícios', str(safe_int(info.get('qtd_total_indicios')))],
            ['Sócios Compartilhados', str(safe_int(info.get('qtd_socios_compartilhados')))],
            ['Índice de Interconexão', safe_float(info.get('indice_interconexao'), 3)],
            ['Índice de Risco CCS', safe_float(info.get('indice_risco_ccs'), 4)],
            ['Score Inconsistências NFe', safe_float(info.get('total'), 2)]
        ]
        
        table = Table(dados_principais, colWidths=[3*inch, 3*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.3*inch))
    
    # SEÇÃO 2: CNPJs DO GRUPO
    if not dossie['cnpjs'].empty:
        story.append(Paragraph(f"<b>2. CNPJs DO GRUPO ({len(dossie['cnpjs'])} empresas)</b>", styles['Heading2']))
        
        for idx, row in dossie['cnpjs'].iterrows():
            story.append(Paragraph(f"<b>CNPJ:</b> {safe_value(row.get('cnpj'))}", styles['Normal']))
            story.append(Paragraph(f"<b>Razão Social:</b> {safe_value(row.get('nm_razao_social'))}", styles['Normal']))
            
            if pd.notna(row.get('nm_fantasia')) and str(row.get('nm_fantasia')).strip():
                story.append(Paragraph(f"<b>Nome Fantasia:</b> {row.get('nm_fantasia')}", styles['Normal']))
            if pd.notna(row.get('cd_cnae')):
                story.append(Paragraph(f"<b>CNAE:</b> {row.get('cd_cnae')}", styles['Normal']))
            if pd.notna(row.get('nm_municipio')):
                story.append(Paragraph(f"<b>Município:</b> {row.get('nm_municipio')}", styles['Normal']))
            if pd.notna(row.get('nm_contador')) and str(row.get('nm_contador')).strip():
                story.append(Paragraph(f"<b>Contador:</b> {row.get('nm_contador')}", styles['Normal']))
            if pd.notna(row.get('dt_constituicao_empresa')):
                story.append(Paragraph(f"<b>Data Constituição:</b> {row.get('dt_constituicao_empresa')}", styles['Normal']))
            if pd.notna(row.get('nm_reg_apuracao')):
                story.append(Paragraph(f"<b>Regime Apuração:</b> {row.get('nm_reg_apuracao')}", styles['Normal']))
            
            story.append(Spacer(1, 0.15*inch))
    
    story.append(PageBreak())
    
    # SEÇÃO 3: VÍNCULOS SOCIETÁRIOS
    if not dossie['socios'].empty:
        story.append(Paragraph(f"<b>3. VÍNCULOS SOCIETÁRIOS ({len(dossie['socios'])} registros)</b>", styles['Heading2']))
        
        socios_unicos = dossie['socios']['cpf_socio'].nunique() if 'cpf_socio' in dossie['socios'].columns else 0
        story.append(Paragraph(f"<b>Total de sócios únicos:</b> {socios_unicos}", styles['Normal']))
        story.append(Spacer(1, 0.2*inch))
        
        # Mapear CNPJs para razões sociais
        cnpj_para_razao = {}
        if not dossie['cnpjs'].empty:
            for _, row in dossie['cnpjs'].iterrows():
                cnpj_para_razao[safe_value(row.get('cnpj'))] = safe_value(row.get('nm_razao_social'))
        
        for idx, row in dossie['socios'].head(50).iterrows():
            cpf = safe_value(row.get('cpf_socio'))
            qtd_empresas = safe_value(row.get('qtd_empresas'), 'N/A')
            
            story.append(Paragraph(f"<b>• CPF:</b> {cpf}", styles['Normal']))
            story.append(Paragraph(f"  <b>Participa de {qtd_empresas} empresas do grupo</b>", styles['Normal']))
            story.append(Spacer(1, 0.1*inch))
        
        story.append(Spacer(1, 0.2*inch))
    
    # SEÇÃO 4: INDÍCIOS FISCAIS
    if not dossie['indicios'].empty:
        story.append(Paragraph(f"<b>4. INDÍCIOS FISCAIS DETALHADOS ({len(dossie['indicios'])} registros)</b>", styles['Heading2']))
        
        tipos = dossie['indicios']['tx_descricao_indicio'].value_counts()
        story.append(Paragraph("<b>Resumo por Tipo de Indício:</b>", styles['Heading3']))
        for tipo, qtd in tipos.items():
            story.append(Paragraph(f"• {tipo}: {qtd} ocorrências", styles['Normal']))
        
        story.append(Spacer(1, 0.2*inch))
        story.append(Paragraph("<b>Lista Completa de Indícios:</b>", styles['Heading3']))
        
        for idx, row in dossie['indicios'].head(100).iterrows():
            story.append(Paragraph(f"• {safe_value(row.get('tx_descricao_indicio'))}", styles['Normal']))
            if pd.notna(row.get('cnpj')):
                story.append(Paragraph(f"  CNPJ: {row.get('cnpj')}", styles['Normal']))
            if pd.notna(row.get('tx_descricao_complemento')):
                complemento = str(row.get('tx_descricao_complemento'))[:100]
                story.append(Paragraph(f"  Complemento: {complemento}", styles['Normal']))
        
        story.append(Spacer(1, 0.2*inch))
    
    story.append(PageBreak())
    
    # SEÇÃO 5: DADOS FINANCEIROS
    if not dossie['principal'].empty:
        info = dossie['principal'].iloc[0]
        story.append(Paragraph("<b>5. ANÁLISE FINANCEIRA</b>", styles['Heading2']))
        
        valor_max = safe_value(info.get('valor_max'), 0)
        acima_limite = 'Sim' if (pd.notna(valor_max) and float(valor_max) > 4800000) else 'Não'
        
        dados_financeiros = [
            ['Métrica Financeira', 'Valor'],
            ['Receita Máxima (12m)', formatar_moeda(valor_max)],
            ['Acima Limite Simples Nacional', acima_limite],
            ['Índice Risco Faturamento/Funcionários', safe_float(info.get('indice_risco_fat_func'), 3)]
        ]
        
        table = Table(dados_financeiros, colWidths=[3*inch, 3*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.3*inch))
    
    # SEÇÃO 6: FUNCIONÁRIOS
    if not dossie['funcionarios'].empty:
        story.append(Paragraph("<b>6. DADOS DE FUNCIONÁRIOS</b>", styles['Heading2']))
        info_func = dossie['funcionarios'].iloc[0]
        
        dados_funcionarios = [
            ['Métrica', 'Valor'],
            ['Total de Funcionários', str(safe_int(info_func.get('total_funcionarios')))],
            ['CNPJs com Funcionários', str(safe_int(info_func.get('cnpjs_com_funcionarios')))]
        ]
        
        table = Table(dados_funcionarios, colWidths=[3*inch, 3*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.2*inch))
    
    # SEÇÃO 7: MEIOS DE PAGAMENTO
    if not dossie['pagamentos'].empty:
        story.append(Paragraph("<b>7. MEIOS DE PAGAMENTO</b>", styles['Heading2']))
        info_pag = dossie['pagamentos'].iloc[0]
        
        dados_pagamentos = [
            ['Tipo', 'Valor'],
            ['Pagamentos das Empresas', formatar_moeda(safe_value(info_pag.get('valor_meios_pagamento_empresas'), 0))],
            ['Pagamentos dos Sócios', formatar_moeda(safe_value(info_pag.get('valor_meios_pagamento_socios'), 0))]
        ]
        
        table = Table(dados_pagamentos, colWidths=[3*inch, 3*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.2*inch))
    
    # SEÇÃO 8: CONVÊNIO 115
    if not dossie['c115'].empty:
        story.append(Paragraph("<b>8. DADOS CONVÊNIO 115</b>", styles['Heading2']))
        info_c115 = dossie['c115'].iloc[0]
        
        dados_c115 = [
            ['Métrica C115', 'Valor'],
            ['Ranking de Risco', str(safe_int(info_c115.get('ranking_risco')))],
            ['Nível de Risco', str(safe_value(info_c115.get('nivel_risco_grupo_economico')))],
            ['Índice de Risco', safe_float(info_c115.get('indice_risco_grupo_economico'), 4)],
            ['CNPJs Relacionados', str(safe_int(info_c115.get('qtd_cnpjs_relacionados')))],
            ['% CNPJs Relacionados', safe_float(info_c115.get('perc_cnpjs_relacionados'), 1) + '%']
        ]
        
        table = Table(dados_c115, colWidths=[3*inch, 3*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.2*inch))
    
    story.append(PageBreak())
    
    # SEÇÃO 9: CCS
    if not dossie['principal'].empty:
        info = dossie['principal'].iloc[0]
        story.append(Paragraph("<b>9. PROCURAÇÃO BANCÁRIA (CCS)</b>", styles['Heading2']))
        
        dados_ccs = [
            ['Métrica CCS', 'Valor'],
            ['Índice de Risco CCS', safe_float(info.get('indice_risco_ccs'), 4)],
            ['Nível de Risco CCS', str(safe_value(info.get('nivel_risco_ccs')))],
            ['Total Contas Únicas', str(safe_int(info.get('ccs_total_contas_unicas')))],
            ['Contas Compartilhadas', str(safe_int(info.get('ccs_qtd_contas_compartilhadas')))],
            ['% Contas Compartilhadas', safe_float(info.get('ccs_perc_contas_compartilhadas'), 2) + '%'],
            ['Max CNPJs por Conta', str(safe_int(info.get('ccs_max_cnpjs_por_conta')))],
            ['Sobreposições de Responsáveis', str(safe_int(info.get('ccs_qtd_sobreposicoes_responsaveis')))],
            ['Média Dias Sobreposição', safe_float(info.get('ccs_media_dias_sobreposicao'), 0)],
            ['Aberturas Coordenadas', str(safe_int(info.get('ccs_qtd_datas_abertura_coordenada')))],
            ['Encerramentos Coordenados', str(safe_int(info.get('ccs_qtd_datas_encerramento_coordenado')))]
        ]
        
        table = Table(dados_ccs, colWidths=[3*inch, 3*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(table)
        story.append(Spacer(1, 0.2*inch))
        
        # Detalhamento de contas compartilhadas
        if not dossie['ccs_compartilhadas'].empty:
            story.append(Paragraph("<b>Contas Compartilhadas (Top 20):</b>", styles['Heading3']))
            for idx, row in dossie['ccs_compartilhadas'].head(20).iterrows():
                cpf = safe_value(row.get('nr_cpf'))
                banco = safe_value(row.get('nm_banco'))
                agencia = safe_value(row.get('cd_agencia'))
                conta = safe_value(row.get('nr_conta'))
                qtd_cnpjs = safe_int(row.get('qtd_cnpjs_usando_conta'))
                qtd_vinculos = safe_int(row.get('qtd_vinculos_ativos'))
                
                story.append(Paragraph(f"• CPF: {cpf} | Banco: {banco} | Agência: {agencia} | Conta: {conta}", styles['Normal']))
                story.append(Paragraph(f"  CNPJs usando: {qtd_cnpjs} | Vínculos ativos: {qtd_vinculos}", styles['Normal']))
    
    story.append(PageBreak())
    
    # SEÇÃO 10: INCONSISTÊNCIAS NFE
    if not dossie['inconsistencias'].empty:
        story.append(Paragraph(f"<b>10. INCONSISTÊNCIAS DE NFE ({len(dossie['inconsistencias'])} documentos)</b>", styles['Heading2']))
        
        tipos_incons = ['cliente_incons', 'email_incons', 'tel_dest_incons', 
                       'tel_emit_incons', 'codigo_produto_incons', 'fornecedor_incons',
                       'end_emit_incons', 'end_dest_incons', 'descricao_produto_incons', 
                       'ip_transmissao_incons']
        
        story.append(Paragraph("<b>Resumo por Tipo de Inconsistência:</b>", styles['Heading3']))
        
        for tipo in tipos_incons:
            if tipo in dossie['inconsistencias'].columns:
                total = len(dossie['inconsistencias'][dossie['inconsistencias'][tipo] == 'S'])
                if total > 0:
                    nome_tipo = tipo.replace('_incons', '').replace('_', ' ').title()
                    story.append(Paragraph(f"• {nome_tipo}: {total} ocorrências", styles['Normal']))
    
    # Rodapé final
    story.append(PageBreak())
    story.append(Paragraph("OBSERVAÇÕES FINAIS", styles['Heading2']))
    story.append(Paragraph("Este dossiê foi gerado automaticamente pelo Sistema GEI (Grupos Econômicos Interconectados) da Receita Estadual de Santa Catarina.", styles['Normal']))
    story.append(Paragraph("As informações contidas neste relatório são de caráter sigiloso e destinam-se exclusivamente ao uso da fiscalização tributária.", styles['Normal']))
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph("Sistema GEI v3.0 - Receita Estadual de Santa Catarina", 
                          ParagraphStyle('Footer', parent=styles['Normal'], fontSize=10, alignment=TA_CENTER)))
    
    doc.build(story)
    buffer.seek(0)
    return buffer

def criar_filtros_sidebar():
    """Cria filtros na sidebar"""
    filtros = {}
    
    with st.sidebar.expander("⚙️ Filtros", expanded=False):
        filtros['score_min'] = st.slider("Score Mínimo", 0.0, 50.0, 0.0, 0.5)
        filtros['score_max'] = st.slider("Score Máximo", 0.0, 50.0, 50.0, 0.5)
        filtros['cnpj_min'] = st.number_input("Min. CNPJs", min_value=1, value=1)
        filtros['cnpj_max'] = st.number_input("Max. CNPJs", min_value=1, value=100)
        filtros['com_indicios'] = st.checkbox("Apenas com indícios")
        filtros['tema'] = st.selectbox("Tema", ["plotly", "plotly_white", "plotly_dark"])
    
    return filtros

# =============================================================================
# FUNÇÕES DAS PÁGINAS PRINCIPAIS
# =============================================================================

def analise_pontual(engine, dados, filtros):
    """Análise pontual de CNPJs específicos"""
    st.markdown("<h1 class='main-header'>Análise Pontual de CNPJs</h1>", unsafe_allow_html=True)
    
    st.info("""
    Esta ferramenta permite analisar CNPJs específicos executando todas as verificações do sistema GEI
    sem criar registros permanentes. Os resultados são exibidos em tempo real para análise imediata.
    """)
    
    # Inicializar session_state
    if 'analise_resultados' not in st.session_state:
        st.session_state.analise_resultados = None
    if 'cnpjs_validos_analise' not in st.session_state:
        st.session_state.cnpjs_validos_analise = []
    
    # Entrada de CNPJs
    st.subheader("1. Entrada de CNPJs")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        cnpjs_input = st.text_area(
            "Digite os CNPJs (um por linha, apenas números):",
            height=150,
            placeholder="12345678000190\n98765432000112\n..."
        )
    
    with col2:
        st.write("**Ou carregue um arquivo:**")
        uploaded_file = st.file_uploader("CSV/TXT", type=['csv', 'txt'])
    
    # Processar CNPJs
    cnpjs_lista = []
    
    if uploaded_file is not None:
        try:
            content = uploaded_file.read().decode('utf-8')
            cnpjs_lista = [linha.strip() for linha in content.split('\n') if linha.strip()]
        except Exception as e:
            st.error(f"Erro ao ler arquivo: {e}")
    elif cnpjs_input:
        cnpjs_lista = [linha.strip() for linha in cnpjs_input.split('\n') if linha.strip()]
    
    # Limpar e validar CNPJs
    cnpjs_validos = []
    cnpjs_invalidos = []
    
    for cnpj in cnpjs_lista:
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
        if len(cnpj_limpo) == 14:
            cnpjs_validos.append(cnpj_limpo)
        elif cnpj_limpo:
            cnpjs_invalidos.append(cnpj)
    
    # Mostrar validação
    if cnpjs_validos or cnpjs_invalidos:
        col1, col2 = st.columns(2)
        with col1:
            st.success(f"✅ {len(cnpjs_validos)} CNPJs válidos")
        with col2:
            if cnpjs_invalidos:
                st.warning(f"⚠️ {len(cnpjs_invalidos)} CNPJs inválidos")
                with st.expander("Ver CNPJs inválidos"):
                    st.write(cnpjs_invalidos)
    
    if not cnpjs_validos:
        st.warning("Nenhum CNPJ válido para análise.")
        return
    
    # Botão de análise
    st.divider()
    
    if st.button("🔍 Executar Análise Completa", type="primary", width='stretch'):
        
        st.session_state.cnpjs_validos_analise = cnpjs_validos
        cnpjs_str = "', '".join(cnpjs_validos)
        
        with st.spinner("Executando análises..."):
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            st.session_state.analise_resultados = {}
            
            # 1. DADOS CADASTRAIS
            try:
                status_text.text("1/10 - Buscando dados cadastrais...")
                progress_bar.progress(10)
                
                query_cadastro = f"""
                SELECT 
                    nu_cnpj as cnpj,
                    nm_razao_social,
                    nm_fantasia,
                    cd_cnae,
                    nm_reg_apuracao,
                    dt_constituicao_empresa,
                    nm_munic as municipio,
                    nm_contador,
                    nm_logradouro,
                    nu_logradouro,
                    tx_complemento,
                    nm_bairro
                FROM usr_sat_ods.vw_ods_contrib
                WHERE nu_cnpj IN ('{cnpjs_str}')
                """
                st.session_state.analise_resultados['cadastro'] = pd.read_sql(query_cadastro, engine)
            except Exception as e:
                st.warning(f"Erro ao buscar cadastro: {e}")
                st.session_state.analise_resultados['cadastro'] = pd.DataFrame()
            
            # 2. VÍNCULOS SOCIETÁRIOS
            try:
                status_text.text("2/10 - Analisando vínculos societários...")
                progress_bar.progress(20)
                
                query_socios = f"""
                SELECT 
                    nu_cnpj_princ as cnpj,
                    nu_cnpj_cpf_secund as cpf_socio,
                    nm_relacao,
                    nm_qualificacao,
                    dt_inicio_relacao,
                    dt_fim_relacao,
                    pe_capital_empresa,
                    sn_relacao_ativa
                FROM usr_sat_ods.vw_cad_vinculo
                WHERE nu_cnpj_princ IN ('{cnpjs_str}')
                AND nm_relacao != 'CONTABILISTA'
                """
                df_socios = pd.read_sql(query_socios, engine)
                
                if not df_socios.empty:
                    socios_compartilhados = df_socios.groupby('cpf_socio')['cnpj'].nunique()
                    socios_compartilhados = socios_compartilhados[socios_compartilhados > 1]
                    
                    st.session_state.analise_resultados['socios'] = df_socios
                    st.session_state.analise_resultados['socios_compartilhados'] = socios_compartilhados
                else:
                    st.session_state.analise_resultados['socios'] = pd.DataFrame()
                    st.session_state.analise_resultados['socios_compartilhados'] = pd.Series()
                    
            except Exception as e:
                st.warning(f"Erro ao buscar sócios: {e}")
                st.session_state.analise_resultados['socios'] = pd.DataFrame()
                st.session_state.analise_resultados['socios_compartilhados'] = pd.Series()
            
            # 3. PGDAS
            try:
                status_text.text("3/10 - Verificando receitas declaradas (PGDAS)...")
                progress_bar.progress(30)
                
                query_pgdas = f"""
                WITH base AS (
                    SELECT
                        nu_cnpj,
                        nu_per_ref,
                        SUM(vl_rec_bruta_estab) OVER (
                            PARTITION BY nu_cnpj
                            ORDER BY nu_per_ref
                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                        ) AS vl_rec_bruta_12m
                    FROM usr_sat_ods.sna_pgdasd_estabelecimento_raw
                    WHERE nu_cnpj IN ('{cnpjs_str}')
                    AND nu_per_ref BETWEEN 202001 AND 202509
                )
                SELECT 
                    nu_cnpj as cnpj,
                    nu_per_ref as periodo,
                    vl_rec_bruta_12m as receita_12m
                FROM base
                WHERE vl_rec_bruta_12m IS NOT NULL
                ORDER BY nu_cnpj, nu_per_ref DESC
                """
                st.session_state.analise_resultados['pgdas'] = pd.read_sql(query_pgdas, engine)
            except Exception as e:
                st.warning(f"Erro ao buscar PGDAS: {e}")
                st.session_state.analise_resultados['pgdas'] = pd.DataFrame()

            # 3.5. DIME (Regime Normal)
            try:
                status_text.text("3.5/10 - Verificando receitas declaradas (DIME - Regime Normal)...")
                progress_bar.progress(35)

                query_dime = f"""
                WITH base AS (
                    SELECT
                        REGEXP_REPLACE(TRIM(CAST(NU_CNPJ AS STRING)), '[^0-9]', '') AS nu_cnpj,
                        nu_per_ref,
                        COALESCE(VL_FATURAMENTO, 0) AS vl_faturamento,
                        COALESCE(VL_RECEITA_BRUTA, 0) AS vl_receita_bruta,
                        COALESCE(VL_TOT_CRED, 0) AS vl_tot_cred,
                        COALESCE(VL_TOT_DEB, 0) AS vl_tot_deb,
                        COALESCE(VL_DEB_RECOLHER, 0) AS vl_deb_recolher,
                        sn_com_movimento
                    FROM usr_sat_ods.ods_decl_dime_raw
                    WHERE REGEXP_REPLACE(TRIM(CAST(NU_CNPJ AS STRING)), '[^0-9]', '') IN ('{cnpjs_str}')
                    AND sn_cancelada = 0
                    AND nu_per_ref BETWEEN 202001 AND 202509
                ),
                receitas_12m AS (
                    SELECT
                        nu_cnpj,
                        nu_per_ref,
                        vl_faturamento,
                        vl_receita_bruta,
                        vl_tot_cred,
                        vl_tot_deb,
                        vl_deb_recolher,
                        sn_com_movimento,
                        SUM(vl_faturamento) OVER (
                            PARTITION BY nu_cnpj
                            ORDER BY nu_per_ref
                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                        ) AS vl_faturamento_12m
                    FROM base
                )
                SELECT
                    nu_cnpj AS cnpj,
                    nu_per_ref AS periodo,
                    vl_faturamento AS faturamento_mensal,
                    vl_receita_bruta AS receita_bruta_mensal,
                    vl_faturamento_12m AS receita_12m,
                    vl_tot_cred AS total_creditos,
                    vl_tot_deb AS total_debitos,
                    vl_deb_recolher AS debito_recolher,
                    sn_com_movimento AS com_movimento,
                    CASE
                        WHEN vl_faturamento = 0 THEN 'SEM MOVIMENTO'
                        WHEN vl_tot_cred = 0 AND vl_tot_deb = 0 THEN 'ZERADA'
                        ELSE 'NORMAL'
                    END AS situacao_declaracao
                FROM receitas_12m
                WHERE vl_faturamento_12m IS NOT NULL
                ORDER BY nu_cnpj, nu_per_ref DESC
                """
                st.session_state.analise_resultados['dime'] = pd.read_sql(query_dime, engine)
            except Exception as e:
                st.warning(f"Erro ao buscar DIME: {e}")
                st.session_state.analise_resultados['dime'] = pd.DataFrame()

            # 3.6. Consolidar Faturamento (PGDAS + DIME)
            try:
                df_pgdas = st.session_state.analise_resultados.get('pgdas', pd.DataFrame())
                df_dime = st.session_state.analise_resultados.get('dime', pd.DataFrame())
                df_cadastro = st.session_state.analise_resultados.get('cadastro', pd.DataFrame())

                # Identificar regime de cada CNPJ pelo cadastro
                regimes_por_cnpj = {}
                if not df_cadastro.empty and 'nm_reg_apuracao' in df_cadastro.columns:
                    for _, row in df_cadastro.iterrows():
                        cnpj = str(row.get('cnpj', '')).replace('.', '').replace('/', '').replace('-', '')
                        regime = str(row.get('nm_reg_apuracao', '')).upper()
                        # Simples Nacional usa PGDAS, Regime Normal usa DIME
                        if 'SIMPLES' in regime or 'SN' in regime:
                            regimes_por_cnpj[cnpj] = 'PGDAS'
                        else:
                            regimes_por_cnpj[cnpj] = 'DIME'

                # Consolidar faturamento
                faturamento_consolidado = []

                # Adicionar dados do PGDAS (para empresas do Simples)
                if not df_pgdas.empty:
                    for _, row in df_pgdas.iterrows():
                        cnpj = str(row['cnpj']).replace('.', '').replace('/', '').replace('-', '')
                        faturamento_consolidado.append({
                            'cnpj': cnpj,
                            'periodo': row['periodo'],
                            'receita_12m': row['receita_12m'],
                            'fonte': 'PGDAS',
                            'regime': 'Simples Nacional'
                        })

                # Adicionar dados da DIME (para empresas do Regime Normal)
                if not df_dime.empty:
                    for _, row in df_dime.iterrows():
                        cnpj = str(row['cnpj']).replace('.', '').replace('/', '').replace('-', '')
                        faturamento_consolidado.append({
                            'cnpj': cnpj,
                            'periodo': row['periodo'],
                            'receita_12m': row['receita_12m'],
                            'faturamento_mensal': row.get('faturamento_mensal', 0),
                            'receita_bruta_mensal': row.get('receita_bruta_mensal', 0),
                            'total_creditos': row.get('total_creditos', 0),
                            'total_debitos': row.get('total_debitos', 0),
                            'fonte': 'DIME',
                            'regime': 'Regime Normal'
                        })

                if faturamento_consolidado:
                    st.session_state.analise_resultados['faturamento'] = pd.DataFrame(faturamento_consolidado)
                else:
                    st.session_state.analise_resultados['faturamento'] = pd.DataFrame()

            except Exception as e:
                st.warning(f"Erro ao consolidar faturamento: {e}")
                st.session_state.analise_resultados['faturamento'] = pd.DataFrame()

            # 4. NFE
            try:
                status_text.text("4/10 - Analisando notas fiscais (NFe/NFCe)...")
                progress_bar.progress(40)
                
                query_nfe = f"""
                SELECT
                    a.chave AS nfe_nu_chave_acesso,
                    a.dhemi_orig AS nfe_dt_emissao,
                    a.procnfe.nfe.infnfe.emit.cnpj AS nfe_cnpj_cpf_emit,
                    a.procnfe.nfe.infnfe.dest.cnpj AS nfe_cnpj_cpf_dest,
                    a.procnfe.nfe.infnfe.dest.email AS nfe_dest_email,
                    a.procnfe.nfe.infnfe.emit.enderemit.fone AS nfe_emit_telefone,
                    a.ip_transmissor AS nfe_ip_transmissao,
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
                    ) AS nfe_dest_end_completo
                FROM nfe.nfe a, a.procnfe.nfe.infnfe.det b
                WHERE (a.procnfe.nfe.infnfe.emit.cnpj IN ('{cnpjs_str}')
                   OR a.procnfe.nfe.infnfe.dest.cnpj IN ('{cnpjs_str}'))
                AND a.situacao = 1
                AND CAST((a.ano_emissao * 100 + a.mes_emissao) AS STRING) LIKE '2025%'
                LIMIT 10000
                """
                st.session_state.analise_resultados['nfe'] = pd.read_sql(query_nfe, engine)
            except Exception as e:
                st.warning(f"Erro ao buscar NFe: {e}")
                st.session_state.analise_resultados['nfe'] = pd.DataFrame()
                
            # 5. C115
            try:
                status_text.text("5/10 - Verificando dados C115...")
                progress_bar.progress(50)
                
                query_c115 = f"""
                SELECT 
                    nu_cnpj_cpf_tomador as cnpj_tomador,
                    nu_identificador_tomador,
                    nu_tel_contato,
                    nu_tel_ou_unidade_consumidora,
                    COUNT(*) as qtd_registros,
                    COUNT(DISTINCT dt_emissao) as qtd_datas_distintas
                FROM c115.c115_dados_cadastrais_dest
                WHERE nu_cnpj_cpf_tomador IN ('{cnpjs_str}')
                GROUP BY 
                    nu_cnpj_cpf_tomador,
                    nu_identificador_tomador,
                    nu_tel_contato,
                    nu_tel_ou_unidade_consumidora
                """
                st.session_state.analise_resultados['c115'] = pd.read_sql(query_c115, engine)
            except Exception as e:
                st.warning(f"Erro ao buscar C115: {e}")
                st.session_state.analise_resultados['c115'] = pd.DataFrame()
            
            # 6. CCS
            try:
                status_text.text("6/10 - Verificando contas bancárias (CCS)...")
                progress_bar.progress(60)
                
                query_ccs = f"""
                SELECT 
                    nr_cnpj as cnpj,
                    nr_cpf,
                    nm_responsavel,
                    nm_banco,
                    cd_agencia,
                    nr_conta,
                    tp_conta,
                    dt_abertura,
                    dt_encerramento,
                    tp_responsavel,
                    dt_inicio_responsavel,
                    dt_final_responsavel
                FROM usr_sat_fsn.fsn_conta_bancaria
                WHERE nr_cnpj IN ('{cnpjs_str}')
                AND (valido IS NULL OR valido = 1)
                """
                st.session_state.analise_resultados['ccs'] = pd.read_sql(query_ccs, engine)
            except Exception as e:
                st.warning(f"Erro ao buscar CCS: {e}")
                st.session_state.analise_resultados['ccs'] = pd.DataFrame()
            
            # 7. FUNCIONÁRIOS
            try:
                status_text.text("7/10 - Verificando funcionários (RAIS/CAGED)...")
                progress_bar.progress(70)
                
                query_funcionarios = f"""
                SELECT 
                    cnpj_cei as cnpj,
                    COUNT(DISTINCT cpf) as total_funcionarios,
                    AVG(vl_remun_media_nom) as remuneracao_media
                FROM rais_caged.vw_rais_vinculos
                WHERE cnpj_cei IN ('{cnpjs_str}')
                AND motivo_desligamento = 'NAO DESLIGADO NO ANO'
                GROUP BY cnpj_cei
                """
                st.session_state.analise_resultados['funcionarios'] = pd.read_sql(query_funcionarios, engine)
            except Exception as e:
                st.warning(f"Erro ao buscar funcionários: {e}")
                st.session_state.analise_resultados['funcionarios'] = pd.DataFrame()
            
            # 8. PAGAMENTOS
            try:
                status_text.text("8/10 - Verificando meios de pagamento...")
                progress_bar.progress(80)
                
                # Primeiro, buscar CPFs dos sócios ativos
                query_socios_cpf = f"""
                SELECT DISTINCT
                    nu_cnpj_princ as cnpj,
                    TRIM(nu_cnpj_cpf_secund) AS cpf_socio
                FROM usr_sat_ods.vw_cad_vinculo
                WHERE nu_cnpj_princ IN ('{cnpjs_str}')
                AND sn_relacao_ativa = 1
                AND nm_relacao != 'CONTABILISTA'
                AND LENGTH(TRIM(nu_cnpj_cpf_secund)) = 11
                """
                df_socios_cpf = pd.read_sql(query_socios_cpf, engine)
                
                # Pagamentos das empresas (CNPJ)
                query_pagamentos_cnpj = f"""
                SELECT 
                    ato_nu_cnpjmf as identificador,
                    'CNPJ' as tipo_identificador,
                    ato_dt_referencia as periodo,
                    SUM(ato_vl_credito + ato_vl_debito + ato_vl_pix) as valor_total
                FROM usr_sat_admcc.acc_r66_totalestab
                WHERE ato_nu_cnpjmf IN ('{cnpjs_str}')
                AND ato_dt_referencia BETWEEN 202501 AND 202509
                AND LENGTH(TRIM(ato_nu_cnpjmf)) = 14
                GROUP BY ato_nu_cnpjmf, ato_dt_referencia
                ORDER BY ato_nu_cnpjmf, ato_dt_referencia
                """
                df_pag_cnpj = pd.read_sql(query_pagamentos_cnpj, engine)
                
                # Pagamentos dos sócios (CPF)
                df_pag_cpf = pd.DataFrame()
                if not df_socios_cpf.empty:
                    cpfs_socios = "', '".join(df_socios_cpf['cpf_socio'].unique())
                    query_pagamentos_cpf = f"""
                    SELECT 
                        ato_nu_cnpjmf as identificador,
                        'CPF' as tipo_identificador,
                        ato_dt_referencia as periodo,
                        SUM(ato_vl_credito + ato_vl_debito + ato_vl_pix) as valor_total
                    FROM usr_sat_admcc.acc_r66_totalestab
                    WHERE ato_nu_cnpjmf IN ('{cpfs_socios}')
                    AND ato_dt_referencia BETWEEN 202501 AND 202509
                    AND LENGTH(TRIM(ato_nu_cnpjmf)) = 11
                    GROUP BY ato_nu_cnpjmf, ato_dt_referencia
                    ORDER BY ato_nu_cnpjmf, ato_dt_referencia
                    """
                    df_pag_cpf = pd.read_sql(query_pagamentos_cpf, engine)
                
                # Combinar ambos
                st.session_state.analise_resultados['pagamentos'] = pd.concat([df_pag_cnpj, df_pag_cpf], ignore_index=True)
                st.session_state.analise_resultados['socios_cpf'] = df_socios_cpf
                
            except Exception as e:
                st.warning(f"Erro ao buscar pagamentos: {e}")
                st.session_state.analise_resultados['pagamentos'] = pd.DataFrame()
                st.session_state.analise_resultados['socios_cpf'] = pd.DataFrame()
            
            # 9. INDÍCIOS
            try:
                status_text.text("9/10 - Verificando indícios fiscais...")
                progress_bar.progress(90)
                
                query_indicios = f"""
                SELECT
                    t.nu_cpf_cnpj as cnpj,
                    t.tx_descricao_indicio,
                    p.tx_descricao_complemento
                FROM neaf.empresa_indicio t, t.indicio_complemento p
                WHERE t.nu_cpf_cnpj IN ('{cnpjs_str}')
                AND t.cd_atual = 1
                """
                st.session_state.analise_resultados['indicios'] = pd.read_sql(query_indicios, engine)
            except Exception as e:
                st.warning(f"Erro ao buscar indícios: {e}")
                st.session_state.analise_resultados['indicios'] = pd.DataFrame()
            
            # 10. GRUPOS EXISTENTES
            try:
                status_text.text("10/10 - Verificando grupos existentes...")
                progress_bar.progress(100)
                
                query_grupos = f"""
                SELECT 
                    cnpj,
                    num_grupo
                FROM gessimples.gei_cnpj
                WHERE cnpj IN ('{cnpjs_str}')
                """
                st.session_state.analise_resultados['grupos_existentes'] = pd.read_sql(query_grupos, engine)
            except Exception as e:
                st.session_state.analise_resultados['grupos_existentes'] = pd.DataFrame()
            
            progress_bar.empty()
            status_text.empty()
    
    # EXIBIÇÃO DOS RESULTADOS (fora do botão)
    if st.session_state.analise_resultados is not None:
        
        resultados = st.session_state.analise_resultados
        cnpjs_validos = st.session_state.cnpjs_validos_analise
        
        st.success("✅ Análise concluída!")
        st.divider()
        
        # RESUMO EXECUTIVO
        st.header("📊 Resumo Executivo")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("CNPJs Analisados", len(cnpjs_validos))
        
        with col2:
            cadastrados = len(resultados['cadastro'])
            st.metric("Com Cadastro", cadastrados)
        
        with col3:
            com_indicios = len(resultados['indicios']) if not resultados['indicios'].empty else 0
            st.metric("Com Indícios", com_indicios)
        
        with col4:
            em_grupos = len(resultados['grupos_existentes']) if not resultados['grupos_existentes'].empty else 0
            st.metric("Já em Grupos GEI", em_grupos)
        
        # TABS PARA RESULTADOS DETALHADOS
        tabs = st.tabs([
            "Cadastro",
            "Sócios",
            "Faturamento (PGDAS/DIME)",
            "Notas Fiscais",
            "C115",
            "Contas (CCS)",
            "Funcionários",
            "Pagamentos",
            "Indícios",
            "Grupos Existentes"
        ])
        
        # TAB 1: CADASTRO
        with tabs[0]:
            if not resultados['cadastro'].empty:
                st.subheader(f"Dados Cadastrais ({len(resultados['cadastro'])} registros)")
                st.dataframe(resultados['cadastro'], width='stretch', hide_index=True)
            else:
                st.info("Nenhum dado cadastral encontrado.")
        
        # TAB 2: SÓCIOS
        with tabs[1]:
            if not resultados['socios'].empty:
                st.subheader(f"Vínculos Societários ({len(resultados['socios'])} vínculos)")
                
                if len(resultados['socios_compartilhados']) > 0:
                    st.warning(f"⚠️ {len(resultados['socios_compartilhados'])} sócios compartilhados encontrados!")
                    
                    st.write("**Sócios Compartilhados:**")
                    df_comp = pd.DataFrame({
                        'CPF': resultados['socios_compartilhados'].index,
                        'Qtd_CNPJs': resultados['socios_compartilhados'].values
                    })
                    st.dataframe(df_comp, width='stretch', hide_index=True)
                
                st.write("**Todos os Vínculos:**")
                st.dataframe(resultados['socios'], width='stretch', hide_index=True)
            else:
                st.info("Nenhum vínculo societário encontrado.")
        
        # TAB 3: FATURAMENTO (PGDAS + DIME)
        with tabs[2]:
            # Verificar se há dados em qualquer uma das fontes
            tem_pgdas = not resultados['pgdas'].empty
            tem_dime = not resultados.get('dime', pd.DataFrame()).empty
            tem_faturamento = not resultados.get('faturamento', pd.DataFrame()).empty

            if tem_pgdas or tem_dime or tem_faturamento:
                st.subheader("Faturamento Declarado (PGDAS / DIME)")

                # Métricas resumidas
                col_m1, col_m2, col_m3 = st.columns(3)
                with col_m1:
                    qtd_pgdas = len(resultados['pgdas']['cnpj'].unique()) if tem_pgdas else 0
                    st.metric("Empresas PGDAS (Simples)", qtd_pgdas)
                with col_m2:
                    qtd_dime = len(resultados.get('dime', pd.DataFrame())['cnpj'].unique()) if tem_dime else 0
                    st.metric("Empresas DIME (Normal)", qtd_dime)
                with col_m3:
                    st.metric("Total de Fontes", (1 if tem_pgdas else 0) + (1 if tem_dime else 0))

                # Sub-tabs para separar PGDAS, DIME e Consolidado
                sub_tabs = st.tabs(["Consolidado", "PGDAS (Simples Nacional)", "DIME (Regime Normal)"])

                # SUB-TAB: CONSOLIDADO
                with sub_tabs[0]:
                    if tem_faturamento:
                        df_fat = resultados['faturamento']
                        st.write("**Visão Consolidada - Receita por CNPJ e Fonte:**")

                        # Receita máxima por CNPJ consolidada
                        receita_consolidada = df_fat.groupby(['cnpj', 'fonte', 'regime'])['receita_12m'].max().reset_index()
                        receita_consolidada.columns = ['CNPJ', 'Fonte', 'Regime', 'Receita_Maxima_12m']
                        receita_consolidada['Acima_Limite_SN'] = receita_consolidada['Receita_Maxima_12m'] > 4800000
                        receita_consolidada['Receita_Maxima_12m_Fmt'] = receita_consolidada['Receita_Maxima_12m'].apply(formatar_moeda)

                        st.dataframe(receita_consolidada[['CNPJ', 'Fonte', 'Regime', 'Receita_Maxima_12m_Fmt', 'Acima_Limite_SN']],
                                   width='stretch', hide_index=True)

                        # Receita total do grupo (somando todas as empresas)
                        receita_total_grupo = receita_consolidada['Receita_Maxima_12m'].sum()
                        st.info(f"**Receita Total do Grupo (soma de todas as empresas):** {formatar_moeda(receita_total_grupo)}")

                        if receita_total_grupo > 4800000:
                            excesso = receita_total_grupo - 4800000
                            st.error(f"⚠️ **ALERTA:** Receita total do grupo ({formatar_moeda(receita_total_grupo)}) excede o limite do Simples Nacional em {formatar_moeda(excesso)}")

                        # Gráfico de evolução consolidada
                        fig_consolidado = px.line(
                            df_fat,
                            x='periodo',
                            y='receita_12m',
                            color='cnpj',
                            line_dash='fonte',
                            title="Evolução da Receita (12 meses) - Todas as Fontes",
                            labels={'receita_12m': 'Receita (R$)', 'periodo': 'Período', 'fonte': 'Fonte'},
                            template=filtros['tema']
                        )
                        fig_consolidado.add_hline(y=4800000, line_dash="dash", line_color="red",
                                                annotation_text="Limite SN (R$ 4,8M)")
                        st.plotly_chart(fig_consolidado, use_container_width=True)
                    else:
                        st.info("Dados consolidados não disponíveis.")

                # SUB-TAB: PGDAS
                with sub_tabs[1]:
                    if tem_pgdas:
                        st.write("**Receitas Declaradas via PGDAS (Simples Nacional):**")

                        receita_max_pgdas = resultados['pgdas'].groupby('cnpj')['receita_12m'].max().reset_index()
                        receita_max_pgdas.columns = ['CNPJ', 'Receita_Maxima_12m']
                        receita_max_pgdas['Acima_Limite_SN'] = receita_max_pgdas['Receita_Maxima_12m'] > 4800000
                        receita_max_pgdas['Receita_Maxima_12m'] = receita_max_pgdas['Receita_Maxima_12m'].apply(formatar_moeda)

                        st.dataframe(receita_max_pgdas, width='stretch', hide_index=True)

                        fig_pgdas = px.line(resultados['pgdas'], x='periodo', y='receita_12m', color='cnpj',
                                          title="Evolução da Receita PGDAS (12 meses)",
                                          template=filtros['tema'])
                        fig_pgdas.add_hline(y=4800000, line_dash="dash", line_color="red",
                                          annotation_text="Limite SN")
                        st.plotly_chart(fig_pgdas, use_container_width=True)

                        st.write("**Dados Completos PGDAS:**")
                        st.dataframe(resultados['pgdas'], width='stretch', hide_index=True)
                    else:
                        st.info("Nenhuma declaração PGDAS encontrada para os CNPJs informados.")

                # SUB-TAB: DIME
                with sub_tabs[2]:
                    if tem_dime:
                        df_dime = resultados['dime']
                        st.write("**Receitas Declaradas via DIME (Regime Normal):**")

                        receita_max_dime = df_dime.groupby('cnpj')['receita_12m'].max().reset_index()
                        receita_max_dime.columns = ['CNPJ', 'Faturamento_Maximo_12m']
                        receita_max_dime['Faturamento_Maximo_12m_Fmt'] = receita_max_dime['Faturamento_Maximo_12m'].apply(formatar_moeda)

                        st.dataframe(receita_max_dime[['CNPJ', 'Faturamento_Maximo_12m_Fmt']], width='stretch', hide_index=True)

                        # Métricas adicionais da DIME
                        col_d1, col_d2, col_d3 = st.columns(3)
                        with col_d1:
                            total_creditos = df_dime['total_creditos'].sum() if 'total_creditos' in df_dime.columns else 0
                            st.metric("Total Créditos ICMS", formatar_moeda(total_creditos))
                        with col_d2:
                            total_debitos = df_dime['total_debitos'].sum() if 'total_debitos' in df_dime.columns else 0
                            st.metric("Total Débitos ICMS", formatar_moeda(total_debitos))
                        with col_d3:
                            debito_recolher = df_dime['debito_recolher'].sum() if 'debito_recolher' in df_dime.columns else 0
                            st.metric("Débito a Recolher", formatar_moeda(debito_recolher))

                        fig_dime = px.line(df_dime, x='periodo', y='receita_12m', color='cnpj',
                                          title="Evolução do Faturamento DIME (12 meses)",
                                          template=filtros['tema'])
                        st.plotly_chart(fig_dime, use_container_width=True)

                        # Situação das declarações
                        if 'situacao_declaracao' in df_dime.columns:
                            st.write("**Situação das Declarações DIME:**")
                            situacao_resumo = df_dime.groupby(['cnpj', 'situacao_declaracao']).size().reset_index(name='qtd')
                            st.dataframe(situacao_resumo, width='stretch', hide_index=True)

                        st.write("**Dados Completos DIME:**")
                        st.dataframe(df_dime, width='stretch', hide_index=True)
                    else:
                        st.info("Nenhuma declaração DIME encontrada para os CNPJs informados.")
            else:
                st.info("Nenhuma declaração de faturamento encontrada (PGDAS ou DIME).")
        
        # TAB 4: NFE
        with tabs[3]:
            if not resultados['nfe'].empty:
                st.subheader(f"Notas Fiscais ({len(resultados['nfe'])} registros)")
                
                col1, col2 = st.columns(2)
                with col1:
                    emitidas = resultados['nfe']['nfe_cnpj_cpf_emit'].isin(cnpjs_validos).sum()
                    st.metric("Notas Emitidas", emitidas)
                with col2:
                    recebidas = resultados['nfe']['nfe_cnpj_cpf_dest'].isin(cnpjs_validos).sum()
                    st.metric("Notas Recebidas", recebidas)
                
                st.dataframe(resultados['nfe'].head(100), width='stretch', hide_index=True)
            else:
                st.info("Nenhuma nota fiscal encontrada para 2025.")
        
        # TAB 5: C115
        with tabs[4]:
            if not resultados['c115'].empty:
                st.subheader(f"Convênio 115 ({len(resultados['c115'])} registros)")
                
                identificadores = resultados['c115'].groupby('nu_identificador_tomador')['cnpj_tomador'].nunique()
                compartilhados = identificadores[identificadores > 1]
                
                if len(compartilhados) > 0:
                    st.warning(f"⚠️ {len(compartilhados)} identificadores compartilhados!")
                
                st.dataframe(resultados['c115'], width='stretch', hide_index=True)
            else:
                st.info("Nenhum dado C115 encontrado.")
        
        # TAB 6: CCS
        with tabs[5]:
            if not resultados['ccs'].empty:
                st.subheader(f"Contas Bancárias ({len(resultados['ccs'])} registros)")
                
                cpfs_contas = resultados['ccs'].groupby('nr_cpf')['cnpj'].nunique()
                cpfs_compartilhados = cpfs_contas[cpfs_contas > 1]
                
                if len(cpfs_compartilhados) > 0:
                    st.warning(f"⚠️ {len(cpfs_compartilhados)} CPFs com acesso a múltiplos CNPJs!")
                    
                    df_cpf = pd.DataFrame({
                        'CPF': cpfs_compartilhados.index,
                        'Qtd_CNPJs': cpfs_compartilhados.values
                    })
                    st.dataframe(df_cpf, width='stretch', hide_index=True)
                
                st.write("**Todas as Contas:**")
                st.dataframe(resultados['ccs'], width='stretch', hide_index=True)
            else:
                st.info("Nenhuma conta bancária encontrada.")
        
        # TAB 7: FUNCIONÁRIOS
        with tabs[6]:
            if not resultados['funcionarios'].empty:
                st.subheader("Funcionários (RAIS/CAGED)")
                st.dataframe(resultados['funcionarios'], width='stretch', hide_index=True)
            else:
                st.info("Nenhum funcionário encontrado.")
        
        # TAB 8: PAGAMENTOS
        with tabs[7]:
            if not resultados['pagamentos'].empty:
                st.subheader(f"Meios de Pagamento ({len(resultados['pagamentos'])} registros)")
                
                # Separar por tipo
                pag_cnpj = resultados['pagamentos'][resultados['pagamentos']['tipo_identificador'] == 'CNPJ']
                pag_cpf = resultados['pagamentos'][resultados['pagamentos']['tipo_identificador'] == 'CPF']
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Registros de CNPJ", len(pag_cnpj))
                with col2:
                    st.metric("Registros de Sócios (CPF)", len(pag_cpf))
                
                # Resumo por tipo
                st.write("**Total por Tipo:**")
                resumo_tipo = resultados['pagamentos'].groupby('tipo_identificador')['valor_total'].sum().reset_index()
                resumo_tipo.columns = ['Tipo', 'Valor_Total']
                resumo_tipo['Valor_Total'] = resumo_tipo['Valor_Total'].apply(formatar_moeda)
                st.dataframe(resumo_tipo, width='stretch', hide_index=True)
                
                # Resumo por identificador (CNPJ/CPF)
                st.write("**Total por Identificador:**")
                resumo_ident = resultados['pagamentos'].groupby(['identificador', 'tipo_identificador'])['valor_total'].sum().reset_index()
                resumo_ident.columns = ['Identificador', 'Tipo', 'Valor_Total']
                resumo_ident = resumo_ident.sort_values('Valor_Total', ascending=False)
                resumo_ident['Valor_Total'] = resumo_ident['Valor_Total'].apply(formatar_moeda)
                st.dataframe(resumo_ident, width='stretch', hide_index=True)
                
                # Gráfico de evolução
                st.write("**Evolução Temporal:**")
                fig = px.line(resultados['pagamentos'], 
                             x='periodo', 
                             y='valor_total', 
                             color='identificador',
                             line_dash='tipo_identificador',
                             title="Evolução dos Meios de Pagamento (Empresas e Sócios)",
                             labels={'valor_total': 'Valor (R$)', 'periodo': 'Período'},
                             template=filtros['tema'])
                st.plotly_chart(fig, use_container_width=True)
                
                # Dados completos
                with st.expander("Ver Dados Completos"):
                    st.dataframe(resultados['pagamentos'], width='stretch', hide_index=True)
                
            else:
                st.info("Nenhum dado de pagamento encontrado.")
        
        # TAB 9: INDÍCIOS
        with tabs[8]:
            if not resultados['indicios'].empty:
                st.subheader(f"Indícios Fiscais ({len(resultados['indicios'])} registros)")
                
                resumo_indicios = resultados['indicios']['tx_descricao_indicio'].value_counts().reset_index()
                resumo_indicios.columns = ['Tipo', 'Quantidade']
                
                col1, col2 = st.columns(2)
                with col1:
                    st.dataframe(resumo_indicios, width='stretch', hide_index=True)
                with col2:
                    fig = px.pie(resumo_indicios, values='Quantidade', names='Tipo',
                                title="Distribuição de Indícios",
                                template=filtros['tema'])
                    st.plotly_chart(fig)
                
                st.write("**Lista Completa:**")
                st.dataframe(resultados['indicios'], width='stretch', hide_index=True)
            else:
                st.success("✅ Nenhum indício fiscal encontrado.")
        
        # TAB 10: GRUPOS EXISTENTES
        with tabs[9]:
            if not resultados['grupos_existentes'].empty:
                st.warning(f"⚠️ {len(resultados['grupos_existentes'])} CNPJs já estão cadastrados em grupos GEI!")
                
                st.dataframe(resultados['grupos_existentes'], width='stretch', hide_index=True)
                
                for grupo in resultados['grupos_existentes']['num_grupo'].unique():
                    st.info(f"Ver detalhes do Grupo {grupo} na aba 'Dossiê do Grupo'")
            else:
                st.success("✅ Nenhum CNPJ está em grupos GEI existentes.")
        
        # CONCLUSÕES E RECOMENDAÇÕES
        st.divider()
        st.header("🎯 Conclusões e Recomendações")
        
        alertas = []
        
        if len(resultados['socios_compartilhados']) > 0:
            alertas.append(f"🔴 **CRÍTICO**: {len(resultados['socios_compartilhados'])} sócios compartilhados entre os CNPJs analisados, indicando possível grupo econômico.")
        
        # Verificar faturamento alto (PGDAS + DIME)
        cnpjs_faturamento_alto = set()
        if not resultados['pgdas'].empty:
            cnpjs_pgdas_alto = resultados['pgdas'][resultados['pgdas']['receita_12m'] > 4800000]['cnpj'].unique()
            cnpjs_faturamento_alto.update(cnpjs_pgdas_alto)
        if not resultados.get('dime', pd.DataFrame()).empty:
            cnpjs_dime_alto = resultados['dime'][resultados['dime']['receita_12m'] > 4800000]['cnpj'].unique()
            cnpjs_faturamento_alto.update(cnpjs_dime_alto)

        if len(cnpjs_faturamento_alto) > 0:
            alertas.append(f"🟡 **ATENÇÃO**: {len(cnpjs_faturamento_alto)} CNPJs com faturamento acima do limite do Simples Nacional (PGDAS/DIME).")
        
        if not resultados['ccs'].empty:
            cpfs_contas = resultados['ccs'].groupby('nr_cpf')['cnpj'].nunique()
            if (cpfs_contas > 1).any():
                alertas.append(f"🟡 **ATENÇÃO**: Contas bancárias com CPFs compartilhados entre CNPJs.")
        
        if not resultados['indicios'].empty:
            alertas.append(f"🔴 **CRÍTICO**: {len(resultados['indicios'])} indícios fiscais encontrados.")
        
        if not resultados['c115'].empty:
            identificadores = resultados['c115'].groupby('nu_identificador_tomador')['cnpj_tomador'].nunique()
            if (identificadores > 1).any():
                alertas.append(f"🟡 **ATENÇÃO**: Identificadores C115 compartilhados entre CNPJs.")
        
        if alertas:
            for alerta in alertas:
                st.markdown(alerta)
            
            st.markdown("---")
            st.info("💡 **Recomendação**: Considere criar um grupo GEI para estes CNPJs e monitorá-los continuamente.")
        else:
            st.success("✅ Nenhum alerta crítico identificado na análise.")

# =====================================================================
        # ANÁLISE DE SIMILARIDADE - EVIDÊNCIAS DE GRUPO ECONÔMICO
        # =====================================================================
        st.divider()
        st.header("🔍 Análise de Similaridade - Evidências de Grupo Econômico")
        
        st.info("""
        Esta análise verifica se os CNPJs compartilham informações que indicam
        formação de grupo econômico, conforme metodologia do Sistema GEI.
        """)
        
        evidencias = {}
        score_similaridade = 0
        max_score_possivel = 0
        
        # Criar abas para cada tipo de análise
        tabs_similaridade = st.tabs([
            "📋 Cadastro",
            "👥 Sócios",
            "📊 Receitas",
            "📄 Notas Fiscais",
            "📱 Convênio 115",
            "🏦 Contas Bancárias",
            "👔 Funcionários",
            "💳 Pagamentos",
            "📊 Score Final"
        ])
        
        # ===================================================================
        # TAB 1: ANÁLISE DE DADOS CADASTRAIS
        # ===================================================================
        with tabs_similaridade[0]:
            st.subheader("Consistência Cadastral")
            
            if not resultados['cadastro'].empty and len(resultados['cadastro']) > 1:
                cadastro_checks = []
                
                # Razão Social
                max_score_possivel += 2
                razoes = resultados['cadastro']['nm_razao_social'].dropna().unique()
                if len(razoes) == 1:
                    cadastro_checks.append({
                        'Atributo': 'Razão Social',
                        'Status': '✅ IDÊNTICA',
                        'Quantidade': '1',
                        'Pontos': 2,
                        'Avaliação': 'CRÍTICO - Forte indício'
                    })
                    evidencias['razao_social'] = True
                    score_similaridade += 2
                elif len(razoes) > 1:
                    cadastro_checks.append({
                        'Atributo': 'Razão Social',
                        'Status': '❌ DIFERENTES',
                        'Quantidade': str(len(razoes)),
                        'Pontos': 0,
                        'Avaliação': '-'
                    })
                
                # Nome Fantasia
                max_score_possivel += 1
                fantasias = resultados['cadastro']['nm_fantasia'].dropna().unique()
                if len(fantasias) == 1 and len(fantasias[0]) > 0:
                    cadastro_checks.append({
                        'Atributo': 'Nome Fantasia',
                        'Status': '✅ IDÊNTICO',
                        'Quantidade': '1',
                        'Pontos': 1,
                        'Avaliação': 'Alto indício'
                    })
                    evidencias['fantasia'] = True
                    score_similaridade += 1
                elif len(fantasias) > 1:
                    cadastro_checks.append({
                        'Atributo': 'Nome Fantasia',
                        'Status': '❌ DIFERENTES',
                        'Quantidade': str(len(fantasias)),
                        'Pontos': 0,
                        'Avaliação': '-'
                    })
                
                # CNAE
                max_score_possivel += 1
                cnaes = resultados['cadastro']['cd_cnae'].dropna().unique()
                if len(cnaes) == 1:
                    cadastro_checks.append({
                        'Atributo': 'CNAE',
                        'Status': '✅ IDÊNTICO',
                        'Quantidade': '1',
                        'Pontos': 1,
                        'Avaliação': 'Mesmo ramo'
                    })
                    evidencias['cnae'] = True
                    score_similaridade += 1
                elif len(cnaes) > 1:
                    cadastro_checks.append({
                        'Atributo': 'CNAE',
                        'Status': '❌ DIFERENTES',
                        'Quantidade': str(len(cnaes)),
                        'Pontos': 0,
                        'Avaliação': '-'
                    })
                
                # Contador
                max_score_possivel += 2
                contadores = resultados['cadastro']['nm_contador'].dropna().unique()
                if len(contadores) == 1 and len(contadores[0]) > 0:
                    cadastro_checks.append({
                        'Atributo': 'Contador',
                        'Status': '✅ MESMO',
                        'Quantidade': '1',
                        'Pontos': 2,
                        'Avaliação': 'CRÍTICO - Gestão comum'
                    })
                    evidencias['contador'] = True
                    score_similaridade += 2
                elif len(contadores) > 1:
                    cadastro_checks.append({
                        'Atributo': 'Contador',
                        'Status': '❌ DIFERENTES',
                        'Quantidade': str(len(contadores)),
                        'Pontos': 0,
                        'Avaliação': '-'
                    })
                
                # Endereço Completo
                max_score_possivel += 3
                enderecos = resultados['cadastro'].apply(
                    lambda row: f"{row.get('nm_logradouro', '')} {row.get('nu_logradouro', '')} {row.get('nm_bairro', '')} {row.get('municipio', '')}".strip(),
                    axis=1
                ).unique()
                if len(enderecos) == 1 and len(enderecos[0]) > 10:
                    cadastro_checks.append({
                        'Atributo': 'Endereço',
                        'Status': '✅ IDÊNTICO',
                        'Quantidade': '1',
                        'Pontos': 3,
                        'Avaliação': 'CRÍTICO - Mesmo local'
                    })
                    evidencias['endereco'] = True
                    score_similaridade += 3
                elif len(enderecos) > 1:
                    cadastro_checks.append({
                        'Atributo': 'Endereço',
                        'Status': '❌ DIFERENTES',
                        'Quantidade': str(len(enderecos)),
                        'Pontos': 0,
                        'Avaliação': '-'
                    })
                
                # Município
                max_score_possivel += 0.5
                municipios = resultados['cadastro']['municipio'].dropna().unique()
                if len(municipios) == 1:
                    cadastro_checks.append({
                        'Atributo': 'Município',
                        'Status': '✅ MESMO',
                        'Quantidade': '1',
                        'Pontos': 0.5,
                        'Avaliação': 'Indício leve'
                    })
                    score_similaridade += 0.5
                elif len(municipios) > 1:
                    cadastro_checks.append({
                        'Atributo': 'Município',
                        'Status': '❌ DIFERENTES',
                        'Quantidade': str(len(municipios)),
                        'Pontos': 0,
                        'Avaliação': '-'
                    })
                
                # Regime de Apuração
                max_score_possivel += 1
                regimes = resultados['cadastro']['nm_reg_apuracao'].dropna().unique()
                if len(regimes) == 1:
                    cadastro_checks.append({
                        'Atributo': 'Regime Tributário',
                        'Status': '✅ MESMO',
                        'Quantidade': str(regimes[0]),
                        'Pontos': 1,
                        'Avaliação': 'Mesmo regime'
                    })
                    score_similaridade += 1
                elif len(regimes) > 1:
                    cadastro_checks.append({
                        'Atributo': 'Regime Tributário',
                        'Status': '⚠️ MISTO',
                        'Quantidade': str(len(regimes)),
                        'Pontos': 0,
                        'Avaliação': 'Possível planejamento'
                    })
                
                df_cadastro = pd.DataFrame(cadastro_checks)
                st.dataframe(df_cadastro, width='stretch', hide_index=True)
                
                pontos_cadastro = df_cadastro['Pontos'].sum()
                if pontos_cadastro >= 5:
                    st.error(f"🔴 CRÍTICO: {pontos_cadastro:.1f} pontos - Forte evidência de grupo econômico")
                elif pontos_cadastro >= 3:
                    st.warning(f"🟡 ALTO: {pontos_cadastro:.1f} pontos - Evidência significativa")
                elif pontos_cadastro >= 1:
                    st.info(f"🟠 MODERADO: {pontos_cadastro:.1f} pontos")
                else:
                    st.success(f"🟢 BAIXO: {pontos_cadastro:.1f} pontos")
            else:
                st.warning("Dados cadastrais insuficientes para análise")
        
        # ===================================================================
        # TAB 2: ANÁLISE DE VÍNCULOS SOCIETÁRIOS
        # ===================================================================
        with tabs_similaridade[1]:
            st.subheader("Análise de Vínculos Societários")
            
            if not resultados['socios'].empty and len(cnpjs_validos) > 1:
                socios_checks = []
                
                # Sócios compartilhados
                max_score_possivel += 5
                socios_compartilhados = resultados.get('socios_compartilhados', pd.Series())
                
                # CALCULAR PRIMEIRO O PERCENTUAL DE INTERCONEXÃO E PARES
                total_cnpjs = len(cnpjs_validos)
                pares_com_socios = 0
                perc_interconexao = 0
                
                if total_cnpjs > 1 and len(socios_compartilhados) > 0:
                    pares_possiveis = (total_cnpjs * (total_cnpjs - 1)) / 2
                    
                    for cpf in socios_compartilhados.index:
                        cnpjs_do_socio = resultados['socios'][resultados['socios']['cpf_socio'] == cpf]['cnpj'].unique()
                        if len(cnpjs_do_socio) > 1:
                            pares_com_socios += len(cnpjs_do_socio) * (len(cnpjs_do_socio) - 1) / 2
                    
                    if pares_possiveis > 0:
                        perc_interconexao = (pares_com_socios / pares_possiveis) * 100
                
                # AGORA SIM ADICIONAR AOS CHECKS
                if len(socios_compartilhados) > 0:
                    pontos_socios = min(len(socios_compartilhados) * 2, 5)
                    
                    socios_checks.append({
                        'Indicador': 'Sócios Compartilhados',
                        'Quantidade': str(len(socios_compartilhados)),
                        'Status': '✅ DETECTADOS',
                        'Pontos': str(pontos_socios),
                        'Avaliação': 'CRÍTICO - Vínculos cruzados'
                    })
                    
                    socios_checks.append({
                        'Indicador': 'Índice de Interconexão',
                        'Quantidade': str(int(pares_com_socios)),
                        'Status': f'{perc_interconexao:.1f}%',
                        'Pontos': '-',
                        'Avaliação': 'Alto' if perc_interconexao > 50 else 'Moderado' if perc_interconexao > 20 else 'Baixo'
                    })
                    
                    evidencias['socios_compartilhados'] = True
                    score_similaridade += pontos_socios
                    
                    # Detalhar os sócios compartilhados
                    st.write("**Sócios que participam de múltiplos CNPJs:**")
                    for cpf, qtd in socios_compartilhados.items():
                        socios_info = resultados['socios'][resultados['socios']['cpf_socio'] == cpf]
                        st.write(f"• **CPF {cpf}**: Presente em {qtd} CNPJs")
                        for _, s in socios_info.iterrows():
                            st.write(f"  - {s['cnpj']}: {s.get('nm_qualificacao', 'N/A')}")
                else:
                    socios_checks.append({
                        'Indicador': 'Sócios Compartilhados',
                        'Quantidade': '0',
                        'Status': '❌ NÃO DETECTADO',
                        'Pontos': '0',
                        'Avaliação': '-'
                    })
                
                df_socios = pd.DataFrame(socios_checks)
                
                # Garantir que todas as colunas sejam string
                for col in df_socios.columns:
                    df_socios[col] = df_socios[col].astype(str)
                
                st.dataframe(df_socios, hide_index=True)
                
                # Calcular pontos apenas dos que não são '-'
                pontos_numericos = df_socios[df_socios['Pontos'] != '-']['Pontos'].astype(float)
                pontos_socios_total = pontos_numericos.sum() if len(pontos_numericos) > 0 else 0
                
                if pontos_socios_total >= 4:
                    st.error(f"🔴 CRÍTICO: {pontos_socios_total:.1f} pontos - Controle societário compartilhado")
                elif pontos_socios_total >= 2:
                    st.warning(f"🟡 ALTO: {pontos_socios_total:.1f} pontos")
                else:
                    st.info(f"🟢 BAIXO: {pontos_socios_total:.1f} pontos")
            else:
                st.warning("Dados de vínculos societários insuficientes")
        
        # ===================================================================
        # TAB 3: ANÁLISE DE RECEITAS (PGDAS + DIME)
        # ===================================================================
        with tabs_similaridade[2]:
            st.subheader("Análise de Faturamento - PGDAS / DIME")

            # Verificar disponibilidade de dados de faturamento
            tem_pgdas = not resultados['pgdas'].empty
            tem_dime = not resultados.get('dime', pd.DataFrame()).empty
            tem_faturamento = not resultados.get('faturamento', pd.DataFrame()).empty

            # Consolidar dados para análise
            if tem_faturamento:
                df_analise = resultados['faturamento'].copy()
            elif tem_pgdas or tem_dime:
                # Fallback: criar dataframe consolidado manualmente
                dados_consolidados = []
                if tem_pgdas:
                    for _, row in resultados['pgdas'].iterrows():
                        dados_consolidados.append({
                            'cnpj': str(row['cnpj']),
                            'periodo': row['periodo'],
                            'receita_12m': row['receita_12m'],
                            'fonte': 'PGDAS'
                        })
                if tem_dime:
                    for _, row in resultados['dime'].iterrows():
                        dados_consolidados.append({
                            'cnpj': str(row['cnpj']),
                            'periodo': row['periodo'],
                            'receita_12m': row['receita_12m'],
                            'fonte': 'DIME'
                        })
                df_analise = pd.DataFrame(dados_consolidados) if dados_consolidados else pd.DataFrame()
            else:
                df_analise = pd.DataFrame()

            if not df_analise.empty and len(cnpjs_validos) > 1:
                receitas_checks = []

                # Informação sobre fontes de dados
                fontes_disponiveis = df_analise['fonte'].unique().tolist() if 'fonte' in df_analise.columns else ['PGDAS']
                st.info(f"**Fontes de dados utilizadas:** {', '.join(fontes_disponiveis)}")

                # Mostrar contagem por fonte
                if 'fonte' in df_analise.columns:
                    col_f1, col_f2 = st.columns(2)
                    with col_f1:
                        cnpjs_pgdas = df_analise[df_analise['fonte'] == 'PGDAS']['cnpj'].nunique() if 'PGDAS' in fontes_disponiveis else 0
                        st.metric("CNPJs com PGDAS (Simples)", cnpjs_pgdas)
                    with col_f2:
                        cnpjs_dime = df_analise[df_analise['fonte'] == 'DIME']['cnpj'].nunique() if 'DIME' in fontes_disponiveis else 0
                        st.metric("CNPJs com DIME (Normal)", cnpjs_dime)

                # Receita somada ultrapassa limite
                max_score_possivel += 5
                receitas_por_cnpj = df_analise.groupby('cnpj')['receita_12m'].max()
                receita_total_grupo = receitas_por_cnpj.sum()
                receita_media = receitas_por_cnpj.mean()

                if receita_total_grupo > 4800000:
                    excesso = receita_total_grupo - 4800000
                    pontos_receita = 5
                    receitas_checks.append({
                        'Indicador': 'Receita Total do Grupo',
                        'Valor': formatar_moeda(receita_total_grupo),
                        'Status': '🔴 ACIMA DO LIMITE',
                        'Excesso': formatar_moeda(excesso),
                        'Pontos': str(pontos_receita),
                        'Avaliação': 'CRÍTICO - Fracionamento'
                    })
                    evidencias['receita_excesso'] = True
                    score_similaridade += pontos_receita

                    st.error(f"""
                    **🔴 ALERTA CRÍTICO - LIMITE ULTRAPASSADO**

                    Receita somada (PGDAS + DIME): **{formatar_moeda(receita_total_grupo)}**

                    Excesso: **{formatar_moeda(excesso)}** ({((excesso/4800000)*100):.1f}% acima do limite)

                    Este é um forte indício de fracionamento para manutenção artificial no Simples Nacional.
                    """)
                else:
                    receitas_checks.append({
                        'Indicador': 'Receita Total do Grupo',
                        'Valor': formatar_moeda(receita_total_grupo),
                        'Status': '✅ DENTRO DO LIMITE',
                        'Excesso': '-',
                        'Pontos': '0',
                        'Avaliação': '-'
                    })

                # Distribuição equilibrada (indício de divisão artificial)
                max_score_possivel += 2
                desvio_padrao = receitas_por_cnpj.std()
                coef_variacao = (desvio_padrao / receita_media) if receita_media > 0 else 0

                if coef_variacao < 0.3 and len(receitas_por_cnpj) > 1:
                    receitas_checks.append({
                        'Indicador': 'Distribuição de Receitas',
                        'Valor': f"CV: {coef_variacao:.2f}",
                        'Status': '⚠️ MUITO UNIFORME',
                        'Excesso': '-',
                        'Pontos': '2',
                        'Avaliação': 'Possível divisão planejada'
                    })
                    evidencias['receita_uniforme'] = True
                    score_similaridade += 2
                else:
                    receitas_checks.append({
                        'Indicador': 'Distribuição de Receitas',
                        'Valor': f"CV: {coef_variacao:.2f}",
                        'Status': '✅ VARIADA',
                        'Excesso': '-',
                        'Pontos': '0',
                        'Avaliação': '-'
                    })

                # Correlação temporal (evolução sincronizada)
                max_score_possivel += 3
                if len(cnpjs_validos) > 1:
                    pivot_receitas = df_analise.pivot_table(
                        index='periodo',
                        columns='cnpj',
                        values='receita_12m',
                        aggfunc='max'
                    )

                    if len(pivot_receitas) >= 3 and pivot_receitas.shape[1] > 1:
                        correlacoes = pivot_receitas.corr()
                        correlacao_media = correlacoes.values[np.triu_indices_from(correlacoes.values, k=1)].mean()

                        if correlacao_media > 0.7:
                            receitas_checks.append({
                                'Indicador': 'Correlação Temporal',
                                'Valor': f"{correlacao_media:.2f}",
                                'Status': '⚠️ ALTA CORRELAÇÃO',
                                'Excesso': '-',
                                'Pontos': '3',
                                'Avaliação': 'Operações sincronizadas'
                            })
                            evidencias['receita_correlacao'] = True
                            score_similaridade += 3
                        elif correlacao_media > 0.5:
                            receitas_checks.append({
                                'Indicador': 'Correlação Temporal',
                                'Valor': f"{correlacao_media:.2f}",
                                'Status': '⚠️ CORRELAÇÃO MODERADA',
                                'Excesso': '-',
                                'Pontos': '1.5',
                                'Avaliação': 'Possível coordenação'
                            })
                            score_similaridade += 1.5
                        else:
                            receitas_checks.append({
                                'Indicador': 'Correlação Temporal',
                                'Valor': f"{correlacao_media:.2f}",
                                'Status': '✅ BAIXA CORRELAÇÃO',
                                'Excesso': '-',
                                'Pontos': '0',
                                'Avaliação': '-'
                            })

                # Análise de regimes mistos (Simples + Normal)
                if 'fonte' in df_analise.columns:
                    fontes_por_cnpj = df_analise.groupby('cnpj')['fonte'].first()
                    if len(fontes_por_cnpj.unique()) > 1:
                        receitas_checks.append({
                            'Indicador': 'Regimes Tributários',
                            'Valor': f"{len(fontes_por_cnpj.unique())} regimes",
                            'Status': '⚠️ MISTO',
                            'Excesso': '-',
                            'Pontos': '1',
                            'Avaliação': 'Possível planejamento tributário'
                        })
                        score_similaridade += 1

                df_receitas = pd.DataFrame(receitas_checks)

                # Garantir que todas as colunas sejam string
                for col in df_receitas.columns:
                    df_receitas[col] = df_receitas[col].astype(str)

                st.dataframe(df_receitas, hide_index=True)

                # GRÁFICOS
                col1, col2 = st.columns(2)

                with col1:
                    st.write("**Distribuição de Receitas por CNPJ:**")

                    # Preparar dados para gráfico com cores por fonte
                    df_bar = df_analise.groupby(['cnpj', 'fonte'])['receita_12m'].max().reset_index()

                    fig1 = px.bar(
                        df_bar,
                        x='cnpj',
                        y='receita_12m',
                        color='fonte' if 'fonte' in df_bar.columns else None,
                        labels={'cnpj': 'CNPJ', 'receita_12m': 'Receita (R$)', 'fonte': 'Fonte'},
                        title="Receita Máxima por CNPJ e Fonte",
                        template=filtros['tema'],
                        barmode='group'
                    )
                    fig1.add_hline(y=4800000, line_dash="dash", line_color="red",
                                 annotation_text="Limite SN")
                    st.plotly_chart(fig1, use_container_width=True)

                with col2:
                    st.write("**Evolução da Receita Total do Grupo:**")

                    # Receita somada por período
                    receita_grupo_temporal = df_analise.groupby('periodo')['receita_12m'].sum().reset_index()
                    receita_grupo_temporal = receita_grupo_temporal.sort_values('periodo')

                    fig2 = px.line(
                        receita_grupo_temporal,
                        x='periodo',
                        y='receita_12m',
                        labels={'periodo': 'Período', 'receita_12m': 'Receita Total (R$)'},
                        title="Receita Total do Grupo ao Longo do Tempo",
                        template=filtros['tema'],
                        markers=True
                    )
                    fig2.add_hline(y=4800000, line_dash="dash", line_color="red",
                                 annotation_text="Limite SN", annotation_position="bottom right")

                    # Adicionar linha com valores
                    fig2.update_traces(
                        mode='lines+markers+text',
                        text=[formatar_moeda(v) for v in receita_grupo_temporal['receita_12m']],
                        textposition='top center',
                        textfont=dict(size=9)
                    )

                    st.plotly_chart(fig2, use_container_width=True)

                # Tabela detalhada da evolução temporal
                if len(pivot_receitas) > 0:
                    st.write("**Receitas Detalhadas por CNPJ e Período:**")

                    # Adicionar coluna de total
                    pivot_display = pivot_receitas.copy()
                    pivot_display['TOTAL GRUPO'] = pivot_display.sum(axis=1)

                    # Formatar valores
                    pivot_display = pivot_display.applymap(lambda x: formatar_moeda(x) if pd.notna(x) else '-')

                    st.dataframe(pivot_display)

                # Tabela resumo por fonte
                if 'fonte' in df_analise.columns:
                    st.write("**Resumo por Fonte de Dados:**")
                    resumo_fonte = df_analise.groupby('fonte').agg({
                        'cnpj': 'nunique',
                        'receita_12m': ['max', 'mean', 'sum']
                    }).round(2)
                    resumo_fonte.columns = ['Qtd CNPJs', 'Receita Máx', 'Receita Média', 'Receita Total']
                    for col in ['Receita Máx', 'Receita Média', 'Receita Total']:
                        resumo_fonte[col] = resumo_fonte[col].apply(formatar_moeda)
                    st.dataframe(resumo_fonte)

            else:
                st.warning("Dados de receitas insuficientes (PGDAS ou DIME)")
        
        # ===================================================================
        # TAB 4: ANÁLISE DE NOTAS FISCAIS
        # ===================================================================
        with tabs_similaridade[3]:
            st.subheader("Compartilhamento em Notas Fiscais")
            
            if not resultados['nfe'].empty and len(cnpjs_validos) > 1:
                nfe_checks = []
                
                # IPs de transmissão compartilhados
                max_score_possivel += 3
                if 'nfe_ip_transmissao' in resultados['nfe'].columns:
                    ips_por_cnpj = {}
                    for cnpj in cnpjs_validos:
                        ips_emit = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'] == cnpj]['nfe_ip_transmissao'].dropna().unique()
                        if len(ips_emit) > 0:
                            ips_por_cnpj[cnpj] = set(ips_emit)
                    
                    if len(ips_por_cnpj) > 1:
                        all_ips = set()
                        for ips in ips_por_cnpj.values():
                            all_ips.update(ips)
                        
                        ips_compartilhados = []
                        for ip in all_ips:
                            cnpjs_com_ip = [cnpj for cnpj, ips in ips_por_cnpj.items() if ip in ips]
                            if len(cnpjs_com_ip) > 1:
                                ips_compartilhados.append(ip)
                        
                        if len(ips_compartilhados) > 0:
                            pontos_ip = min(len(ips_compartilhados), 3)
                            nfe_checks.append({
                                'Indicador': 'IPs de Transmissão',
                                'Quantidade': len(ips_compartilhados),
                                'Status': '✅ COMPARTILHADOS',
                                'Pontos': pontos_ip,
                                'Avaliação': 'CRÍTICO - Mesma origem'
                            })
                            evidencias['ip_compartilhado'] = True
                            score_similaridade += pontos_ip
                            
                            st.write("**IPs Compartilhados:**")
                            for ip in ips_compartilhados[:5]:
                                cnpjs_ip = [cnpj for cnpj, ips in ips_por_cnpj.items() if ip in ips]
                                st.write(f"• {ip}: {len(cnpjs_ip)} CNPJs")
                        else:
                            nfe_checks.append({
                                'Indicador': 'IPs de Transmissão',
                                'Quantidade': 0,
                                'Status': '❌ NÃO COMPARTILHADOS',
                                'Pontos': 0,
                                'Avaliação': '-'
                            })
                
                # Clientes compartilhados
                max_score_possivel += 2
                emitentes = resultados['nfe']['nfe_cnpj_cpf_emit'].isin(cnpjs_validos)
                if emitentes.any():
                    clientes_por_cnpj = {}
                    for cnpj in cnpjs_validos:
                        clientes = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'] == cnpj]['nfe_cnpj_cpf_dest'].dropna().unique()
                        clientes_por_cnpj[cnpj] = set(clientes)
                    
                    if len(clientes_por_cnpj) > 1:
                        clientes_compartilhados = set.intersection(*clientes_por_cnpj.values())
                        
                        if len(clientes_compartilhados) > 0:
                            pontos_clientes = min(len(clientes_compartilhados) / 10, 2)
                            nfe_checks.append({
                                'Indicador': 'Clientes Comuns',
                                'Quantidade': len(clientes_compartilhados),
                                'Status': '✅ DETECTADOS',
                                'Pontos': pontos_clientes,
                                'Avaliação': 'Mesma base de clientes'
                            })
                            evidencias['clientes_comuns'] = True
                            score_similaridade += pontos_clientes
                        else:
                            nfe_checks.append({
                                'Indicador': 'Clientes Comuns',
                                'Quantidade': 0,
                                'Status': '❌ NÃO DETECTADOS',
                                'Pontos': 0,
                                'Avaliação': '-'
                            })
                
                # Fornecedores compartilhados
                max_score_possivel += 2
                destinatarios = resultados['nfe']['nfe_cnpj_cpf_dest'].isin(cnpjs_validos)
                if destinatarios.any():
                    fornecedores_por_cnpj = {}
                    for cnpj in cnpjs_validos:
                        fornecedores = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_dest'] == cnpj]['nfe_cnpj_cpf_emit'].dropna().unique()
                        fornecedores_por_cnpj[cnpj] = set(fornecedores)
                    
                    if len(fornecedores_por_cnpj) > 1:
                        fornecedores_compartilhados = set.intersection(*fornecedores_por_cnpj.values())
                        
                        if len(fornecedores_compartilhados) > 0:
                            pontos_fornec = min(len(fornecedores_compartilhados) / 10, 2)
                            nfe_checks.append({
                                'Indicador': 'Fornecedores Comuns',
                                'Quantidade': len(fornecedores_compartilhados),
                                'Status': '✅ DETECTADOS',
                                'Pontos': pontos_fornec,
                                'Avaliação': 'Mesma cadeia de suprimentos'
                            })
                            evidencias['fornecedores_comuns'] = True
                            score_similaridade += pontos_fornec
                        else:
                            nfe_checks.append({
                                'Indicador': 'Fornecedores Comuns',
                                'Quantidade': 0,
                                'Status': '❌ NÃO DETECTADOS',
                                'Pontos': 0,
                                'Avaliação': '-'
                            })
                
                # Códigos de produtos compartilhados
                max_score_possivel += 1
                if 'nfe_cd_produto' in resultados['nfe'].columns:
                    produtos_por_cnpj = {}
                    for cnpj in cnpjs_validos:
                        produtos = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'] == cnpj]['nfe_cd_produto'].dropna().unique()
                        produtos_por_cnpj[cnpj] = set(produtos)
                    
                    if len(produtos_por_cnpj) > 1:
                        produtos_compartilhados = set.intersection(*produtos_por_cnpj.values())
                        
                        if len(produtos_compartilhados) >= 5:
                            nfe_checks.append({
                                'Indicador': 'Códigos de Produto Comuns',
                                'Quantidade': len(produtos_compartilhados),
                                'Status': '✅ DETECTADOS',
                                'Pontos': 1,
                                'Avaliação': 'Mesmo catálogo'
                            })
                            evidencias['produtos_comuns'] = True
                            score_similaridade += 1
                        elif len(produtos_compartilhados) > 0:
                            nfe_checks.append({
                                'Indicador': 'Códigos de Produto Comuns',
                                'Quantidade': len(produtos_compartilhados),
                                'Status': '⚠️ POUCOS',
                                'Pontos': 0.5,
                                'Avaliação': 'Alguma sobreposição'
                            })
                            score_similaridade += 0.5
                
                # Descrição de produtos compartilhados
                max_score_possivel += 1
                if 'nfe_de_produto' in resultados['nfe'].columns:
                    desc_por_cnpj = {}
                    for cnpj in cnpjs_validos:
                        desc = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'] == cnpj]['nfe_de_produto'].dropna().unique()
                        desc_por_cnpj[cnpj] = set(desc)
                    
                    if len(desc_por_cnpj) > 1:
                        desc_compartilhadas = set.intersection(*desc_por_cnpj.values())
                        
                        if len(desc_compartilhadas) >= 5:
                            nfe_checks.append({
                                'Indicador': 'Descrições de Produto Comuns',
                                'Quantidade': len(desc_compartilhadas),
                                'Status': '✅ DETECTADOS',
                                'Pontos': 1,
                                'Avaliação': 'Mesmo portfólio'
                            })
                            evidencias['desc_produtos_comuns'] = True
                            score_similaridade += 1
                
                # Telefones do emitente compartilhados
                max_score_possivel += 2
                if 'nfe_emit_telefone' in resultados['nfe'].columns:
                    tel_emit_por_cnpj = {}
                    for cnpj in cnpjs_validos:
                        tels = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'] == cnpj]['nfe_emit_telefone'].dropna().unique()
                        if len(tels) > 0:
                            tel_emit_por_cnpj[cnpj] = set(tels)
                    
                    if len(tel_emit_por_cnpj) > 1:
                        all_tels = set()
                        for tels in tel_emit_por_cnpj.values():
                            all_tels.update(tels)
                        
                        tels_compartilhados = [tel for tel in all_tels if sum(1 for tels in tel_emit_por_cnpj.values() if tel in tels) > 1]
                        
                        if len(tels_compartilhados) > 0:
                            pontos_tel = min(len(tels_compartilhados), 2)
                            nfe_checks.append({
                                'Indicador': 'Telefones Emitente',
                                'Quantidade': len(tels_compartilhados),
                                'Status': '✅ COMPARTILHADOS',
                                'Pontos': pontos_tel,
                                'Avaliação': 'CRÍTICO - Mesmo contato'
                            })
                            evidencias['tel_emit_compartilhado'] = True
                            score_similaridade += pontos_tel
                
                # E-mails de destinatário compartilhados
                max_score_possivel += 1
                if 'nfe_dest_email' in resultados['nfe'].columns:
                    emails_por_cnpj = {}
                    for cnpj in cnpjs_validos:
                        emails = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'] == cnpj]['nfe_dest_email'].dropna().unique()
                        if len(emails) > 0:
                            emails_por_cnpj[cnpj] = set(emails)
                    
                    if len(emails_por_cnpj) > 1:
                        emails_compartilhados = set.intersection(*emails_por_cnpj.values())
                        
                        if len(emails_compartilhados) > 0:
                            nfe_checks.append({
                                'Indicador': 'E-mails Destinatário',
                                'Quantidade': len(emails_compartilhados),
                                'Status': '✅ COMPARTILHADOS',
                                'Pontos': 1,
                                'Avaliação': 'Mesmos contatos'
                            })
                            evidencias['email_dest_compartilhado'] = True
                            score_similaridade += 1
                
                # Endereços de emissão compartilhados
                max_score_possivel += 2
                if 'nfe_emit_end_completo' in resultados['nfe'].columns:
                    enderecos_emit = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_emit'].isin(cnpjs_validos)]['nfe_emit_end_completo'].dropna().unique()
                    if len(enderecos_emit) == 1 and len(enderecos_emit[0]) > 10:
                        nfe_checks.append({
                            'Indicador': 'Endereço de Emissão',
                            'Quantidade': 1,
                            'Status': '✅ MESMO ENDEREÇO',
                            'Pontos': 2,
                            'Avaliação': 'CRÍTICO - Mesmo local'
                        })
                        evidencias['endereco_nfe_emit'] = True
                        score_similaridade += 2
                    elif len(enderecos_emit) > 1:
                        nfe_checks.append({
                            'Indicador': 'Endereço de Emissão',
                            'Quantidade': len(enderecos_emit),
                            'Status': '❌ DIFERENTES',
                            'Pontos': 0,
                            'Avaliação': '-'
                        })
                
                # Endereços de destino compartilhados
                max_score_possivel += 2
                if 'nfe_dest_end_completo' in resultados['nfe'].columns:
                    enderecos_dest = resultados['nfe'][resultados['nfe']['nfe_cnpj_cpf_dest'].isin(cnpjs_validos)]['nfe_dest_end_completo'].dropna().unique()
                    if len(enderecos_dest) == 1 and len(enderecos_dest[0]) > 10:
                        nfe_checks.append({
                            'Indicador': 'Endereço de Destino',
                            'Quantidade': 1,
                            'Status': '✅ MESMO ENDEREÇO',
                            'Pontos': 2,
                            'Avaliação': 'CRÍTICO - Mesmo local'
                        })
                        evidencias['endereco_nfe_dest'] = True
                        score_similaridade += 2
                    elif len(enderecos_dest) > 1:
                        nfe_checks.append({
                            'Indicador': 'Endereço de Destino',
                            'Quantidade': len(enderecos_dest),
                            'Status': '❌ DIFERENTES',
                            'Pontos': 0,
                            'Avaliação': '-'
                        })
                
                if nfe_checks:
                    df_nfe = pd.DataFrame(nfe_checks)
                    st.dataframe(df_nfe, width='stretch', hide_index=True)
                    
                    pontos_nfe = df_nfe['Pontos'].sum()
                    if pontos_nfe >= 5:
                        st.error(f"🔴 CRÍTICO: {pontos_nfe:.1f} pontos - Operações fortemente interligadas")
                    elif pontos_nfe >= 3:
                        st.warning(f"🟡 ALTO: {pontos_nfe:.1f} pontos")
                    else:
                        st.info(f"🟢 MODERADO: {pontos_nfe:.1f} pontos")
            else:
                st.warning("Dados de notas fiscais insuficientes")
        
        # ===================================================================
        # TAB 5: ANÁLISE DE CONVÊNIO 115
        # ===================================================================
        with tabs_similaridade[4]:
            st.subheader("Análise Convênio 115 - Identificadores Compartilhados")
            
            if not resultados['c115'].empty and len(cnpjs_validos) > 1:
                c115_checks = []
                
                # Identificadores de tomador compartilhados
                max_score_possivel += 3
                identificadores = resultados['c115'].groupby('nu_identificador_tomador')['cnpj_tomador'].nunique()
                identificadores_compart = identificadores[identificadores > 1]
                
                if len(identificadores_compart) > 0:
                    pontos_id = min(len(identificadores_compart), 3)
                    c115_checks.append({
                        'Indicador': 'Identificadores Compartilhados',
                        'Quantidade': len(identificadores_compart),
                        'Status': '✅ DETECTADOS',
                        'Pontos': pontos_id,
                        'Avaliação': 'CRÍTICO - Mesmo identificador'
                    })
                    evidencias['c115_identificador'] = True
                    score_similaridade += pontos_id
                    
                    st.write("**Identificadores Compartilhados:**")
                    for identificador, qtd in identificadores_compart.head(10).items():
                        st.write(f"• {identificador}: {qtd} CNPJs")
                else:
                    c115_checks.append({
                        'Indicador': 'Identificadores Compartilhados',
                        'Quantidade': 0,
                        'Status': '❌ NÃO DETECTADOS',
                        'Pontos': 0,
                        'Avaliação': '-'
                    })
                
                # Telefones de contato compartilhados
                max_score_possivel += 2
                if 'nu_tel_contato' in resultados['c115'].columns:
                    telefones = resultados['c115'].groupby('nu_tel_contato')['cnpj_tomador'].nunique()
                    telefones_compart = telefones[telefones > 1]
                    
                    if len(telefones_compart) > 0:
                        pontos_tel = min(len(telefones_compart), 2)
                        c115_checks.append({
                            'Indicador': 'Telefones Compartilhados',
                            'Quantidade': len(telefones_compart),
                            'Status': '✅ DETECTADOS',
                            'Pontos': pontos_tel,
                            'Avaliação': 'Alto - Mesmo contato'
                        })
                        evidencias['c115_telefone'] = True
                        score_similaridade += pontos_tel
                    else:
                        c115_checks.append({
                            'Indicador': 'Telefones Compartilhados',
                            'Quantidade': 0,
                            'Status': '❌ NÃO DETECTADOS',
                            'Pontos': 0,
                            'Avaliação': '-'
                        })
                
                if c115_checks:
                    df_c115 = pd.DataFrame(c115_checks)
                    st.dataframe(df_c115, width='stretch', hide_index=True)
            else:
                st.warning("Dados do Convênio 115 insuficientes")
        
        # ===================================================================
        # TAB 6: ANÁLISE DE CONTAS BANCÁRIAS (CCS)
        # ===================================================================
        with tabs_similaridade[5]:
            st.subheader("Análise de Contas Bancárias - CCS")
            
            if not resultados['ccs'].empty and len(cnpjs_validos) > 1:
                ccs_checks = []
                
                # CPFs compartilhando acesso a contas
                max_score_possivel += 4
                cpfs_contas = resultados['ccs'].groupby('nr_cpf')['cnpj'].nunique()
                cpfs_compartilhados = cpfs_contas[cpfs_contas > 1]
                
                if len(cpfs_compartilhados) > 0:
                    pontos_cpf = min(len(cpfs_compartilhados) * 2, 4)
                    ccs_checks.append({
                        'Indicador': 'CPFs com Múltiplas Contas',
                        'Quantidade': len(cpfs_compartilhados),
                        'Status': '✅ DETECTADOS',
                        'Pontos': pontos_cpf,
                        'Avaliação': 'CRÍTICO - Gestão financeira comum'
                    })
                    evidencias['ccs_cpf_compartilhado'] = True
                    score_similaridade += pontos_cpf
                    
                    st.write("**CPFs com Acesso a Múltiplas Contas:**")
                    for cpf, qtd in cpfs_compartilhados.head(10).items():
                        st.write(f"• CPF {cpf}: Acesso a contas de {qtd} CNPJs")
                        contas_cpf = resultados['ccs'][resultados['ccs']['nr_cpf'] == cpf]
                        for _, conta in contas_cpf.iterrows():
                            st.write(f"  - {conta['cnpj']}: {conta.get('nm_banco', 'N/A')} - Ag: {conta.get('cd_agencia', 'N/A')}")
                else:
                    ccs_checks.append({
                        'Indicador': 'CPFs com Múltiplas Contas',
                        'Quantidade': 0,
                        'Status': '❌ NÃO DETECTADOS',
                        'Pontos': 0,
                        'Avaliação': '-'
                    })
                
                # Bancos e agências comuns
                max_score_possivel += 1
                bancos_agencias = resultados['ccs'].groupby(['nm_banco', 'cd_agencia'])['cnpj'].nunique()
                bancos_comuns = bancos_agencias[bancos_agencias > 1]
                
                if len(bancos_comuns) > 0:
                    ccs_checks.append({
                        'Indicador': 'Banco/Agência Comuns',
                        'Quantidade': len(bancos_comuns),
                        'Status': '✅ DETECTADOS',
                        'Pontos': 1,
                        'Avaliação': 'Mesma praça bancária'
                    })
                    score_similaridade += 1
                
                if ccs_checks:
                    df_ccs = pd.DataFrame(ccs_checks)
                    st.dataframe(df_ccs, width='stretch', hide_index=True)
            else:
                st.warning("Dados de contas bancárias insuficientes")
        
        # ===================================================================
        # TAB 7: ANÁLISE DE FUNCIONÁRIOS
        # ===================================================================
        with tabs_similaridade[6]:
            st.subheader("Análise de Funcionários - RAIS/CAGED")
            
            if not resultados['funcionarios'].empty:
                func_checks = []
                
                # Baixo número de funcionários vs receita
                max_score_possivel += 3
                for _, row in resultados['funcionarios'].iterrows():
                    cnpj = row['cnpj']
                    funcionarios = row['total_funcionarios']

                    # Buscar receita máxima do CNPJ (PGDAS ou DIME)
                    receita = None
                    fonte_receita = None

                    if not resultados['pgdas'].empty:
                        receita_pgdas = resultados['pgdas'][resultados['pgdas']['cnpj'] == cnpj]['receita_12m'].max()
                        if pd.notna(receita_pgdas) and receita_pgdas > 0:
                            receita = receita_pgdas
                            fonte_receita = 'PGDAS'

                    if not resultados.get('dime', pd.DataFrame()).empty:
                        receita_dime = resultados['dime'][resultados['dime']['cnpj'] == cnpj]['receita_12m'].max()
                        if pd.notna(receita_dime) and receita_dime > 0:
                            if receita is None or receita_dime > receita:
                                receita = receita_dime
                                fonte_receita = 'DIME'

                    if receita is not None and receita > 0:
                        receita_por_func = receita / (funcionarios + 1)  # +1 para evitar divisão por zero

                        if receita_por_func > 500000:  # R$ 500k por funcionário
                            func_checks.append({
                                'CNPJ': cnpj,
                                'Funcionários': int(funcionarios),
                                'Receita': formatar_moeda(receita),
                                'Fonte': fonte_receita,
                                'Receita/Func': formatar_moeda(receita_por_func),
                                'Status': '⚠️ DESPROPORCIONAL',
                                'Avaliação': 'Possível terceirização ou operação concentrada'
                            })
                            score_similaridade += 1
                
                # Total de funcionários do grupo
                total_funcionarios = resultados['funcionarios']['total_funcionarios'].sum()
                st.metric("Total de Funcionários no Grupo", int(total_funcionarios))
                
                if func_checks:
                    st.write("**Análise Receita vs Funcionários:**")
                    df_func = pd.DataFrame(func_checks)
                    st.dataframe(df_func, width='stretch', hide_index=True)
                else:
                    st.success("✅ Proporção receita/funcionários dentro do esperado")
            else:
                st.warning("Dados de funcionários insuficientes")
        
        # ===================================================================
        # TAB 8: ANÁLISE DE MEIOS DE PAGAMENTO
        # ===================================================================
        with tabs_similaridade[7]:
            st.subheader("Análise de Meios de Pagamento")
            
            if not resultados['pagamentos'].empty:
                pag_checks = []
                
                # Separar pagamentos por tipo
                pag_cnpj = resultados['pagamentos'][resultados['pagamentos']['tipo_identificador'] == 'CNPJ']
                pag_cpf = resultados['pagamentos'][resultados['pagamentos']['tipo_identificador'] == 'CPF']
                
                # Análise de valores das empresas
                max_score_possivel += 2
                if not pag_cnpj.empty:
                    valores_empresas = pag_cnpj.groupby('identificador')['valor_total'].sum()
                    
                    st.write("**Valores de Meios de Pagamento por CNPJ:**")
                    for cnpj, valor in valores_empresas.items():
                        st.write(f"• {cnpj}: {formatar_moeda(valor)}")
                
                # Análise de valores dos sócios
                if not pag_cpf.empty:
                    valores_socios = pag_cpf.groupby('identificador')['valor_total'].sum()
                    
                    st.write("**Valores de Meios de Pagamento dos Sócios (CPF):**")
                    
                    # Verificar se há sócios compartilhados com meios de pagamento
                    if not resultados.get('socios_cpf', pd.DataFrame()).empty:
                        cpfs_com_pagamento = set(valores_socios.index)
                        
                        # Ver quais CPFs estão em múltiplos CNPJs
                        cpfs_por_cnpj = resultados['socios_cpf'].groupby('cpf_socio')['cnpj'].nunique()
                        cpfs_compartilhados_com_pag = cpfs_por_cnpj[cpfs_por_cnpj > 1]
                        cpfs_compartilhados_com_pag = cpfs_compartilhados_com_pag[cpfs_compartilhados_com_pag.index.isin(cpfs_com_pagamento)]
                        
                        if len(cpfs_compartilhados_com_pag) > 0:
                            pontos_pag_socios = min(len(cpfs_compartilhados_com_pag), 2)
                            pag_checks.append({
                                'Indicador': 'Sócios com Meios Pagamento',
                                'Quantidade': len(cpfs_compartilhados_com_pag),
                                'Status': '✅ DETECTADOS',
                                'Pontos': pontos_pag_socios,
                                'Avaliação': 'Alto - Gestão financeira comum'
                            })
                            evidencias['socios_meios_pagamento'] = True
                            score_similaridade += pontos_pag_socios
                            
                            st.write(f"**⚠️ {len(cpfs_compartilhados_com_pag)} sócios compartilhados com meios de pagamento:**")
                            for cpf, qtd_cnpj in cpfs_compartilhados_com_pag.items():
                                valor_socio = valores_socios.get(cpf, 0)
                                st.write(f"• CPF {cpf}: Sócio de {qtd_cnpj} CNPJs - Pagamentos: {formatar_moeda(valor_socio)}")
                    
                    # Mostrar top sócios por valor
                    st.write("**Top 10 Sócios por Valor de Pagamentos:**")
                    top_socios = valores_socios.sort_values(ascending=False).head(10)
                    for cpf, valor in top_socios.items():
                        st.write(f"• CPF {cpf}: {formatar_moeda(valor)}")
                
                if pag_checks:
                    df_pag = pd.DataFrame(pag_checks)
                    st.dataframe(df_pag, width='stretch', hide_index=True)
                else:
                    st.info("Análise de meios de pagamento requer dados adicionais de sócios.")
            else:
                st.warning("Dados de meios de pagamento insuficientes")
        
        # ===================================================================
        # TAB 9: SCORE FINAL E CONCLUSÃO
        # ===================================================================
        with tabs_similaridade[8]:
            st.subheader("📊 Score Final de Similaridade")
            
            # Métricas principais
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Score Total", f"{score_similaridade:.1f}", 
                         help="Pontuação total baseada em todas as evidências")
            
            with col2:
                st.metric("Score Máximo Possível", f"{max_score_possivel:.1f}",
                         help="Pontuação máxima com base nos dados disponíveis")
            
            with col3:
                percentual = (score_similaridade / max_score_possivel * 100) if max_score_possivel > 0 else 0
                st.metric("Percentual", f"{percentual:.1f}%",
                         help="Percentual do score em relação ao máximo")
            
            with col4:
                total_evidencias = len([v for v in evidencias.values() if v])
                st.metric("Evidências", total_evidencias,
                         help="Número de evidências positivas encontradas")
            
            # Determinação do nível de risco
            st.divider()
            
            if score_similaridade >= 15:
                nivel_risco = "🔴 CRÍTICO"
                cor_risco = "error"
                conclusao = """
                **FORTE EVIDÊNCIA DE GRUPO ECONÔMICO**
                
                Os CNPJs analisados apresentam múltiplas e graves evidências de pertencerem ao mesmo 
                grupo econômico. As similaridades detectadas em dados cadastrais, vínculos societários, 
                padrões operacionais e indicadores fiscais sugerem fortemente operação coordenada e 
                gestão centralizada.
                
                **RECOMENDAÇÃO URGENTE:**
                - Criação imediata de grupo GEI para monitoramento integrado
                - Análise aprofundada de possível planejamento tributário abusivo
                - Verificação de fraude à lei (fracionamento artificial)
                - Intimação dos contribuintes para esclarecimentos
                - Considerar procedimento fiscal conjunto
                """
            elif score_similaridade >= 10:
                nivel_risco = "🟡 ALTO"
                cor_risco = "warning"
                conclusao = """
                **EVIDÊNCIA SIGNIFICATIVA DE GRUPO ECONÔMICO**
                
                Os CNPJs apresentam várias características compatíveis com grupo econômico. 
                As evidências encontradas justificam investigação mais aprofundada.
                
                **RECOMENDAÇÃO:**
                - Criação de grupo GEI para monitoramento
                - Análise complementar com dados adicionais
                - Solicitar documentação adicional aos contribuintes
                - Monitoramento reforçado nos próximos períodos
                - Verificar histórico de alterações cadastrais
                """
            elif score_similaridade >= 5:
                nivel_risco = "🟠 MODERADO"
                cor_risco = "info"
                conclusao = """
                **INDÍCIOS MODERADOS DE GRUPO ECONÔMICO**
                
                Alguns indícios sugerem possível vinculação entre os CNPJs, mas não são conclusivos.
                Recomenda-se monitoramento e coleta de evidências adicionais.
                
                **RECOMENDAÇÃO:**
                - Monitoramento periódico dos CNPJs
                - Atenção a novos indícios que possam surgir
                - Cruzamento com outras bases de dados
                - Acompanhar evolução das receitas
                """
            else:
                nivel_risco = "🟢 BAIXO"
                cor_risco = "success"
                conclusao = """
                **BAIXA EVIDÊNCIA DE GRUPO ECONÔMICO**
                
                Com base nos dados analisados, não foram encontradas evidências significativas de que 
                os CNPJs pertençam ao mesmo grupo econômico. As similaridades detectadas podem ser 
                coincidências ou características comuns do setor.
                
                **RECOMENDAÇÃO:**
                - Monitoramento de rotina conforme procedimentos padrão
                - Atenção caso surjam novos indícios futuramente
                """
            
            # Exibir nível de risco
            if cor_risco == "error":
                st.error(f"**Nível de Risco: {nivel_risco}**")
            elif cor_risco == "warning":
                st.warning(f"**Nível de Risco: {nivel_risco}**")
            elif cor_risco == "info":
                st.info(f"**Nível de Risco: {nivel_risco}**")
            else:
                st.success(f"**Nível de Risco: {nivel_risco}**")
            
            # Conclusão detalhada
            st.markdown("### 🎯 Conclusão da Análise")
            st.markdown(conclusao)
            
            # Tabela resumo de evidências
            if evidencias:
                st.markdown("### 📋 Resumo das Evidências Encontradas")
                
                categorias_evidencias = {
                    'Cadastrais': ['razao_social', 'fantasia', 'cnae', 'contador', 'endereco'],
                    'Societárias': ['socios_compartilhados'],
                    'Fiscais': ['receita_excesso', 'receita_uniforme', 'receita_correlacao'],
                    'Operacionais': ['ip_compartilhado', 'clientes_comuns', 'fornecedores_comuns', 'produtos_comuns', 'desc_produtos_comuns', 'tel_emit_compartilhado', 'email_dest_compartilhado', 'endereco_nfe_emit', 'endereco_nfe_dest'],
                    'C115': ['c115_identificador', 'c115_telefone'],
                    'Financeiras': ['ccs_cpf_compartilhado', 'socios_meios_pagamento']
                }
                
                resumo_evidencias = []
                for categoria, chaves in categorias_evidencias.items():
                    evidencias_categoria = [k for k in chaves if evidencias.get(k, False)]
                    if evidencias_categoria:
                        resumo_evidencias.append({
                            'Categoria': categoria,
                            'Quantidade': len(evidencias_categoria),
                            'Evidências': ', '.join([k.replace('_', ' ').title() for k in evidencias_categoria])
                        })
                
                if resumo_evidencias:
                    df_resumo = pd.DataFrame(resumo_evidencias)
                    st.dataframe(df_resumo, width='stretch', hide_index=True)
            
            # Gráfico de distribuição de pontos
            st.markdown("### 📈 Distribuição de Pontos por Categoria")
            
            categorias_pontos = {
                'Cadastro': sum([2 if evidencias.get('razao_social') else 0,
                                1 if evidencias.get('fantasia') else 0,
                                1 if evidencias.get('cnae') else 0,
                                2 if evidencias.get('contador') else 0,
                                3 if evidencias.get('endereco') else 0]),
                'Sócios': 5 if evidencias.get('socios_compartilhados') else 0,
                'Receitas': sum([5 if evidencias.get('receita_excesso') else 0,
                                2 if evidencias.get('receita_uniforme') else 0,
                                3 if evidencias.get('receita_correlacao') else 0]),
                'NFe': sum([3 if evidencias.get('ip_compartilhado') else 0,
                           2 if evidencias.get('clientes_comuns') else 0,
                           2 if evidencias.get('fornecedores_comuns') else 0,
                           1 if evidencias.get('produtos_comuns') else 0,
                           2 if evidencias.get('endereco_nfe') else 0]),
                'C115': sum([3 if evidencias.get('c115_identificador') else 0,
                            2 if evidencias.get('c115_telefone') else 0]),
                'CCS': 4 if evidencias.get('ccs_cpf_compartilhado') else 0
            }
            
            df_categorias = pd.DataFrame([
                {'Categoria': k, 'Pontos': v}
                for k, v in categorias_pontos.items() if v > 0
            ])
            
            if not df_categorias.empty:
                fig = px.bar(df_categorias, x='Categoria', y='Pontos',
                            title="Pontos por Categoria de Evidência",
                            template=filtros['tema'],
                            color='Pontos',
                            color_continuous_scale='Reds')
                st.plotly_chart(fig, use_container_width=True)
        
        # BOTÃO DE EXPORTAÇÃO
        st.divider()
        st.subheader("Exportação de Relatório")
        
        st.write("""
        Clique no botão abaixo para gerar um relatório em PDF com todas as informações 
        consolidadas desta análise pontual.
        """)
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col2:
            if st.button("📄 Gerar PDF da Análise", type="primary", width='stretch', key="gerar_pdf_analise"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    status_text.text("Iniciando geração do PDF...")
                    progress_bar.progress(10)
                    
                    status_text.text("Coletando dados da análise...")
                    progress_bar.progress(30)
                    
                    status_text.text("Organizando informações...")
                    progress_bar.progress(50)
                    
                    status_text.text("Gerando documento PDF...")
                    progress_bar.progress(70)
                    
                    pdf_buffer = gerar_pdf_analise_pontual(cnpjs_validos, resultados)
                    
                    progress_bar.progress(90)
                    status_text.text("Finalizando...")
                    
                    progress_bar.progress(100)
                    status_text.text("PDF gerado com sucesso!")
                    
                    st.success("PDF gerado com sucesso!")
                    
                    st.download_button(
                        label="⬇️ Download PDF",
                        data=pdf_buffer,
                        file_name=f"analise_pontual_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                        mime="application/pdf",
                        width='stretch',
                        key="download_pdf_analise_auto"
                    )
                    
                    import time
                    time.sleep(2)
                    progress_bar.empty()
                    status_text.empty()
                    
                except Exception as e:
                    st.error(f"Erro ao gerar PDF: {e}")
                    progress_bar.empty()
                    status_text.empty()
        
        st.divider()
        
        st.write("**O que inclui o relatório PDF:**")
        st.write("• Resumo executivo com métricas principais")
        st.write("• Alertas críticos identificados")
        st.write("• Dados cadastrais completos de todos os CNPJs")
        st.write("• Vínculos societários detalhados")
        st.write("• Análise de sócios compartilhados")
        st.write("• Receitas declaradas (PGDAS)")
        st.write("• Notas fiscais emitidas e recebidas")
        st.write("• Dados do Convênio 115")
        st.write("• Contas bancárias (CCS)")
        st.write("• Informações de funcionários")
        st.write("• Meios de pagamento")
        st.write("• Indícios fiscais identificados")
        st.write("• Verificação de grupos GEI existentes")
        st.write("• Conclusões e recomendações")

def dashboard_executivo(dados, filtros):
    """Dashboard executivo principal"""
    st.markdown("<h1 class='main-header'>Dashboard Executivo</h1>", unsafe_allow_html=True)
    
    df = aplicar_filtros(dados['percent'], filtros)
    
    if df.empty:
        st.warning("Nenhum dado encontrado.")
        return
    
    # Panorama Geral
    st.subheader("Panorama Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de Grupos", f"{len(df):,}")
    with col2:
        st.metric("Total de CNPJs", f"{int(df['qntd_cnpj'].sum()):,}")
    with col3:
        score_col = 'score_final_ccs' if 'score_final_ccs' in df.columns else 'score_final_avancado'
        st.metric("Score Médio", f"{df[score_col].mean():.2f}")
    with col4:
        score_col = 'score_final_ccs' if 'score_final_ccs' in df.columns else 'score_final_avancado'
        st.metric("Grupos Críticos", f"{len(df[df[score_col] >= 20]):,}")
    
    # Análises gráficas
    st.subheader("Análises")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        score_col = 'score_final_ccs' if 'score_final_ccs' in df.columns else 'score_final_avancado'
        fig = px.histogram(df, x=score_col, nbins=20, 
                          title="Distribuição de Scores", template=filtros['tema'])
        fig.update_layout(height=300)
        st.plotly_chart(fig)
    
    with col2:
        if 'nivel_risco_grupo_economico' in df.columns:
            dist = df['nivel_risco_grupo_economico'].value_counts()
            fig = px.pie(values=dist.values, names=dist.index, 
                        title="Distribuição C115", template=filtros['tema'])
            fig.update_layout(height=300)
            st.plotly_chart(fig)
    
    with col3:
        if not dados['contador'].empty:
            top = dados['contador'].head(10).sort_values('media', ascending=True)
            fig = px.bar(top, x='media', y='nm_contador', orientation='h',
                         title="Top 10 Contadores", template=filtros['tema'])
            fig.update_layout(height=300)
            st.plotly_chart(fig)
    
    # Top grupos críticos
    st.subheader("Top 15 Grupos Críticos")
    score_col = 'score_final_ccs' if 'score_final_ccs' in df.columns else 'score_final_avancado'
    df_top = df.nlargest(15, score_col).copy()
    
    if 'valor_max' in df_top.columns:
        df_top['Receita'] = df_top['valor_max'].apply(formatar_moeda)
    
    colunas = ['num_grupo', score_col, 'qntd_cnpj',
               'Receita', 'qtd_total_indicios', 'nivel_risco_grupo_economico']
    colunas_exist = [c for c in colunas if c in df_top.columns]

    st.dataframe(df_top[colunas_exist], width='stretch', hide_index=True)

    # =========================================================================
    # IMPACTO FISCAL - GRUPOS DE ALTO RISCO
    # =========================================================================
    st.divider()
    st.subheader("Impacto Fiscal - Grupos de Alto Risco")

    st.info("""
    Esta análise identifica grupos com **score alto** que potencialmente operam de forma fragmentada
    para permanecer no Simples Nacional, evitando a tributação do Regime Normal.
    """)

    # Definir threshold para "alto risco"
    col1, col2 = st.columns(2)
    with col1:
        score_threshold = st.slider(
            "Score mínimo para considerar alto risco:",
            min_value=10.0,
            max_value=50.0,
            value=20.0,
            step=1.0,
            key="score_threshold_impacto"
        )
    with col2:
        receita_min = st.slider(
            "Receita mínima (em milhões):",
            min_value=1.0,
            max_value=10.0,
            value=4.8,
            step=0.5,
            key="receita_threshold_impacto"
        ) * 1e6

    # Filtrar grupos de alto risco
    df_alto_risco = df[
        (df[score_col] >= score_threshold) &
        (df['valor_max'] >= receita_min)
    ].copy()

    if df_alto_risco.empty:
        st.warning("Nenhum grupo encontrado com os critérios selecionados.")
    else:
        # Métricas principais
        col1, col2, col3, col4 = st.columns(4)

        qtd_grupos_risco = len(df_alto_risco)
        qtd_cnpjs_risco = int(df_alto_risco['qntd_cnpj'].sum())
        soma_faturamento = df_alto_risco['valor_max'].sum()

        # Cálculo do impacto fiscal estimado - APENAS PARA EMPRESAS DO SIMPLES
        # Diferença entre Regime Normal e Simples Nacional (ICMS SC)
        # Normal: 17% | Simples: ~10% => Diferença: 7%
        DIFERENCA_ALIQUOTA = 0.07  # 7% de diferença

        # Calcular faturamento apenas de empresas no Simples Nacional
        if 'qntd_sn' in df_alto_risco.columns and 'qntd_normal' in df_alto_risco.columns:
            # Proporção de empresas no Simples por grupo
            total_cnpjs = df_alto_risco['qntd_sn'].fillna(0) + df_alto_risco['qntd_normal'].fillna(0)
            df_alto_risco['prop_simples'] = df_alto_risco['qntd_sn'].fillna(0) / total_cnpjs.replace(0, 1)
            # Faturamento estimado do Simples = valor_max * proporção de empresas no Simples
            df_alto_risco['faturamento_simples'] = df_alto_risco['valor_max'] * df_alto_risco['prop_simples']
            soma_faturamento_simples = df_alto_risco['faturamento_simples'].sum()
            qtd_cnpjs_simples = int(df_alto_risco['qntd_sn'].fillna(0).sum())
        else:
            # Se não tiver a coluna, assume todo faturamento é do Simples
            soma_faturamento_simples = soma_faturamento
            df_alto_risco['faturamento_simples'] = df_alto_risco['valor_max']
            qtd_cnpjs_simples = qtd_cnpjs_risco

        # Impacto calculado apenas sobre faturamento do Simples Nacional
        impacto_fiscal_estimado = soma_faturamento_simples * DIFERENCA_ALIQUOTA

        with col1:
            st.metric("Grupos de Alto Risco", f"{qtd_grupos_risco:,}")
        with col2:
            st.metric("CNPJs no Simples", f"{qtd_cnpjs_simples:,}")
        with col3:
            st.metric("Faturamento Simples", formatar_moeda(soma_faturamento_simples))
        with col4:
            st.metric("Impacto Fiscal Estimado", formatar_moeda(impacto_fiscal_estimado), delta="potencial não arrecadado")

        st.divider()

        # Detalhamento do cálculo
        st.write("**Metodologia do Cálculo de Impacto Fiscal:**")
        st.markdown(f"""
        - **Simples Nacional:** Alíquota média de **10%**
        - **Regime Normal:** ICMS de **17%** (SC)
        - **Diferença:** **7%** de tributo não recolhido
        - **Fórmula:** Faturamento do Simples × 7% = Impacto Estimado

        > **Nota:** O cálculo considera apenas o faturamento das empresas do Simples Nacional.
        > Empresas já no Regime Normal não são consideradas no impacto.
        """)

        st.divider()

        # Tabela detalhada dos grupos de alto risco
        st.write("**Grupos Identificados:**")

        df_display = df_alto_risco.copy()
        df_display['Faturamento'] = df_display['valor_max'].apply(formatar_moeda)
        df_display['Impacto_Estimado'] = (df_display['valor_max'] * DIFERENCA_ALIQUOTA).apply(formatar_moeda)
        df_display['Acima_Limite_SN'] = df_display['valor_max'].apply(lambda x: 'SIM' if x > 4800000 else 'NÃO')

        colunas_exibir = ['num_grupo', score_col, 'qntd_cnpj', 'Faturamento',
                         'Impacto_Estimado', 'Acima_Limite_SN']
        if 'nivel_risco_grupo_economico' in df_display.columns:
            colunas_exibir.append('nivel_risco_grupo_economico')
        if 'nivel_risco_ccs' in df_display.columns:
            colunas_exibir.append('nivel_risco_ccs')

        colunas_exibir = [c for c in colunas_exibir if c in df_display.columns]

        st.dataframe(
            df_display[colunas_exibir].sort_values(score_col, ascending=False),
            width='stretch',
            hide_index=True
        )

        # Gráfico de distribuição
        col1, col2 = st.columns(2)

        with col1:
            fig = px.histogram(
                df_alto_risco,
                x='valor_max',
                nbins=20,
                title="Distribuição de Faturamento - Grupos de Alto Risco",
                template=filtros['tema'],
                labels={'valor_max': 'Faturamento (R$)'}
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Top 10 por impacto
            df_top_impacto = df_alto_risco.nlargest(10, 'valor_max').copy()
            df_top_impacto['Impacto'] = df_top_impacto['valor_max'] * DIFERENCA_ALIQUOTA / 1e6

            fig = px.bar(
                df_top_impacto,
                x='num_grupo',
                y='Impacto',
                title="Top 10 Grupos por Impacto Fiscal (em milhões)",
                template=filtros['tema'],
                labels={'Impacto': 'Impacto Fiscal (R$ milhões)', 'num_grupo': 'Grupo'}
            )
            st.plotly_chart(fig, use_container_width=True)

        # Download
        csv = df_display[colunas_exibir].to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Grupos Alto Risco (CSV)",
            data=csv,
            file_name="grupos_alto_risco_impacto_fiscal.csv",
            mime="text/csv"
        )

def ranking_grupos(dados, filtros):
    """Página de ranking de grupos"""
    st.markdown("<h1 class='main-header'>Ranking de Grupos</h1>", unsafe_allow_html=True)
    
    df = aplicar_filtros(dados['percent'], filtros)
    
    if df.empty:
        st.warning("Nenhum dado encontrado.")
        return
    
    # Controles de paginação
    col1, col2 = st.columns(2)
    with col1:
        registros = st.selectbox("Registros/página", [10, 25, 50, 100], index=1)
    with col2:
        score_col = 'score_final_ccs' if 'score_final_ccs' in df.columns else 'score_final_avancado'
        ordenacao = st.selectbox("Ordenar por", [score_col, 'valor_max', 'qntd_cnpj'])
    
    df_sorted = df.sort_values(ordenacao, ascending=False).reset_index(drop=True)
    df_sorted.insert(0, 'Posição', range(1, len(df_sorted) + 1))
    
    total_pag = max(1, (len(df_sorted) - 1) // registros + 1)
    pag = st.number_input("Página", min_value=1, max_value=total_pag, value=1) - 1
    
    inicio = pag * registros
    fim = min(inicio + registros, len(df_sorted))
    
    df_pag = df_sorted.iloc[inicio:fim].copy()
    
    if 'valor_max' in df_pag.columns:
        df_pag['valor_max'] = df_pag['valor_max'].apply(formatar_moeda)
    
    st.dataframe(df_pag, width='stretch', hide_index=True)
    st.info(f"Mostrando {inicio+1} a {fim} de {len(df_sorted)}")

# ====================================================================================
# FUNÇÕES PARA O MENU CONTADORES - ADICIONAR APÓS AS OUTRAS FUNÇÕES DE CONSULTA
# ====================================================================================

def listar_contadores():
    """Lista todos os contadores disponíveis para seleção"""
    query = """
    SELECT DISTINCT 
        cod_contador,
        contador
    FROM schema.tabela_principal
    WHERE cod_contador IS NOT NULL
    ORDER BY contador
    """
    df = execute_query(query)
    return df

def obter_grupos_contador(cod_contador):
    """Obtém todos os grupos econômicos de um contador específico"""
    query = f"""
    SELECT DISTINCT
        num_grupo,
        cnpj,
        razao_social,
        nome_fantasia,
        endereco_completo,
        municipio,
        uf,
        cep,
        telefone,
        email,
        data_abertura,
        situacao_cadastral,
        cnae_principal,
        descricao_cnae,
        porte_empresa,
        score_geral,
        score_regularidade,
        score_movimentacao,
        score_consistencia
    FROM schema.tabela_principal
    WHERE cod_contador = {cod_contador}
    ORDER BY num_grupo, razao_social
    """
    df = execute_query(query)
    return df

def analisar_riscos_contador(cod_contador):
    """Gera insights sobre os riscos dos grupos de um contador"""
    
    # Query para análise de categorias de risco
    query_categorias = f"""
    SELECT 
        categoria_risco,
        COUNT(DISTINCT num_grupo) as qtd_grupos,
        ROUND(AVG(score_geral), 2) as score_medio,
        COUNT(*) as total_empresas
    FROM schema.tabela_principal
    WHERE cod_contador = {cod_contador}
    GROUP BY categoria_risco
    ORDER BY qtd_grupos DESC
    """
    
    # Query para análise de CNAEs mais comuns
    query_cnaes = f"""
    SELECT 
        cnae_principal,
        descricao_cnae,
        COUNT(DISTINCT num_grupo) as qtd_grupos,
        COUNT(*) as qtd_empresas,
        ROUND(AVG(score_geral), 2) as score_medio
    FROM schema.tabela_principal
    WHERE cod_contador = {cod_contador}
    GROUP BY cnae_principal, descricao_cnae
    ORDER BY qtd_grupos DESC
    LIMIT 10
    """
    
    # Query para análise de práticas recorrentes
    query_praticas = f"""
    SELECT 
        pratica_identificada,
        COUNT(DISTINCT num_grupo) as qtd_grupos,
        ROUND(AVG(score_geral), 2) as score_medio,
        COUNT(*) as total_ocorrencias
    FROM schema.tabela_praticas
    WHERE cod_contador = {cod_contador}
    GROUP BY pratica_identificada
    ORDER BY qtd_grupos DESC
    LIMIT 10
    """
    
    df_categorias = execute_query(query_categorias)
    df_cnaes = execute_query(query_cnaes)
    df_praticas = execute_query(query_praticas)
    
    return df_categorias, df_cnaes, df_praticas

def get_grupos_por_contador(engine, nm_contador):
    """
    Obtém os grupos econômicos vinculados a um contador específico.
    """
    query = """
    SELECT DISTINCT 
        g.num_grupo, 
        g.cnpj, 
        c.nm_razao_social, 
        c.nm_fantasia, 
        CONCAT_WS(', ', c.nm_logradouro, c.nu_logradouro, c.tx_complemento, c.nm_bairro) as endereco,
        c.cd_cnae, 
        c.nm_munic as nm_municipio, 
        c.cd_uf as sg_uf, 
        c.cd_cep as nu_cep,
        c.cd_sit_cadastral, 
        c.dt_sit_cadastral, 
        p.score_final_ccs, 
        p.score_final_avancado, 
        p.score_final_completo, 
        p.nivel_risco_ccs, 
        p.indice_interconexao, 
        p.indice_risco_indicios, 
        p.indice_risco_pagamentos, 
        p.indice_risco_fat_func, 
        p.indice_risco_ccs, 
        p.qntd_cnpj, 
        p.valor_max, 
        p.total_funcionarios 
    FROM gessimples.gei_cnpj g 
    JOIN gessimples.gei_cadastro c ON g.cnpj = c.nu_cnpj 
    LEFT JOIN gessimples.gei_percent p ON CAST(g.num_grupo AS STRING) = CAST(p.num_grupo AS STRING) 
    WHERE c.nm_contador = '{}'
    ORDER BY p.score_final_ccs DESC NULLS LAST
    """.format(nm_contador)
    
    return pd.read_sql(query, engine)

def analisar_riscos_contador(engine, nm_contador):
    """
    Analisa os riscos e padrões dos grupos econômicos de um contador.
    """
    query = """
    WITH grupos_contador AS (
        SELECT DISTINCT 
            g.num_grupo, 
            g.cnpj, 
            c.cd_cnae,
            SUBSTR(CAST(c.cd_cnae AS STRING), 1, 2) AS secao_cnae,
            p.score_final_ccs, 
            p.nivel_risco_ccs, 
            p.indice_interconexao, 
            p.indice_risco_indicios, 
            p.indice_risco_ccs, 
            p.qntd_cnpj
        FROM gessimples.gei_cnpj g 
        JOIN gessimples.gei_cadastro c ON g.cnpj = c.nu_cnpj 
        LEFT JOIN gessimples.gei_percent p ON CAST(g.num_grupo AS STRING) = CAST(p.num_grupo AS STRING) 
        WHERE c.nm_contador = '{}'
    ),
    metricas_cnae AS (
        SELECT 
            secao_cnae, 
            COUNT(DISTINCT num_grupo) AS qtd_grupos, 
            ROUND(AVG(score_final_ccs), 2) AS media_score, 
            ROUND(AVG(indice_risco_ccs), 2) AS media_risco_ccs, 
            SUM(qntd_cnpj) AS total_cnpjs
        FROM grupos_contador 
        WHERE secao_cnae IS NOT NULL 
        GROUP BY secao_cnae
    ),
    praticas_concorrentes AS (
        SELECT 
            secao_cnae, 
            COUNT(DISTINCT num_grupo) AS grupos_mesma_secao, 
            ROUND(AVG(indice_interconexao), 4) AS media_interconexao
        FROM grupos_contador 
        WHERE secao_cnae IS NOT NULL 
        GROUP BY secao_cnae 
        HAVING COUNT(DISTINCT num_grupo) > 1
    )
    SELECT 
        m.*, 
        pc.grupos_mesma_secao, 
        pc.media_interconexao
    FROM metricas_cnae m 
    LEFT JOIN praticas_concorrentes pc ON m.secao_cnae = pc.secao_cnae 
    ORDER BY m.media_score DESC
    """.format(nm_contador)
    
    return pd.read_sql(query, engine)

def get_distribuicao_niveis_risco(engine, nm_contador):
    """Retorna distribuição dos níveis de risco CCS dos grupos do contador"""
    query = f"""
    SELECT 
        COALESCE(p.nivel_risco_ccs, 'SEM DADOS') AS nivel_risco,
        COUNT(DISTINCT g.num_grupo) AS qtd_grupos,
        ROUND(AVG(p.score_final_ccs), 2) AS score_medio
    FROM gessimples.gei_cnpj g
    JOIN gessimples.gei_cadastro c ON g.cnpj = c.nu_cnpj
    LEFT JOIN gessimples.gei_percent p ON CAST(g.num_grupo AS STRING) = CAST(p.num_grupo AS STRING)
    WHERE c.nm_contador = '{nm_contador}'
    GROUP BY p.nivel_risco_ccs
    ORDER BY score_medio DESC NULLS LAST
    """
    return pd.read_sql(query, engine)

def renderizar_detalhe_contador(engine, nm_contador, nm_gerfe, filtros):
    """Renderiza a página detalhada de um contador específico"""
    st.markdown(f"<h1 class='main-header'>📊 Análise Detalhada: {nm_contador}</h1>", unsafe_allow_html=True)
    st.caption(f"Unidade Fiscal: {nm_gerfe}")
    
    # Buscar dados
    with st.spinner("Carregando grupos econômicos..."):
        df_grupos = get_grupos_por_contador(engine, nm_contador)
        df_insights = analisar_riscos_contador(engine, nm_contador)
        df_niveis = get_distribuicao_niveis_risco(engine, nm_contador)
    
    if df_grupos.empty:
        st.warning("Nenhum grupo econômico encontrado para este contador.")
        return
    
    # Métricas gerais
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de Grupos", len(df_grupos['num_grupo'].unique()))
    with col2:
        st.metric("Score Médio CCS", f"{df_grupos['score_final_ccs'].mean():.2f}")
    with col3:
        st.metric("Total de CNPJs", int(df_grupos['qntd_cnpj'].sum()))
    with col4:
        alto_risco = len(df_grupos[df_grupos['nivel_risco_ccs'].isin(['CRÍTICO', 'ALTO'])])
        st.metric("Grupos Alto Risco", alto_risco)
    
    # Distribuição de níveis de risco
    st.subheader("📈 Distribuição de Níveis de Risco")
    if not df_niveis.empty:
        col1, col2 = st.columns([2, 1])
        with col1:
            fig_niveis = px.bar(df_niveis, 
                               x='nivel_risco', 
                               y='qtd_grupos',
                               title='Grupos por Nível de Risco CCS',
                               color='score_medio',
                               template=filtros['tema'],
                               color_continuous_scale='RdYlGn_r')
            st.plotly_chart(fig_niveis, use_container_width=True)
        with col2:
            st.dataframe(df_niveis, use_container_width=True, hide_index=True)
    
    # Insights por categoria
    st.subheader("🎯 Insights por Categoria (CNAE)")
    if not df_insights.empty:
        # Adicionar descrições de seção CNAE
        secoes_cnae = {
            '01': 'Agricultura', '02': 'Pecuária', '03': 'Pesca',
            '05': 'Mineração', '10': 'Alimentos', '11': 'Bebidas',
            '13': 'Têxtil', '14': 'Vestuário', '15': 'Couro',
            '16': 'Madeira', '17': 'Papel', '18': 'Impressão',
            '19': 'Petróleo', '20': 'Química', '21': 'Farmacêutica',
            '22': 'Borracha', '23': 'Minerais', '24': 'Metalurgia',
            '25': 'Metal', '26': 'Eletrônicos', '27': 'Elétricos',
            '28': 'Máquinas', '29': 'Veículos', '30': 'Transporte',
            '31': 'Móveis', '32': 'Produtos Diversos', '33': 'Manutenção',
            '35': 'Energia', '36': 'Água', '37': 'Esgoto',
            '41': 'Construção', '42': 'Construção', '43': 'Construção',
            '45': 'Comércio Veículos', '46': 'Comércio Atacado', '47': 'Comércio Varejo',
            '49': 'Transporte', '50': 'Transporte', '51': 'Transporte',
            '52': 'Armazenamento', '53': 'Correio', '55': 'Alojamento',
            '56': 'Alimentação', '58': 'Edição', '59': 'Cinema',
            '60': 'TV', '61': 'Telecomunicações', '62': 'TI',
            '63': 'Informação', '64': 'Financeiro', '65': 'Seguros',
            '66': 'Financeiro', '68': 'Imobiliário', '69': 'Jurídico',
            '70': 'Consultoria', '71': 'Arquitetura', '72': 'Pesquisa',
            '73': 'Publicidade', '74': 'Design', '75': 'Veterinário',
            '77': 'Aluguel', '78': 'Emprego', '79': 'Turismo',
            '80': 'Segurança', '81': 'Limpeza', '82': 'Administrativo',
            '84': 'Público', '85': 'Educação', '86': 'Saúde',
            '87': 'Social', '88': 'Social', '90': 'Artes',
            '91': 'Cultura', '92': 'Jogos', '93': 'Esportes',
            '94': 'Organizações', '95': 'Reparação', '96': 'Serviços',
            '97': 'Domésticos', '99': 'Organismos'
        }
        
        df_insights['descricao_cnae'] = df_insights['secao_cnae'].map(secoes_cnae)
        df_insights['categoria'] = df_insights['secao_cnae'] + ' - ' + df_insights['descricao_cnae'].fillna('Outros')
        
        # Categorias com maior risco
        st.markdown("**🔴 Categorias com Maior Risco Médio:**")
        top_riscos = df_insights.nlargest(5, 'media_score')[['categoria', 'qtd_grupos', 'media_score', 'media_risco_ccs', 'total_cnpjs']]
        top_riscos.columns = ['Categoria', 'Qtd Grupos', 'Score Médio', 'Risco CCS Médio', 'Total CNPJs']
        st.dataframe(top_riscos, use_container_width=True, hide_index=True)
        
        # Práticas concorrentes
        st.markdown("**⚠️ Categorias com Maior Concorrência (Grupos na mesma seção CNAE):**")
        concorrentes = df_insights[df_insights['grupos_mesma_secao'].notna()].nlargest(5, 'grupos_mesma_secao')
        if not concorrentes.empty:
            concorrentes_view = concorrentes[['categoria', 'grupos_mesma_secao', 'media_interconexao', 'media_score']]
            concorrentes_view.columns = ['Categoria', 'Grupos Concorrentes', 'Índice Interconexão', 'Score Médio']
            st.dataframe(concorrentes_view, use_container_width=True, hide_index=True)
            
            st.info(f"""
            💡 **Insight:** O contador possui {int(concorrentes['grupos_mesma_secao'].max())} grupos atuando 
            na mesma categoria ({concorrentes.iloc[0]['categoria']}), com índice de interconexão de 
            {concorrentes.iloc[0]['media_interconexao']:.4f}. Isso pode indicar especialização ou 
            possível relacionamento entre empresas concorrentes.
            """)
    
    # Tabela principal de grupos
    st.subheader("📋 Grupos Econômicos")
    
    # Filtros
    col1, col2, col3 = st.columns(3)
    with col1:
        filtro_risco = st.multiselect(
            "Filtrar por Nível de Risco CCS",
            options=df_grupos['nivel_risco_ccs'].unique(),
            default=None
        )
    with col2:
        score_min = st.number_input("Score CCS Mínimo", 
                                     min_value=0.0, 
                                     max_value=float(df_grupos['score_final_ccs'].max()),
                                     value=0.0)
    with col3:
        filtro_cnae = st.text_input("Filtrar por CNAE (primeiros dígitos)")
    
    # Aplicar filtros
    df_filtrado = df_grupos.copy()
    if filtro_risco:
        df_filtrado = df_filtrado[df_filtrado['nivel_risco_ccs'].isin(filtro_risco)]
    if score_min > 0:
        df_filtrado = df_filtrado[df_filtrado['score_final_ccs'] >= score_min]
    if filtro_cnae:
        df_filtrado = df_filtrado[df_filtrado['cd_cnae'].str.startswith(filtro_cnae, na=False)]
    
    # Preparar dataframe para exibição
    df_display = df_filtrado[[
        'num_grupo', 'cnpj', 'nm_razao_social', 'nm_fantasia', 
        'endereco', 'cd_cnae', 'nm_municipio', 'sg_uf',
        'score_final_ccs', 'nivel_risco_ccs', 'qntd_cnpj',
        'indice_risco_ccs', 'valor_max'
    ]].copy()
    
    df_display.columns = [
        'Grupo', 'CNPJ', 'Razão Social', 'Nome Fantasia',
        'Endereço', 'CNAE', 'Município', 'UF',
        'Score CCS', 'Nível Risco', 'Qtd CNPJs',
        'Índice CCS', 'Faturamento Máx'
    ]
    
    # Formatar valores
    df_display['Score CCS'] = df_display['Score CCS'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "-")
    df_display['Índice CCS'] = df_display['Índice CCS'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "-")
    df_display['Faturamento Máx'] = df_display['Faturamento Máx'].apply(
        lambda x: f"R$ {x:,.2f}" if pd.notna(x) else "-"
    )
    
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    # Download
    csv = df_display.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 Baixar dados em CSV",
        data=csv,
        file_name=f"grupos_{nm_contador.replace(' ', '_')}.csv",
        mime="text/csv"
    )

def menu_contadores(engine, dados, filtros):
    """Análise de contadores"""
    
    # Verificar se há um contador selecionado na sessão
    if 'contador_selecionado' not in st.session_state:
        st.session_state.contador_selecionado = None
    
    # Botão para voltar (se estiver em detalhe)
    if st.session_state.contador_selecionado:
        if st.button("⬅️ Voltar para lista de contadores"):
            st.session_state.contador_selecionado = None
            st.rerun()
        
        # Renderizar detalhe do contador
        contador_info = st.session_state.contador_selecionado
        renderizar_detalhe_contador(engine, contador_info['nm_contador'], contador_info['nm_gerfe'], filtros)
        return
    
    # Lista de contadores (código original mantido)
    st.markdown("<h1 class='main-header'>Análise de Contadores</h1>", unsafe_allow_html=True)
    
    if dados['contador'].empty:
        st.warning("Nenhum dado de contador disponível.")
        return
    
    df_cont = dados['contador'].copy()
    
    # Panorama Geral
    st.subheader("Panorama Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Contadores", len(df_cont))
    with col2:
        st.metric("Média Score", f"{df_cont['media'].mean():.2f}")
    with col3:
        st.metric("Total Grupos", int(df_cont['qntd_grupos'].sum()))
    with col4:
        st.metric("GERFEs Distintas", df_cont['nm_gerfe'].nunique())
    
    # Top Contadores por Risco
    st.subheader("Top 20 Contadores por Score Médio")
    top_20 = df_cont.nlargest(20, 'media')
    
    fig = px.bar(top_20, x='media', y='nm_contador', orientation='h',
                title="Score Médio dos Grupos por Contador",
                template=filtros['tema'],
                hover_data=['qntd_grupos', 'nm_gerfe'])
    st.plotly_chart(fig, use_container_width=True)
    
    # Tabela Detalhada com botões
    st.subheader("Detalhamento Completo")
    
    # Filtros
    col1, col2, col3 = st.columns(3)
    with col1:
        min_grupos = st.number_input("Mínimo de Grupos", min_value=1, value=3)
    with col2:
        min_score = st.number_input("Score Médio Mínimo", min_value=0.0, value=5.0, step=0.5)
    with col3:
        search_contador = st.text_input("Buscar contador")
    
    df_filtrado = df_cont[
        (df_cont['qntd_grupos'] >= min_grupos) &
        (df_cont['media'] >= min_score)
    ].sort_values('media', ascending=False)
    
    if search_contador:
        df_filtrado = df_filtrado[
            df_filtrado['nm_contador'].str.contains(search_contador, case=False, na=False)
        ]
    
    # Exibir com botões de ação
    st.write(f"**{len(df_filtrado)} contadores encontrados**")
    
    for idx, row in df_filtrado.iterrows():
        col1, col2, col3, col4, col5 = st.columns([3, 2, 1, 1, 1])
        with col1:
            st.write(f"**{row['nm_contador']}**")
        with col2:
            st.write(row['nm_gerfe'])
        with col3:
            st.metric("Grupos", int(row['qntd_grupos']), label_visibility="collapsed")
        with col4:
            st.metric("Score", f"{row['media']:.2f}", label_visibility="collapsed")
        with col5:
            if st.button("📊 Detalhes", key=f"btn_{idx}"):
                st.session_state.contador_selecionado = {
                    'nm_contador': row['nm_contador'],
                    'nm_gerfe': row['nm_gerfe']
                }
                st.rerun()
    
    # Download
    csv = df_filtrado.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name="contadores_analise.csv",
        mime="text/csv"
    )
            
def menu_pagamentos(engine, dados, filtros):
    """Análise de meios de pagamento"""
    st.markdown("<h1 class='main-header'>Análise de Meios de Pagamento</h1>", unsafe_allow_html=True)
    
    if dados['pagamentos_metricas'].empty:
        st.warning("Nenhum dado de pagamento disponível.")
        return
    
    # Merge com percent para ter scores
    score_col = 'score_final_ccs' if 'score_final_ccs' in dados['percent'].columns else 'score_final_avancado'
    df_pag = dados['pagamentos_metricas'].merge(
        dados['percent'][['num_grupo', score_col, 'qntd_cnpj', 'indice_risco_pagamentos']],
        on='num_grupo',
        how='left'
    )
    
    # Limpar valores NaN para os gráficos
    df_pag = df_pag.dropna(subset=[score_col, 'indice_risco_pagamentos', 'qntd_cnpj'])
    
    # Panorama Geral
    st.subheader("Panorama Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Grupos com Dados", len(df_pag))
    with col2:
        total_empresas = df_pag['valor_meios_pagamento_empresas'].sum()
        st.metric("Total Empresas", formatar_moeda(total_empresas))
    with col3:
        total_socios = df_pag['valor_meios_pagamento_socios'].sum()
        st.metric("Total Sócios", formatar_moeda(total_socios))
    with col4:
        media_risco = df_pag['indice_risco_pagamentos'].mean()
        st.metric("Índice Risco Médio", f"{media_risco:.3f}")
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribuição do índice de risco
        fig = px.histogram(df_pag, x='indice_risco_pagamentos', nbins=30,
                          title="Distribuição do Índice de Risco Pagamentos",
                          template=filtros['tema'])
        st.plotly_chart(fig)
    
    with col2:
        # Scatter: Score vs Risco Pagamentos
        fig = px.scatter(df_pag, x=score_col, y='indice_risco_pagamentos',
                        hover_data=['num_grupo', 'qntd_cnpj'],
                        title="Score Total vs Risco Pagamentos",
                        template=filtros['tema'])
        st.plotly_chart(fig)
    
    # Top grupos por risco de confusão patrimonial
    st.subheader("Top 20 Grupos - Maior Risco de Confusão Patrimonial")
    
    df_top = df_pag[df_pag['valor_meios_pagamento_empresas'] > 0].nlargest(20, 'indice_risco_pagamentos').copy()
    df_top['Valor Empresas'] = df_top['valor_meios_pagamento_empresas'].apply(formatar_moeda)
    df_top['Valor Sócios'] = df_top['valor_meios_pagamento_socios'].apply(formatar_moeda)
    
    st.dataframe(df_top[['num_grupo', 'indice_risco_pagamentos', 'Valor Empresas', 'Valor Sócios',
                         'qntd_cnpj', score_col]],
                width='stretch', hide_index=True)

    # =========================================================================
    # DRILL DOWN POR GRUPO
    # =========================================================================
    st.divider()
    st.subheader("Detalhes por Grupo")

    grupo_selecionado = st.selectbox(
        "Selecione um grupo para ver detalhes:",
        options=['Selecione...'] + sorted(df_pag['num_grupo'].unique().tolist()),
        key="pagamentos_drill_down"
    )

    if grupo_selecionado and grupo_selecionado != 'Selecione...':
        info_grupo = df_pag[df_pag['num_grupo'] == grupo_selecionado].iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Grupo", grupo_selecionado)
        with col2:
            st.metric("CNPJs", int(info_grupo['qntd_cnpj']))
        with col3:
            st.metric("Valor Empresas", formatar_moeda(info_grupo['valor_meios_pagamento_empresas']))
        with col4:
            st.metric("Valor Sócios", formatar_moeda(info_grupo['valor_meios_pagamento_socios']))

        st.write(f"**Índice de Risco Pagamentos:** {info_grupo['indice_risco_pagamentos']:.4f}")
        st.write(f"**Score Final:** {info_grupo[score_col]:.2f}")

        # Buscar detalhes dos CNPJs do grupo
        try:
            cnpjs_grupo = dados['cnpj'][dados['cnpj']['num_grupo'] == grupo_selecionado]['cnpj'].tolist()
            if cnpjs_grupo:
                st.write(f"**CNPJs do grupo:** {', '.join(cnpjs_grupo[:10])}{'...' if len(cnpjs_grupo) > 10 else ''}")
        except:
            pass

def menu_funcionarios(engine, dados, filtros):
    """Análise de funcionários"""
    st.markdown("<h1 class='main-header'>Análise de Funcionários</h1>", unsafe_allow_html=True)
    
    if dados['funcionarios_metricas'].empty:
        st.warning("Nenhum dado de funcionários disponível.")
        return
    
    # Merge com percent
    score_col = 'score_final_ccs' if 'score_final_ccs' in dados['percent'].columns else 'score_final_avancado'
    df_func = dados['funcionarios_metricas'].merge(
        dados['percent'][['num_grupo', score_col, 'qntd_cnpj', 'valor_max', 'indice_risco_fat_func']],
        on='num_grupo',
        how='left'
    )
    
    # Limpar valores NaN para os gráficos
    df_func = df_func.dropna(subset=[score_col, 'total_funcionarios', 'valor_max'])
    
    # Panorama Geral
    st.subheader("Panorama Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Grupos com Dados", len(df_func))
    with col2:
        st.metric("Total Funcionários", f"{int(df_func['total_funcionarios'].sum()):,}")
    with col3:
        media_func = df_func['total_funcionarios'].mean()
        st.metric("Média Funcionários/Grupo", f"{media_func:.1f}")
    with col4:
        if 'indice_risco_fat_func' in df_func.columns:
            media_risco = df_func['indice_risco_fat_func'].mean()
            st.metric("Índice Risco Médio", f"{media_risco:.3f}")
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribuição de funcionários
        fig = px.histogram(df_func, x='total_funcionarios', nbins=30,
                          title="Distribuição de Funcionários por Grupo",
                          template=filtros['tema'])
        st.plotly_chart(fig)
    
    with col2:
        # Scatter: Faturamento vs Funcionários
        fig = px.scatter(df_func, x='total_funcionarios', y='valor_max',
                        hover_data=['num_grupo', 'qntd_cnpj'],
                        title="Faturamento vs Funcionários",
                        template=filtros['tema'],
                        labels={'valor_max': 'Faturamento', 'total_funcionarios': 'Funcionários'})
        st.plotly_chart(fig)
    
    # Top grupos com maior desproporção
    st.subheader("Top 20 Grupos - Alta Receita / Poucos Funcionários")
    
    if 'indice_risco_fat_func' in df_func.columns:
        df_top = df_func[
            (df_func['valor_max'] > 1000000) & 
            (df_func['total_funcionarios'] <= 10)
        ].nlargest(20, 'indice_risco_fat_func').copy()
        
        df_top['Faturamento'] = df_top['valor_max'].apply(formatar_moeda)
        
        st.dataframe(df_top[['num_grupo', 'indice_risco_fat_func', 'Faturamento', 
                             'total_funcionarios', 'qntd_cnpj', score_col]], 
                    width='stretch', hide_index=True)
    else:
        # Fallback se não tiver o índice
        df_top = df_func[
            (df_func['valor_max'] > 1000000) & 
            (df_func['total_funcionarios'] <= 10)
        ].nlargest(20, 'valor_max').copy()
        
        df_top['Faturamento'] = df_top['valor_max'].apply(formatar_moeda)
        df_top['Receita_por_Funcionario'] = df_top['valor_max'] / df_top['total_funcionarios']
        
        st.dataframe(df_top[['num_grupo', 'Faturamento', 'total_funcionarios',
                             'Receita_por_Funcionario', 'qntd_cnpj', score_col]],
                    width='stretch', hide_index=True)

    # =========================================================================
    # DRILL DOWN POR GRUPO
    # =========================================================================
    st.divider()
    st.subheader("Detalhes por Grupo")

    grupo_selecionado = st.selectbox(
        "Selecione um grupo para ver detalhes:",
        options=['Selecione...'] + sorted(df_func['num_grupo'].unique().tolist()),
        key="funcionarios_drill_down"
    )

    if grupo_selecionado and grupo_selecionado != 'Selecione...':
        info_grupo = df_func[df_func['num_grupo'] == grupo_selecionado].iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Grupo", grupo_selecionado)
        with col2:
            st.metric("CNPJs", int(info_grupo['qntd_cnpj']))
        with col3:
            st.metric("Total Funcionários", int(info_grupo['total_funcionarios']))
        with col4:
            st.metric("Faturamento", formatar_moeda(info_grupo['valor_max']))

        if 'indice_risco_fat_func' in info_grupo:
            st.write(f"**Índice de Risco Fat/Func:** {info_grupo['indice_risco_fat_func']:.4f}")
        st.write(f"**Score Final:** {info_grupo[score_col]:.2f}")

        # Cálculo de receita por funcionário
        if info_grupo['total_funcionarios'] > 0:
            receita_por_func = info_grupo['valor_max'] / info_grupo['total_funcionarios']
            st.write(f"**Receita por Funcionário:** {formatar_moeda(receita_por_func)}")

def menu_c115(engine, dados, filtros):
    """Análise Convênio 115"""
    st.markdown("<h1 class='main-header'>Análise Convênio 115</h1>", unsafe_allow_html=True)
    
    if dados['c115_ranking'].empty:
        st.warning("Nenhum dado C115 disponível.")
        return
    
    df_c115 = dados['c115_ranking'].copy()
    
    # Panorama Geral
    st.subheader("Panorama Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Grupos Monitorados", len(df_c115))
    with col2:
        criticos = len(df_c115[df_c115['nivel_risco_grupo_economico'] == 'CRÍTICO'])
        st.metric("Grupos Críticos", criticos)
    with col3:
        media_indice = df_c115['indice_risco_grupo_economico'].mean()
        st.metric("Índice Risco Médio", f"{media_indice:.2f}")
    with col4:
        total_tomadores = df_c115['total_tomadores'].sum()
        st.metric("Total Tomadores", f"{int(total_tomadores):,}")
    
    # Distribuição por Nível de Risco
    st.subheader("Distribuição por Nível de Risco")
    
    col1, col2 = st.columns(2)
    
    with col1:
        dist_risco = df_c115['nivel_risco_grupo_economico'].value_counts()
        fig = px.pie(values=dist_risco.values, names=dist_risco.index,
                    title="Distribuição de Grupos por Nível de Risco C115",
                    template=filtros['tema'])
        st.plotly_chart(fig)
    
    with col2:
        fig = px.histogram(df_c115, x='indice_risco_grupo_economico', nbins=30,
                          title="Distribuição do Índice de Risco",
                          template=filtros['tema'])
        st.plotly_chart(fig)
    
    # Top 30 Grupos por Risco C115
    st.subheader("Top 30 Grupos - Maior Risco C115")
    
    df_top = df_c115.nlargest(30, 'indice_risco_grupo_economico')
    
    st.dataframe(df_top[['num_grupo', 'ranking_risco', 'nivel_risco_grupo_economico',
                         'indice_risco_grupo_economico', 'total_cnpjs', 'qtd_cnpjs_relacionados',
                         'perc_cnpjs_relacionados', 'pares_com_tres_tipos_comum']], 
                width='stretch', hide_index=True)
    
    # Análise Detalhada
    st.subheader("Análise Detalhada por Grupo")
    
    grupos_disponiveis = sorted(df_c115['num_grupo'].unique())
    grupo_selecionado = st.selectbox("Selecione um grupo:", grupos_disponiveis, key="c115_grupo")
    
    if grupo_selecionado:
        info = df_c115[df_c115['num_grupo'] == grupo_selecionado].iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Ranking", int(info['ranking_risco']))
        with col2:
            st.metric("Nível Risco", info['nivel_risco_grupo_economico'])
        with col3:
            st.metric("Índice Risco", f"{info['indice_risco_grupo_economico']:.2f}")
        with col4:
            st.metric("CNPJs Relacionados", int(info['qtd_cnpjs_relacionados']))

def menu_ccs(engine, dados, filtros):
    """Análise de Procuração Bancária (CCS)"""
    st.markdown("<h1 class='main-header'>Procuração Bancária (CCS)</h1>", unsafe_allow_html=True)
    
    st.info("Análise de contas bancárias compartilhadas entre CNPJs do mesmo grupo econômico.")
    
    if dados['ccs_metricas'].empty:
        st.warning("Nenhum dado CCS disponível.")
        return
    
    # Merge com percent para ter scores E nivel_risco_ccs
    score_col = 'score_final_ccs' if 'score_final_ccs' in dados['percent'].columns else 'score_final_avancado'
    
    # Incluir nivel_risco_ccs no merge
    colunas_merge = ['num_grupo', score_col, 'qntd_cnpj']
    if 'nivel_risco_ccs' in dados['percent'].columns:
        colunas_merge.append('nivel_risco_ccs')
    
    df_ccs = dados['ccs_metricas'].merge(
        dados['percent'][colunas_merge],
        on='num_grupo',
        how='left'
    )
    
    # Panorama Geral
    st.subheader("Panorama Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Grupos com Dados CCS", len(df_ccs))
    with col2:
        grupos_compartilhamento = len(df_ccs[df_ccs['qtd_contas_compartilhadas'] > 0])
        st.metric("Grupos com Compartilhamento", grupos_compartilhamento)
    with col3:
        total_compartilhadas = df_ccs['qtd_contas_compartilhadas'].sum()
        st.metric("Total Contas Compartilhadas", int(total_compartilhadas))
    with col4:
        media_indice = df_ccs['indice_risco_ccs'].mean()
        st.metric("Índice Risco CCS Médio", f"{media_indice:.4f}")
    
    # Gráficos
    st.subheader("Análises Visuais")
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribuição do índice de risco CCS
        fig = px.histogram(df_ccs, x='indice_risco_ccs', nbins=30,
                          title="Distribuição do Índice de Risco CCS",
                          template=filtros['tema'])
        st.plotly_chart(fig)
    
    with col2:
        # Distribuição por nível de risco
        if 'nivel_risco_ccs' in df_ccs.columns:
            dist_nivel = df_ccs['nivel_risco_ccs'].value_counts()
            fig = px.pie(values=dist_nivel.values, names=dist_nivel.index,
                        title="Distribuição por Nível de Risco CCS",
                        template=filtros['tema'])
            st.plotly_chart(fig)
    
    # Análise de Compartilhamento
    st.subheader("Análise de Compartilhamento de Contas")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Scatter: Contas compartilhadas vs Score
        fig = px.scatter(df_ccs, x='qtd_contas_compartilhadas', y=score_col,
                        hover_data=['num_grupo', 'qntd_cnpj'],
                        title="Contas Compartilhadas vs Score Total",
                        template=filtros['tema'])
        st.plotly_chart(fig)
    
    with col2:
        # Max CNPJs por conta
        fig = px.histogram(df_ccs, x='max_cnpjs_por_conta', nbins=20,
                          title="Distribuição - Máx CNPJs por Conta",
                          template=filtros['tema'])
        st.plotly_chart(fig)
    
    # Top Grupos por Risco CCS
    st.subheader("Top 30 Grupos - Maior Risco CCS")
    
    df_top = df_ccs[df_ccs['qtd_contas_compartilhadas'] > 0].nlargest(30, 'indice_risco_ccs')
    
    # Montar lista de colunas dinamicamente
    colunas_exibir = ['num_grupo', 'indice_risco_ccs']
    if 'nivel_risco_ccs' in df_top.columns:
        colunas_exibir.append('nivel_risco_ccs')
    colunas_exibir.extend(['qtd_contas_compartilhadas', 'perc_contas_compartilhadas',
                          'max_cnpjs_por_conta', 'qtd_sobreposicoes_responsaveis',
                          'media_dias_sobreposicao', 'qntd_cnpj', score_col])
    
    st.dataframe(df_top[colunas_exibir])
    
    # Análise Detalhada por Grupo
    st.subheader("Análise Detalhada por Grupo")
    
    grupos_disponiveis = sorted(df_ccs['num_grupo'].unique())
    grupo_selecionado = st.selectbox("Selecione um grupo para análise detalhada:", 
                                     grupos_disponiveis, key="ccs_grupo_detalhe")
    
    if grupo_selecionado:
        info_grupo = df_ccs[df_ccs['num_grupo'] == grupo_selecionado].iloc[0]
        
        st.write("### Métricas do Grupo")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Índice Risco CCS", f"{info_grupo['indice_risco_ccs']:.4f}")
        with col2:
            st.metric("Nível Risco", info_grupo.get('nivel_risco_ccs', 'N/A'))
        with col3:
            st.metric("Contas Compartilhadas", int(info_grupo['qtd_contas_compartilhadas']))
        with col4:
            st.metric("Max CNPJs/Conta", int(info_grupo['max_cnpjs_por_conta']))
        
        st.divider()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Sobreposições", int(info_grupo['qtd_sobreposicoes_responsaveis']))
        with col2:
            st.metric("Média Dias Sobreposição", f"{info_grupo['media_dias_sobreposicao']:.0f}")
        with col3:
            st.metric("Aberturas Coordenadas", int(info_grupo['qtd_datas_abertura_coordenada']))
        
        # Carregar dados detalhados
        try:
            # Contas compartilhadas do grupo
            query_compartilhadas = f"""
            SELECT * FROM {DATABASE}.gei_ccs_cpf_compartilhado
            WHERE CAST(num_grupo AS INT) = {grupo_selecionado}
            ORDER BY qtd_cnpjs_usando_conta DESC
            """
            df_compartilhadas = pd.read_sql(query_compartilhadas, engine)
            
            if not df_compartilhadas.empty:
                st.write("### Contas Compartilhadas")
                st.dataframe(df_compartilhadas[['nr_cpf', 'nm_banco', 'cd_agencia', 'nr_conta',
                                               'qtd_cnpjs_usando_conta', 'qtd_vinculos',
                                               'qtd_vinculos_ativos', 'status_conta']].head(20),
                            width='stretch', hide_index=True)
            
            # Sobreposições de responsáveis
            query_sobreposicoes = f"""
            SELECT * FROM {DATABASE}.gei_ccs_sobreposicao_responsaveis
            WHERE CAST(num_grupo AS INT) = {grupo_selecionado}
            ORDER BY dias_sobreposicao DESC
            """
            df_sobreposicoes = pd.read_sql(query_sobreposicoes, engine)
            
            if not df_sobreposicoes.empty:
                st.write("### Sobreposições de Responsáveis")
                st.dataframe(df_sobreposicoes[['nr_cpf', 'cnpj1', 'cnpj2', 
                                               'dias_sobreposicao', 'inicio1', 'fim1']].head(20),
                            width='stretch', hide_index=True)
            
            # Padrões coordenados
            query_padroes = f"""
            SELECT * FROM {DATABASE}.gei_ccs_padroes_coordenados
            WHERE CAST(num_grupo AS INT) = {grupo_selecionado}
            ORDER BY dt_evento DESC
            """
            df_padroes = pd.read_sql(query_padroes, engine)
            
            if not df_padroes.empty:
                st.write("### Padrões Coordenados de Abertura/Encerramento")
                st.dataframe(df_padroes[['tipo_evento', 'dt_evento', 'qtd_cnpjs',
                                        'qtd_contas', 'qtd_cpfs_distintos']].head(20),
                            width='stretch', hide_index=True)
                
        except Exception as e:
            st.error(f"Erro ao carregar detalhes: {e}")


def menu_financeiro(engine, dados, filtros):
    """Análise financeira detalhada"""
    st.markdown("<h1 class='main-header'>Análise Financeira Detalhada</h1>", unsafe_allow_html=True)
    
    df = aplicar_filtros(dados['percent'], filtros)
    
    if df.empty:
        st.warning("Nenhum dado encontrado.")
        return
    
    # Panorama Geral
    st.subheader("Indicadores Financeiros")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        receita = df['valor_max'].sum()
        st.metric("Receita Total Monitorada", formatar_moeda(receita))
    
    with col2:
        acima = len(df[df['valor_max'] > 4800000])
        perc = (acima / len(df) * 100) if len(df) > 0 else 0
        st.metric("Acima Limite SN", f"{acima:,}", f"{perc:.1f}%")
    
    with col3:
        if 'total_funcionarios' in df.columns:
            df_v = df[(df['valor_max'] > 0) & (df['total_funcionarios'] > 0)]
            if not df_v.empty:
                media = (df_v['valor_max'] / df_v['total_funcionarios']).mean()
                st.metric("Receita/Funcionário", formatar_moeda(media))
    
    with col4:
        media_score = df[df['valor_max'] > 4800000]['score_final_ccs' if 'score_final_ccs' in df.columns else 'score_final_avancado'].mean()
        st.metric("Score Médio (>Limite)", f"{media_score:.2f}")
    
    # Distribuição por Faixas de Receita
    st.subheader("Distribuição por Faixa de Receita")
    
    faixas = {
        '0-1M': (0, 1e6),
        '1-2M': (1e6, 2e6),
        '2-3M': (2e6, 3e6),
        '3-4M': (3e6, 4e6),
        '4-4.8M': (4e6, 4.8e6),
        '>4.8M': (4.8e6, float('inf'))
    }
    
    contagens = []
    scores_medios = []
    score_col = 'score_final_ccs' if 'score_final_ccs' in df.columns else 'score_final_avancado'
    
    for nome, (inicio, fim) in faixas.items():
        grupos_faixa = df[(df['valor_max'] >= inicio) & (df['valor_max'] < fim)]
        contagens.append(len(grupos_faixa))
        scores_medios.append(grupos_faixa[score_col].mean() if len(grupos_faixa) > 0 else 0)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(x=list(faixas.keys()), y=contagens, 
                    title="Grupos por Faixa de Receita",
                    template=filtros['tema'],
                    labels={'x': 'Faixa', 'y': 'Quantidade de Grupos'})
        st.plotly_chart(fig)
    
    with col2:
        fig = px.bar(x=list(faixas.keys()), y=scores_medios,
                    title="Score Médio por Faixa de Receita",
                    template=filtros['tema'],
                    labels={'x': 'Faixa', 'y': 'Score Médio'})
        st.plotly_chart(fig)
    
    # Análise Temporal (PGDAS + DIME consolidado)
    st.subheader("Evolução Temporal")

    try:
        # Query consolidada PGDAS + DIME
        query_temporal = """
        WITH pgdas_data AS (
            SELECT
                gc.num_grupo,
                COALESCE(pg.jan2025, 0) as jan2025,
                COALESCE(pg.fev2025, 0) as fev2025,
                COALESCE(pg.mar2025, 0) as mar2025,
                COALESCE(pg.abr2025, 0) as abr2025,
                COALESCE(pg.mai2025, 0) as mai2025,
                COALESCE(pg.jun2025, 0) as jun2025,
                COALESCE(pg.jul2025, 0) as jul2025,
                COALESCE(pg.ago2025, 0) as ago2025,
                COALESCE(pg.set2025, 0) as set2025,
                'PGDAS' as fonte
            FROM gessimples.gei_cnpj gc
            JOIN gessimples.gei_pgdas pg ON gc.cnpj = pg.cnpj
        ),
        dime_data AS (
            SELECT
                gc.num_grupo,
                COALESCE(dm.jan2025, 0) as jan2025,
                COALESCE(dm.fev2025, 0) as fev2025,
                COALESCE(dm.mar2025, 0) as mar2025,
                COALESCE(dm.abr2025, 0) as abr2025,
                COALESCE(dm.mai2025, 0) as mai2025,
                COALESCE(dm.jun2025, 0) as jun2025,
                COALESCE(dm.jul2025, 0) as jul2025,
                COALESCE(dm.ago2025, 0) as ago2025,
                COALESCE(dm.set2025, 0) as set2025,
                'DIME' as fonte
            FROM gessimples.gei_cnpj gc
            JOIN gessimples.gei_dime dm ON gc.cnpj = dm.cnpj
        ),
        consolidado AS (
            SELECT * FROM pgdas_data
            UNION ALL
            SELECT * FROM dime_data
        )
        SELECT
            num_grupo,
            SUM(jan2025) as jan2025,
            SUM(fev2025) as fev2025,
            SUM(mar2025) as mar2025,
            SUM(abr2025) as abr2025,
            SUM(mai2025) as mai2025,
            SUM(jun2025) as jun2025,
            SUM(jul2025) as jul2025,
            SUM(ago2025) as ago2025,
            SUM(set2025) as set2025
        FROM consolidado
        GROUP BY num_grupo
        LIMIT 20
        """
        df_temp = pd.read_sql(query_temporal, engine)

        if not df_temp.empty:
            # Pegar top 10 grupos por receita total
            df_temp['total'] = df_temp[[c for c in df_temp.columns if c != 'num_grupo']].sum(axis=1)
            df_temp = df_temp.nlargest(10, 'total')

            # Transformar para formato long
            meses = ['jan2025', 'fev2025', 'mar2025', 'abr2025', 'mai2025',
                    'jun2025', 'jul2025', 'ago2025', 'set2025']

            df_long = df_temp.melt(id_vars=['num_grupo'],
                                  value_vars=meses,
                                  var_name='mes',
                                  value_name='receita')

            fig = px.line(df_long, x='mes', y='receita', color='num_grupo',
                         title="Evolução de Receita - Top 10 Grupos (PGDAS + DIME, 2025)",
                         template=filtros['tema'])
            st.plotly_chart(fig)
    except Exception as e:
        st.info(f"Dados temporais não disponíveis: {e}")
    
    # Top Grupos Financeiros
    st.subheader("Top 30 Grupos por Receita")
    df_top = df.nlargest(30, 'valor_max').copy()
    df_top['Receita'] = df_top['valor_max'].apply(formatar_moeda)

    st.dataframe(df_top[['num_grupo', 'Receita', 'qntd_cnpj', 'total_funcionarios',
                         score_col, 'nivel_risco_grupo_economico']],
                width='stretch', hide_index=True)

    # =========================================================================
    # DRILL DOWN POR GRUPO
    # =========================================================================
    st.divider()
    st.subheader("Detalhes por Grupo")

    grupo_selecionado = st.selectbox(
        "Selecione um grupo para ver detalhes:",
        options=['Selecione...'] + sorted(df['num_grupo'].unique().tolist()),
        key="financeiro_drill_down"
    )

    if grupo_selecionado and grupo_selecionado != 'Selecione...':
        info_grupo = df[df['num_grupo'] == grupo_selecionado].iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Grupo", grupo_selecionado)
        with col2:
            st.metric("CNPJs", int(info_grupo['qntd_cnpj']))
        with col3:
            st.metric("Receita Máxima", formatar_moeda(info_grupo['valor_max']))
        with col4:
            st.metric("Período Máximo", str(info_grupo.get('periodo_max', 'N/A')))

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("1º Excesso Limite", str(info_grupo.get('periodo', 'Nunca')))
        with col2:
            func = info_grupo.get('total_funcionarios', 0)
            st.metric("Funcionários", int(func) if pd.notna(func) else 0)
        with col3:
            st.metric("Score Final", f"{info_grupo[score_col]:.2f}")

        # Indicar se está acima do limite do Simples
        if info_grupo['valor_max'] > 4800000:
            st.error(f"ATENÇÃO: Receita acima do limite do Simples Nacional (R$ 4,8M)")
            excesso = info_grupo['valor_max'] - 4800000
            st.write(f"**Excesso sobre o limite:** {formatar_moeda(excesso)}")

def inconsistencias_nfe(engine, dados, filtros):
    """Análise de inconsistências de NFe"""
    st.markdown("<h1 class='main-header'>Inconsistências de NFe</h1>", unsafe_allow_html=True)
    
    st.info("Esta análise identifica valores compartilhados entre múltiplos CNPJs do mesmo grupo.")
    
    # Seleção de Grupo
    st.subheader("Análise por Grupo")
    
    grupos_disponiveis = sorted(dados['percent']['num_grupo'].unique())
    grupo_selecionado = st.selectbox(
        "Selecione um grupo para análise detalhada:",
        grupos_disponiveis,
        key="incons_grupo"
    )
    
    if grupo_selecionado:
        # Carregar dados do grupo com CAMPOS CORRETOS
        try:
            query_incons = f"""
            SELECT 
                nfe_nu_chave_acesso,
                nfe_dt_emissao,
                nfe_cnpj_cpf_emit,
                nfe_cnpj_cpf_dest,
                nfe_dest_email,
                nfe_dest_telefone,
                nfe_emit_telefone,
                nfe_cd_produto,
                nfe_de_produto,
                nfe_emit_end_completo,
                nfe_dest_end_completo,
                nfe_ip_transmissao,
                cliente_incons,
                email_incons,
                tel_dest_incons,
                tel_emit_incons,
                codigo_produto_incons,
                fornecedor_incons,
                end_emit_incons,
                end_dest_incons,
                descricao_produto_incons,
                ip_transmissao_incons
            FROM {DATABASE}.gei_nfe_completo
            WHERE CAST(grupo_emit AS INT) = {grupo_selecionado}
               OR CAST(grupo_dest AS INT) = {grupo_selecionado}
            LIMIT 5000
            """
            df_incons = pd.read_sql(query_incons, engine)
            
            if df_incons.empty:
                st.warning("Nenhuma inconsistência encontrada para este grupo.")
                return
            
            # Informações do Grupo
            info_grupo = dados['percent'][dados['percent']['num_grupo'] == grupo_selecionado].iloc[0]
            score_col = 'score_final_ccs' if 'score_final_ccs' in info_grupo.index else 'score_final_avancado'
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Score Final", f"{info_grupo[score_col]:.2f}")
            with col2:
                st.metric("Documentos Analisados", f"{len(df_incons):,}")
            with col3:
                st.metric("CNPJs no Grupo", int(info_grupo['qntd_cnpj']))
            with col4:
                st.metric("Score Inconsistências", f"{info_grupo['total']:.2f}")
            
            # Resumo de Inconsistências
            st.subheader("Resumo de Inconsistências")
            
            tipos_incons = {
                'Clientes': 'cliente_incons',
                'E-mails': 'email_incons',
                'Tel. Destinatário': 'tel_dest_incons',
                'Tel. Emitente': 'tel_emit_incons',
                'Códigos Produto': 'codigo_produto_incons',
                'Fornecedores': 'fornecedor_incons',
                'End. Emitente': 'end_emit_incons',
                'End. Destinatário': 'end_dest_incons',
                'Desc. Produto': 'descricao_produto_incons',
                'IP Transmissão': 'ip_transmissao_incons'
            }
            
            resumo = []
            for nome, coluna in tipos_incons.items():
                total = len(df_incons[df_incons[coluna] == 'S'])
                perc = (total / len(df_incons) * 100) if len(df_incons) > 0 else 0
                resumo.append({
                    'Tipo': nome,
                    'Quantidade': total,
                    'Percentual': f"{perc:.1f}%"
                })
            
            df_resumo = pd.DataFrame(resumo).sort_values('Quantidade', ascending=False)
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                st.dataframe(df_resumo, hide_index=True)
            
            with col2:
                fig = px.bar(df_resumo, x='Tipo', y='Quantidade',
                           title="Inconsistências por Tipo",
                           template=filtros['tema'])
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig)
            
            # Detalhamento por Tipo com até 3 exemplos
            st.subheader("Detalhamento por Tipo de Inconsistência (até 3 exemplos por tipo)")
            
            # Mapeamento CORRETO baseado no SQL
            mapeamento_campos = {
                'cliente_incons': {
                    'nome': 'Clientes',
                    'campos': [
                        ('nfe_cnpj_cpf_dest', 'Cliente (Destinatário)'),
                        ('nfe_cnpj_cpf_emit', 'Emitente'),
                        ('nfe_cnpj_cpf_dest', 'Destinatário')
                    ]
                },
                'email_incons': {
                    'nome': 'E-mails',
                    'campos': [
                        ('nfe_dest_email', 'Email Destinatário'),
                        ('nfe_cnpj_cpf_emit', 'Emitente'),
                        ('nfe_cnpj_cpf_dest', 'Destinatário')
                    ]
                },
                'tel_dest_incons': {
                    'nome': 'Tel. Destinatário',
                    'campos': [
                        ('nfe_dest_telefone', 'Telefone Destinatário'),
                        ('nfe_cnpj_cpf_emit', 'Emitente'),
                        ('nfe_cnpj_cpf_dest', 'Destinatário')
                    ]
                },
                'tel_emit_incons': {
                    'nome': 'Tel. Emitente',
                    'campos': [
                        ('nfe_emit_telefone', 'Telefone Emitente'),
                        ('nfe_cnpj_cpf_emit', 'Emitente'),
                        ('nfe_cnpj_cpf_dest', 'Destinatário')
                    ]
                },
                'codigo_produto_incons': {
                    'nome': 'Códigos Produto',
                    'campos': [
                        ('nfe_cd_produto', 'Código Produto'),
                        ('nfe_cnpj_cpf_emit', 'Emitente')
                    ]
                },
                'fornecedor_incons': {
                    'nome': 'Fornecedores',
                    'campos': [
                        ('nfe_cnpj_cpf_emit', 'Fornecedor (Emitente)'),
                        ('nfe_cnpj_cpf_dest', 'Destinatário')
                    ]
                },
                'end_emit_incons': {
                    'nome': 'End. Emitente',
                    'campos': [
                        ('nfe_emit_end_completo', 'Endereço Emitente'),
                        ('nfe_cnpj_cpf_emit', 'Emitente'),
                        ('nfe_cnpj_cpf_dest', 'Destinatário')
                    ]
                },
                'end_dest_incons': {
                    'nome': 'End. Destinatário',
                    'campos': [
                        ('nfe_dest_end_completo', 'Endereço Destinatário'),
                        ('nfe_cnpj_cpf_emit', 'Emitente'),
                        ('nfe_cnpj_cpf_dest', 'Destinatário')
                    ]
                },
                'descricao_produto_incons': {
                    'nome': 'Desc. Produto',
                    'campos': [
                        ('nfe_de_produto', 'Descrição Produto'),
                        ('nfe_cnpj_cpf_emit', 'Emitente')
                    ]
                },
                'ip_transmissao_incons': {
                    'nome': 'IP Transmissão',
                    'campos': [
                        ('nfe_ip_transmissao', 'IP Transmissão'),
                        ('nfe_cnpj_cpf_emit', 'Emitente')
                    ]
                }
            }
            
            for coluna_incons, info_campo in mapeamento_campos.items():
                if coluna_incons in df_incons.columns:
                    df_tipo = df_incons[df_incons[coluna_incons] == 'S'].head(3)
                    
                    if not df_tipo.empty:
                        total_tipo = len(df_incons[df_incons[coluna_incons] == 'S'])
                        with st.expander(f"{info_campo['nome']} ({total_tipo} ocorrências)"):
                            for idx, row in df_tipo.iterrows():
                                st.write(f"**Nota Fiscal {idx + 1}:**")
                                st.write(f"- **Chave NFe:** {row.get('nfe_nu_chave_acesso', 'N/A')}")
                                st.write(f"- **Data Emissão:** {row.get('nfe_dt_emissao', 'N/A')}")
                                
                                # Mostrar campos específicos sem duplicação
                                campos_exibidos = set()
                                for campo_bd, label in info_campo['campos']:
                                    if campo_bd in row.index and pd.notna(row.get(campo_bd)):
                                        valor = row.get(campo_bd)
                                        chave_unica = f"{label}:{valor}"
                                        if chave_unica not in campos_exibidos:
                                            st.write(f"- **{label}:** {valor}")
                                            campos_exibidos.add(chave_unica)
                                
                                st.divider()
            
            # Download
            csv = df_incons.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV Completo",
                data=csv,
                file_name=f"inconsistencias_grupo_{grupo_selecionado}.csv",
                mime="text/csv"
            )
            
        except Exception as e:
            st.error(f"Erro ao carregar inconsistências: {e}")

def indicios_fiscais(dados, filtros):
    """Análise de indícios fiscais"""
    st.markdown("<h1 class='main-header'>Indícios Fiscais</h1>", unsafe_allow_html=True)
    st.info("Indícios fiscais identificados no sistema por grupo econômico.")
    
    # Análise geral
    df = dados['percent']
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        grupos_com = len(df[df['qtd_total_indicios'] > 0])
        st.metric("Grupos com Indícios", grupos_com)
    with col2:
        total = df['qtd_total_indicios'].sum()
        st.metric("Total de Indícios", f"{int(total):,}")
    with col3:
        media = df['qtd_total_indicios'].mean()
        st.metric("Média por Grupo", f"{media:.1f}")
    with col4:
        maximo = df['qtd_total_indicios'].max()
        st.metric("Máximo em um Grupo", int(maximo))
    
    # Top grupos
    st.subheader("Top 30 Grupos com Mais Indícios")
    df_top = df.nlargest(30, 'qtd_total_indicios')
    score_col = 'score_final_ccs' if 'score_final_ccs' in df.columns else 'score_final_avancado'
    st.dataframe(df_top[['num_grupo', 'qtd_total_indicios', 'qtd_tipos_indicios_distintos',
                        score_col, 'qntd_cnpj']],
                width='stretch', hide_index=True)

    # =========================================================================
    # DRILL DOWN POR GRUPO
    # =========================================================================
    st.divider()
    st.subheader("Detalhes por Grupo")

    df_com_indicios = df[df['qtd_total_indicios'] > 0]
    if df_com_indicios.empty:
        st.info("Nenhum grupo com indícios para detalhar.")
    else:
        grupo_selecionado = st.selectbox(
            "Selecione um grupo para ver detalhes dos indícios:",
            options=['Selecione...'] + sorted(df_com_indicios['num_grupo'].unique().tolist()),
            key="indicios_drill_down"
        )

        if grupo_selecionado and grupo_selecionado != 'Selecione...':
            info_grupo = df[df['num_grupo'] == grupo_selecionado].iloc[0]

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Grupo", grupo_selecionado)
            with col2:
                st.metric("Total Indícios", int(info_grupo['qtd_total_indicios']))
            with col3:
                st.metric("Tipos Distintos", int(info_grupo['qtd_tipos_indicios_distintos']))
            with col4:
                st.metric("CNPJs", int(info_grupo['qntd_cnpj']))

            col1, col2 = st.columns(2)
            with col1:
                perc = info_grupo.get('perc_cnpjs_com_indicios', 0)
                st.metric("% CNPJs com Indícios", f"{perc:.1f}%" if pd.notna(perc) else "N/A")
            with col2:
                st.metric("Score Final", f"{info_grupo[score_col]:.2f}")

            # Mostrar detalhes dos indícios se disponível
            if 'indicios' in dados and not dados['indicios'].empty:
                df_indicios_grupo = dados['indicios'][dados['indicios']['num_grupo'] == grupo_selecionado]
                if not df_indicios_grupo.empty:
                    st.write("**Indícios encontrados:**")
                    st.dataframe(df_indicios_grupo, hide_index=True, use_container_width=True)

def vinculos_societarios(dados, filtros):
    """Análise de vínculos societários"""
    st.markdown("<h1 class='main-header'>Vínculos Societários</h1>", unsafe_allow_html=True)
    
    df = aplicar_filtros(dados['percent'], filtros)
    
    if df.empty:
        st.warning("Nenhum dado encontrado.")
        return
    
    st.subheader("Métricas")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        grupos = len(df[df['qtd_socios_compartilhados'] > 0])
        perc = (grupos / len(df) * 100) if len(df) > 0 else 0
        st.metric("Grupos c/ Sócios Compartilhados", f"{grupos:,}", f"{perc:.1f}%")
    
    with col2:
        media = df['qtd_socios_compartilhados'].mean()
        st.metric("Média de Sócios", f"{media:.1f}")
    
    with col3:
        if 'indice_interconexao' in df.columns:
            st.metric("Índice Médio", f"{df['indice_interconexao'].mean():.3f}")
    
    grupo = st.selectbox("Selecione um grupo:", df['num_grupo'].tolist())
    
    if grupo:
        info = df[df['num_grupo'] == grupo].iloc[0]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("CNPJs", f"{int(info['qntd_cnpj'])}")
        with col2:
            st.metric("Sócios Compartilhados", f"{int(info['qtd_socios_compartilhados'])}")
        with col3:
            if 'indice_interconexao' in info:
                st.metric("Índice", f"{info['indice_interconexao']:.3f}")
        
        df_socios = dados['socios_compartilhados'][
            dados['socios_compartilhados']['num_grupo'] == grupo
        ]
        
        if not df_socios.empty:
            st.write("**Sócios:**")
            st.dataframe(df_socios.head(20), width='stretch', hide_index=True)

def dossie_grupo(engine, dados, filtros):
    """Dossiê completo do grupo"""
    st.markdown("<h1 class='main-header'>Dossiê Completo do Grupo</h1>", unsafe_allow_html=True)
    
    st.info("Visualize e exporte todas as informações consolidadas de um grupo.")
    
    # Seleção do grupo
    if dados['percent'].empty:
        st.warning("Nenhum grupo disponível.")
        return
    
    grupos_disponiveis = sorted(dados['percent']['num_grupo'].unique())
    
    # Adiciona opção "Selecione..." como padrão
    grupo_selecionado = st.selectbox(
        "Selecione o grupo para visualizar o dossiê completo:",
        options=['Selecione um grupo...'] + grupos_disponiveis,
        key="grupo_dossie"
    )
    
    # Só carrega se um grupo válido foi selecionado
    if not grupo_selecionado or grupo_selecionado == 'Selecione um grupo...':
        st.info("👆 Selecione um grupo acima para visualizar o dossiê completo")
        return
    
    # Carregar dossiê completo
    with st.spinner(f"Carregando dossiê completo do Grupo {grupo_selecionado}..."):
        dossie = carregar_dossie_completo(engine, grupo_selecionado)
    
    # Informações principais
    st.header(f"Grupo {grupo_selecionado}")
    
    if not dossie['principal'].empty:
        info = dossie['principal'].iloc[0]
        score_col = 'score_final_ccs' if 'score_final_ccs' in info.index else 'score_final_avancado'
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            score_val = info.get(score_col, 0)
            st.metric("Score Final", f"{score_val:.2f}" if pd.notna(score_val) else "N/A")
        with col2:
            cnpj_val = info.get('qntd_cnpj', 0)
            st.metric("Quantidade de CNPJs", int(cnpj_val) if pd.notna(cnpj_val) else 0)
        with col3:
            receita_val = info.get('valor_max', 0)
            st.metric("Receita Máxima", formatar_moeda(receita_val) if pd.notna(receita_val) else "R$ 0,00")
        with col4:
            func_val = info.get('total_funcionarios', 0)
            st.metric("Total Funcionários", int(func_val) if pd.notna(func_val) else 0)
        
        st.divider()
        
        # Métricas adicionais
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Nível Risco C115", str(info.get('nivel_risco_grupo_economico', 'N/A')))
        with col2:
            st.metric("Nível Risco CCS", str(info.get('nivel_risco_ccs', 'N/A')))
        with col3:
            indicios_val = info.get('qtd_total_indicios', 0)
            st.metric("Total Indícios", int(indicios_val) if pd.notna(indicios_val) else 0)
        with col4:
            socios_val = info.get('qtd_socios_compartilhados', 0)
            st.metric("Sócios Compartilhados", int(socios_val) if pd.notna(socios_val) else 0)
    
    # Tabs para organizar informações
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13, tab14 = st.tabs([
        "CNPJs e Cadastro",
        "Receita/Faturamento",
        "Sócios",
        "Indícios",
        "Inconsistências NFe",
        "C115",
        "CCS",
        "Funcionários",
        "Pagamentos",
        "Métricas Detalhadas",
        "Energia Elétrica",
        "Telecomunicações",
        "Análise de Similaridade",
        "Exportação"
    ])
    
    # =========================================================================
    # TAB 1: CNPJs E CADASTRO
    # =========================================================================
    with tab1:
        st.subheader("CNPJs do Grupo")
        
        if not dossie['cnpjs'].empty:
            st.write(f"**Total de {len(dossie['cnpjs'])} CNPJs**")
            
            # Garantir que todas as colunas sejam string para evitar erro Arrow
            df_display = dossie['cnpjs'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display, hide_index=True, width='stretch')
            
            # Download
            csv = df_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CNPJs (CSV)",
                data=csv,
                file_name=f"cnpjs_grupo_{grupo_selecionado}.csv",
                mime="text/csv"
            )
        else:
            st.error("❌ Nenhum CNPJ encontrado para este grupo.")

    # =========================================================================
    # TAB 2: RECEITA/FATURAMENTO (PGDAS + DIME)
    # =========================================================================
    with tab2:
        st.subheader("Receita/Faturamento (PGDAS + DIME)")

        # Métricas do gei_percent
        if not dossie['principal'].empty:
            info = dossie['principal'].iloc[0]

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                valor_max = info.get('valor_max', 0)
                st.metric("Receita Máxima (12m)", formatar_moeda(valor_max) if pd.notna(valor_max) else "R$ 0,00")
            with col2:
                periodo_max = info.get('periodo_max', 'N/A')
                st.metric("Período Máximo", str(periodo_max) if pd.notna(periodo_max) else "N/A")
            with col3:
                periodo_exc = info.get('periodo', 'N/A')
                st.metric("1º Excesso Limite SN", str(periodo_exc) if pd.notna(periodo_exc) else "Nunca")
            with col4:
                acima_limite = "SIM" if pd.notna(valor_max) and valor_max > 4800000 else "NÃO"
                st.metric("Acima R$ 4,8M?", acima_limite)

            st.divider()

        # Dados de faturamento detalhado
        if 'faturamento' in dossie and not dossie['faturamento'].empty:
            df_fat = dossie['faturamento'].copy()

            # Resumo por fonte
            col1, col2 = st.columns(2)
            with col1:
                qtd_pgdas = len(df_fat[df_fat['fonte'] == 'PGDAS'])
                st.metric("CNPJs com PGDAS (Simples)", qtd_pgdas)
            with col2:
                qtd_dime = len(df_fat[df_fat['fonte'] == 'DIME'])
                st.metric("CNPJs com DIME (Normal)", qtd_dime)

            st.divider()

            # Sub-tabs para PGDAS e DIME
            sub_tab1, sub_tab2, sub_tab3 = st.tabs(["Consolidado", "PGDAS (Simples)", "DIME (Normal)"])

            with sub_tab1:
                st.write("**Faturamento Consolidado (últimos meses de 2025):**")

                # Calcular último valor de cada CNPJ
                meses_cols = ['set2025', 'ago2025', 'jul2025', 'jun2025', 'mai2025', 'abr2025', 'mar2025', 'fev2025', 'jan2025']
                df_consolidado = df_fat.copy()

                # Pegar o último valor não-zero para cada CNPJ
                def get_ultimo_valor(row):
                    for mes in meses_cols:
                        if mes in row and pd.notna(row[mes]) and row[mes] > 0:
                            return row[mes]
                    return 0

                df_consolidado['ultimo_valor_12m'] = df_consolidado.apply(get_ultimo_valor, axis=1)

                # Agrupar por CNPJ pegando o maior valor (caso tenha PGDAS e DIME)
                df_resumo = df_consolidado.groupby('cnpj').agg({
                    'ultimo_valor_12m': 'max',
                    'fonte': lambda x: ', '.join(x.unique())
                }).reset_index()
                df_resumo.columns = ['CNPJ', 'Receita 12m', 'Fonte']
                df_resumo['Acima Limite'] = df_resumo['Receita 12m'].apply(lambda x: 'SIM' if x > 4800000 else 'NÃO')
                df_resumo['Receita 12m Formatada'] = df_resumo['Receita 12m'].apply(formatar_moeda)

                # Totais
                total_receita = df_resumo['Receita 12m'].sum()
                cnpjs_acima = len(df_resumo[df_resumo['Receita 12m'] > 4800000])

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total CNPJs", len(df_resumo))
                with col2:
                    st.metric("Receita Total (soma)", formatar_moeda(total_receita))
                with col3:
                    st.metric("CNPJs Acima Limite", cnpjs_acima)

                st.dataframe(
                    df_resumo.sort_values('Receita 12m', ascending=False)[['CNPJ', 'Receita 12m Formatada', 'Fonte', 'Acima Limite']],
                    hide_index=True,
                    use_container_width=True
                )

                # Gráfico de evolução consolidada
                st.divider()
                st.write("**Evolução do Faturamento por Mês:**")
                try:
                    # Transformar dados de wide para long
                    meses_disponiveis = [m for m in meses_cols if m in df_fat.columns]
                    df_chart = df_fat.melt(
                        id_vars=['cnpj', 'fonte'],
                        value_vars=meses_disponiveis,
                        var_name='periodo',
                        value_name='receita'
                    )
                    df_chart = df_chart[df_chart['receita'].notna() & (df_chart['receita'] > 0)]

                    if not df_chart.empty:
                        # Ordenar períodos cronologicamente
                        ordem_meses = {'jan2025': 1, 'fev2025': 2, 'mar2025': 3, 'abr2025': 4,
                                      'mai2025': 5, 'jun2025': 6, 'jul2025': 7, 'ago2025': 8, 'set2025': 9}
                        df_chart['ordem'] = df_chart['periodo'].map(ordem_meses)
                        df_chart = df_chart.sort_values('ordem')

                        # Gráfico 1: Receita TOTAL do grupo (soma de todos os CNPJs)
                        df_total_grupo = df_chart.groupby('periodo').agg({
                            'receita': 'sum',
                            'ordem': 'first'
                        }).reset_index().sort_values('ordem')

                        fig_total = px.line(
                            df_total_grupo,
                            x='periodo',
                            y='receita',
                            title="Receita TOTAL do Grupo (soma de todos os CNPJs)",
                            labels={'receita': 'Receita Total (R$)', 'periodo': 'Período'},
                            markers=True
                        )
                        fig_total.add_hline(y=4800000, line_dash="dash", line_color="red",
                                           annotation_text="Limite SN (R$ 4,8M)")
                        fig_total.update_traces(line=dict(width=3, color='#1f77b4'), marker=dict(size=10))
                        st.plotly_chart(fig_total, use_container_width=True)

                        # Verificar se ultrapassou limite em algum mês
                        meses_acima = df_total_grupo[df_total_grupo['receita'] > 4800000]
                        if not meses_acima.empty:
                            primeiro_mes = meses_acima.iloc[0]['periodo']
                            valor_primeiro = meses_acima.iloc[0]['receita']
                            st.error(f"⚠️ **ALERTA:** Receita total do grupo ultrapassou o limite do Simples Nacional em **{primeiro_mes}** (R$ {valor_primeiro:,.2f})")

                        st.divider()

                        # Gráfico 2: Evolução individual por CNPJ
                        st.write("**Evolução por CNPJ (individual):**")
                        fig = px.line(
                            df_chart,
                            x='periodo',
                            y='receita',
                            color='cnpj',
                            line_dash='fonte',
                            title="Evolução do Faturamento por CNPJ",
                            labels={'receita': 'Receita (R$)', 'periodo': 'Período', 'fonte': 'Fonte', 'cnpj': 'CNPJ'},
                            markers=True
                        )
                        fig.add_hline(y=4800000, line_dash="dash", line_color="red",
                                     annotation_text="Limite SN (R$ 4,8M)")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Sem dados suficientes para gerar o gráfico.")
                except Exception as e:
                    st.warning(f"Não foi possível gerar o gráfico: {e}")

            with sub_tab2:
                df_pgdas = df_fat[df_fat['fonte'] == 'PGDAS'].copy()
                if not df_pgdas.empty:
                    st.write(f"**{len(df_pgdas)} CNPJs com dados PGDAS (Simples Nacional):**")

                    # Gráfico PGDAS (antes de formatar)
                    try:
                        meses_disponiveis = [m for m in meses_cols if m in df_pgdas.columns]
                        df_chart_pgdas = df_pgdas.melt(
                            id_vars=['cnpj'],
                            value_vars=meses_disponiveis,
                            var_name='periodo',
                            value_name='receita'
                        )
                        df_chart_pgdas = df_chart_pgdas[df_chart_pgdas['receita'].notna() & (df_chart_pgdas['receita'] > 0)]

                        if not df_chart_pgdas.empty:
                            ordem_meses = {'jan2025': 1, 'fev2025': 2, 'mar2025': 3, 'abr2025': 4,
                                          'mai2025': 5, 'jun2025': 6, 'jul2025': 7, 'ago2025': 8, 'set2025': 9}
                            df_chart_pgdas['ordem'] = df_chart_pgdas['periodo'].map(ordem_meses)
                            df_chart_pgdas = df_chart_pgdas.sort_values('ordem')

                            fig_pgdas = px.line(
                                df_chart_pgdas,
                                x='periodo',
                                y='receita',
                                color='cnpj',
                                title="Evolução da Receita PGDAS (Simples Nacional)",
                                labels={'receita': 'Receita (R$)', 'periodo': 'Período', 'cnpj': 'CNPJ'},
                                markers=True
                            )
                            fig_pgdas.add_hline(y=4800000, line_dash="dash", line_color="red",
                                               annotation_text="Limite SN (R$ 4,8M)")
                            st.plotly_chart(fig_pgdas, use_container_width=True)
                    except Exception as e:
                        st.warning(f"Não foi possível gerar o gráfico PGDAS: {e}")

                    st.divider()
                    st.write("**Tabela Detalhada:**")
                    df_pgdas_display = df_pgdas.copy()
                    for col in df_pgdas_display.columns:
                        if col not in ['cnpj', 'fonte']:
                            df_pgdas_display[col] = df_pgdas_display[col].apply(lambda x: formatar_moeda(x) if pd.notna(x) else 'R$ 0,00')
                    st.dataframe(df_pgdas_display, hide_index=True, use_container_width=True)
                else:
                    st.info("Nenhum CNPJ com dados PGDAS encontrado.")

            with sub_tab3:
                df_dime = df_fat[df_fat['fonte'] == 'DIME'].copy()
                if not df_dime.empty:
                    st.write(f"**{len(df_dime)} CNPJs com dados DIME (Regime Normal):**")

                    # Gráfico DIME (antes de formatar)
                    try:
                        meses_disponiveis = [m for m in meses_cols if m in df_dime.columns]
                        df_chart_dime = df_dime.melt(
                            id_vars=['cnpj'],
                            value_vars=meses_disponiveis,
                            var_name='periodo',
                            value_name='faturamento'
                        )
                        df_chart_dime = df_chart_dime[df_chart_dime['faturamento'].notna() & (df_chart_dime['faturamento'] > 0)]

                        if not df_chart_dime.empty:
                            ordem_meses = {'jan2025': 1, 'fev2025': 2, 'mar2025': 3, 'abr2025': 4,
                                          'mai2025': 5, 'jun2025': 6, 'jul2025': 7, 'ago2025': 8, 'set2025': 9}
                            df_chart_dime['ordem'] = df_chart_dime['periodo'].map(ordem_meses)
                            df_chart_dime = df_chart_dime.sort_values('ordem')

                            fig_dime = px.line(
                                df_chart_dime,
                                x='periodo',
                                y='faturamento',
                                color='cnpj',
                                title="Evolução do Faturamento DIME (Regime Normal)",
                                labels={'faturamento': 'Faturamento (R$)', 'periodo': 'Período', 'cnpj': 'CNPJ'},
                                markers=True
                            )
                            st.plotly_chart(fig_dime, use_container_width=True)
                    except Exception as e:
                        st.warning(f"Não foi possível gerar o gráfico DIME: {e}")

                    st.divider()
                    st.write("**Tabela Detalhada:**")
                    df_dime_display = df_dime.copy()
                    for col in df_dime_display.columns:
                        if col not in ['cnpj', 'fonte']:
                            df_dime_display[col] = df_dime_display[col].apply(lambda x: formatar_moeda(x) if pd.notna(x) else 'R$ 0,00')
                    st.dataframe(df_dime_display, hide_index=True, use_container_width=True)
                else:
                    st.info("Nenhum CNPJ com dados DIME encontrado.")
        else:
            st.info("Nenhum dado de faturamento disponível para este grupo.")

    # =========================================================================
    # TAB 3: SÓCIOS
    # =========================================================================
    with tab3:
        st.subheader("Sócios Compartilhados")
        
        if not dossie['socios'].empty:
            st.write(f"**Total de {len(dossie['socios'])} registros**")
            
            # Converter para string
            df_display = dossie['socios'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display, width='stretch', hide_index=True)
            
            # Análise
            if 'cpf_socio' in dossie['socios'].columns:
                st.write("**Análise de Sócios:**")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Sócios Únicos", dossie['socios']['cpf_socio'].nunique())
                
                with col2:
                    if 'qtd_empresas' in dossie['socios'].columns:
                        max_empresas = dossie['socios']['qtd_empresas'].max()
                        st.metric("Max Empresas/Sócio", int(max_empresas) if pd.notna(max_empresas) else 0)
        else:
            st.info("Nenhum sócio compartilhado encontrado.")
    
    # =========================================================================
    # TAB 4: INDÍCIOS
    # =========================================================================
    with tab4:
        st.subheader("Indícios Fiscais")
        
        if not dossie['indicios'].empty:
            st.write(f"**Total de {len(dossie['indicios'])} indícios**")
            
            # Resumo por tipo
            resumo_tipos = dossie['indicios'].groupby('tx_descricao_indicio').size().reset_index(name='Quantidade')
            resumo_tipos = resumo_tipos.sort_values('Quantidade', ascending=False)
            
            # TABELA EM LARGURA TOTAL
            st.write("**Resumo por Tipo:**")
            st.dataframe(resumo_tipos, width='stretch', hide_index=True)
            
            # GRÁFICO ABAIXO DA TABELA
            fig = px.bar(resumo_tipos.head(10), x='Quantidade', y='tx_descricao_indicio',
                       orientation='h', title="Top 10 Tipos de Indícios",
                       template=filtros['tema'])
            st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            
            st.write("**Lista Completa:**")
            
            # Converter para string
            df_display = dossie['indicios'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display, width='stretch', hide_index=True)
            
            # Download
            csv = df_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Indícios (CSV)",
                data=csv,
                file_name=f"indicios_grupo_{grupo_selecionado}.csv",
                mime="text/csv"
            )
        else:
            st.info("Nenhum indício encontrado.")
    
    # =========================================================================
    # TAB 5: INCONSISTÊNCIAS NFE
    # =========================================================================
    with tab5:
        st.subheader("Inconsistências em Notas Fiscais")
        
        if not dossie['inconsistencias'].empty:
            st.write(f"**Total de {len(dossie['inconsistencias'])} documentos analisados**")
            
            # Calcular estatísticas
            tipos_incons = {
                'cliente_incons': 'Cliente',
                'email_incons': 'E-mail',
                'tel_dest_incons': 'Telefone Destinatário',
                'tel_emit_incons': 'Telefone Emitente',
                'codigo_produto_incons': 'Código Produto',
                'fornecedor_incons': 'Fornecedor',
                'end_emit_incons': 'Endereço Emitente',
                'end_dest_incons': 'Endereço Destinatário',
                'descricao_produto_incons': 'Descrição Produto',
                'ip_transmissao_incons': 'IP Transmissão'
            }
            
            # Resumo geral
            resumo = []
            for campo, label in tipos_incons.items():
                if campo in dossie['inconsistencias'].columns:
                    total = len(dossie['inconsistencias'][dossie['inconsistencias'][campo] == 'S'])
                    if total > 0:  # Só adiciona se houver inconsistências
                        resumo.append({
                            'Tipo': label,
                            'Quantidade': total
                        })
            
            if resumo:
                df_resumo = pd.DataFrame(resumo).sort_values('Quantidade', ascending=False)
                
                # Gráfico resumo
                st.write("**Resumo Geral:**")
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    st.dataframe(df_resumo, hide_index=True, use_container_width=True)
                
                with col2:
                    fig = px.bar(df_resumo, x='Tipo', y='Quantidade',
                               title="Inconsistências por Tipo",
                               template=filtros['tema'])
                    fig.update_xaxes(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                
                st.divider()
                
                # Detalhamento por tipo de inconsistência
                st.write("**Detalhamento por Tipo de Inconsistência:**")
                
                # Criar abas para cada tipo de inconsistência
                tipo_tabs = st.tabs([label for label in df_resumo['Tipo'].tolist()])
                
                for idx, (campo, label) in enumerate(tipos_incons.items()):
                    if campo in dossie['inconsistencias'].columns:
                        # Filtrar apenas as notas com esta inconsistência
                        df_filtrado = dossie['inconsistencias'][
                            dossie['inconsistencias'][campo] == 'S'
                        ].copy()
                        
                        if not df_filtrado.empty:
                            # Encontrar a aba correspondente
                            tab_idx = df_resumo[df_resumo['Tipo'] == label].index
                            if len(tab_idx) > 0:
                                with tipo_tabs[tab_idx[0]]:
                                    st.write(f"**{len(df_filtrado)} notas com inconsistência em {label}**")
                                    
                                    # Selecionar colunas relevantes para exibição
                                    colunas_exibir = []
                                    
                                    # Colunas básicas sempre presentes
                                    if 'chave_nfe' in df_filtrado.columns:
                                        colunas_exibir.append('chave_nfe')
                                    if 'num_nota' in df_filtrado.columns:
                                        colunas_exibir.append('num_nota')
                                    if 'cnpj_emit' in df_filtrado.columns:
                                        colunas_exibir.append('cnpj_emit')
                                    if 'cnpj_dest' in df_filtrado.columns:
                                        colunas_exibir.append('cnpj_dest')
                                    if 'dt_emissao' in df_filtrado.columns:
                                        colunas_exibir.append('dt_emissao')
                                    if 'vl_total_nf' in df_filtrado.columns:
                                        colunas_exibir.append('vl_total_nf')
                                    
                                    # Adicionar colunas específicas da inconsistência
                                    # (removendo o sufixo _incons para pegar os valores reais)
                                    campo_base = campo.replace('_incons', '')
                                    if campo_base in df_filtrado.columns:
                                        colunas_exibir.append(campo_base)
                                    
                                    # Filtrar apenas as colunas que existem
                                    colunas_exibir = [col for col in colunas_exibir if col in df_filtrado.columns]
                                    
                                    if colunas_exibir:
                                        df_display = df_filtrado[colunas_exibir].head(100).copy()
                                        
                                        # Formatar valores
                                        for col in df_display.columns:
                                            if col == 'vl_total_nf':
                                                df_display[col] = df_display[col].apply(
                                                    lambda x: formatar_moeda(x) if pd.notna(x) else 'N/A'
                                                )
                                            elif 'dt_' in col:
                                                df_display[col] = pd.to_datetime(df_display[col], errors='coerce').dt.strftime('%d/%m/%Y')
                                            else:
                                                df_display[col] = df_display[col].astype(str)
                                        
                                        # Renomear colunas para melhor visualização
                                        rename_dict = {
                                            'chave_nfe': 'Chave NFe',
                                            'num_nota': 'Número',
                                            'cnpj_emit': 'CNPJ Emitente',
                                            'cnpj_dest': 'CNPJ Destinatário',
                                            'dt_emissao': 'Data Emissão',
                                            'vl_total_nf': 'Valor Total',
                                            campo_base: label
                                        }
                                        df_display.rename(columns=rename_dict, inplace=True)
                                        
                                        st.dataframe(df_display, hide_index=True, use_container_width=True)
                                        
                                        # Botão de download
                                        csv = df_filtrado.to_csv(index=False).encode('utf-8')
                                        st.download_button(
                                            label=f"📥 Download CSV - {label}",
                                            data=csv,
                                            file_name=f"inconsistencias_{campo_base}_grupo_{grupo_selecionado}.csv",
                                            mime="text/csv",
                                            key=f"download_{campo}"
                                        )
                                    else:
                                        st.dataframe(df_filtrado.head(100), hide_index=True, use_container_width=True)
            else:
                st.success("✅ Nenhuma inconsistência encontrada!")
        else:
            st.info("Nenhum documento analisado.")
    
    # =========================================================================
    # TAB 6: CONVÊNIO 115
    # =========================================================================
    with tab6:
        st.subheader("Dados Convênio 115")
        
        if not dossie['c115'].empty:
            info_c115 = dossie['c115'].iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                ranking = info_c115.get('ranking_risco', 0)
                st.metric("Ranking Risco", int(ranking) if pd.notna(ranking) else 'N/A')
            with col2:
                st.metric("Nível Risco", info_c115.get('nivel_risco_grupo_economico', 'N/A'))
            with col3:
                indice = info_c115.get('indice_risco_grupo_economico', 0)
                st.metric("Índice Risco", f"{indice:.4f}" if pd.notna(indice) else 'N/A')
            with col4:
                qtd = info_c115.get('qtd_cnpjs_relacionados', 0)
                st.metric("CNPJs Relacionados", int(qtd) if pd.notna(qtd) else 0)
            
            st.divider()
            
            # Converter para string
            df_display = dossie['c115'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display, width='stretch', hide_index=True)
        else:
            st.info("Nenhum dado C115 disponível para este grupo.")
    
    # =========================================================================
    # TAB 7: CCS
    # =========================================================================
    with tab7:
        st.subheader("Dados CCS (Contas Compartilhadas)")
        
        # Métricas principais do gei_percent
        if not dossie['principal'].empty:
            info = dossie['principal'].iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                indice = info.get('indice_risco_ccs', 0)
                st.metric("Índice Risco CCS", f"{indice:.4f}" if pd.notna(indice) else 'N/A')
            with col2:
                st.metric("Nível Risco CCS", info.get('nivel_risco_ccs', 'N/A'))
            with col3:
                qtd = info.get('ccs_qtd_contas_compartilhadas', 0)
                st.metric("Contas Compartilhadas", int(qtd) if pd.notna(qtd) else 0)
            with col4:
                max_cnpj = info.get('ccs_max_cnpjs_por_conta', 0)
                st.metric("Max CNPJs/Conta", int(max_cnpj) if pd.notna(max_cnpj) else 0)
            
            st.divider()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                sobr = info.get('ccs_qtd_sobreposicoes_responsaveis', 0)
                st.metric("Sobreposições", int(sobr) if pd.notna(sobr) else 0)
            with col2:
                media = info.get('ccs_media_dias_sobreposicao', 0)
                st.metric("Média Dias Sobreposição", f"{media:.0f}" if pd.notna(media) else '0')
            with col3:
                aber = info.get('ccs_qtd_datas_abertura_coordenada', 0)
                st.metric("Aberturas Coordenadas", int(aber) if pd.notna(aber) else 0)
        
        # Contas compartilhadas
        if not dossie['ccs_compartilhadas'].empty:
            st.write("**Contas Compartilhadas:**")
            
            df_display = dossie['ccs_compartilhadas'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display.head(50), width='stretch', hide_index=True)
        else:
            st.info("Nenhuma conta compartilhada encontrada.")
        
        # Sobreposições
        if not dossie['ccs_sobreposicoes'].empty:
            st.write("**Sobreposições de Responsáveis:**")
            
            df_display = dossie['ccs_sobreposicoes'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display.head(50), width='stretch', hide_index=True)
        else:
            st.info("Nenhuma sobreposição encontrada.")
        
        # Padrões coordenados
        if not dossie['ccs_padroes'].empty:
            st.write("**Padrões Coordenados:**")
            
            df_display = dossie['ccs_padroes'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display, width='stretch', hide_index=True)
        else:
            st.info("Nenhum padrão coordenado encontrado.")
    
    # =========================================================================
    # TAB 8: FUNCIONÁRIOS
    # =========================================================================
    with tab8:
        st.subheader("Dados de Funcionários")
        
        if not dossie['funcionarios'].empty:
            info_func = dossie['funcionarios'].iloc[0]
            
            col1, col2 = st.columns(2)
            with col1:
                total = info_func.get('total_funcionarios', 0)
                st.metric("Total Funcionários", int(total) if pd.notna(total) else 0)
            with col2:
                cnpjs = info_func.get('cnpjs_com_funcionarios', 0)
                st.metric("CNPJs com Funcionários", int(cnpjs) if pd.notna(cnpjs) else 0)
            
            st.divider()
            
            # Converter para string
            df_display = dossie['funcionarios'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display, width='stretch', hide_index=True)
        else:
            st.info("Nenhum dado de funcionários disponível.")
    
    # =========================================================================
    # TAB 9: PAGAMENTOS
    # =========================================================================
    with tab9:
        st.subheader("Dados de Meios de Pagamento")
        
        if not dossie['pagamentos'].empty:
            info_pag = dossie['pagamentos'].iloc[0]
            
            col1, col2 = st.columns(2)
            with col1:
                valor_empresas = info_pag.get('valor_meios_pagamento_empresas', 0)
                st.metric("Pagamentos Empresas", formatar_moeda(valor_empresas) if pd.notna(valor_empresas) else 'R$ 0,00')
            with col2:
                valor_socios = info_pag.get('valor_meios_pagamento_socios', 0)
                st.metric("Pagamentos Sócios", formatar_moeda(valor_socios) if pd.notna(valor_socios) else 'R$ 0,00')
            
            st.divider()
            
            # Converter para string
            df_display = dossie['pagamentos'].copy()
            for col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
            
            st.dataframe(df_display, width='stretch', hide_index=True)
        else:
            st.info("Nenhum dado de pagamentos disponível.")
    
    # =========================================================================
    # TAB 10: MÉTRICAS DETALHADAS
    # =========================================================================
    with tab10:
        st.subheader("Métricas Detalhadas")
        
        if not dossie['principal'].empty:
            info = dossie['principal'].iloc[0]
            
            # Criar dataframe com todas as métricas
            metricas = []
            for col in info.index:
                if pd.notna(info[col]):
                    metricas.append({
                        'Métrica': col,
                        'Valor': str(info[col])
                    })
            
            df_metricas = pd.DataFrame(metricas)
            st.dataframe(df_metricas, width='stretch', hide_index=True)

    # =========================================================================
    # TAB 11: ENERGIA ELÉTRICA (NF3e)
    # =========================================================================
    with tab11:
        st.subheader("⚡ Consumo de Energia Elétrica")

        # Verificar se há dados de NF3e
        if 'nf3e' in dossie and not dossie['nf3e'].empty:
            df_nf3e = dossie['nf3e'].copy()

            # Métricas do grupo
            if 'nf3e_metricas' in dossie and not dossie['nf3e_metricas'].empty:
                metricas = dossie['nf3e_metricas'].iloc[0]
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Empresas Consumidoras", int(metricas.get('qt_empresas_consumidoras', 0)))
                with col2:
                    st.metric("Valor Total Energia", formatar_moeda(metricas.get('vl_energia_grupo', 0)))
                with col3:
                    st.metric("Qtd. Notas", int(metricas.get('qt_notas_grupo', 0)))

                st.divider()

            # Resumo por CNPJ
            st.write("**Consumo de Energia por CNPJ (acumulado 12 meses):**")

            # Pegar o último valor disponível
            meses_cols = ['set2025', 'ago2025', 'jul2025', 'jun2025', 'mai2025', 'abr2025',
                         'mar2025', 'fev2025', 'jan2025', 'dez2024', 'nov2024', 'out2024']

            def get_ultimo_valor_energia(row):
                for mes in meses_cols:
                    if mes in row and pd.notna(row[mes]) and row[mes] > 0:
                        return row[mes]
                return 0

            df_nf3e['ultimo_valor_12m'] = df_nf3e.apply(get_ultimo_valor_energia, axis=1)

            # Resumo
            df_resumo_energia = df_nf3e[['cnpj', 'ultimo_valor_12m']].copy()
            df_resumo_energia.columns = ['CNPJ', 'Energia 12m (R$)']
            df_resumo_energia['Energia Formatada'] = df_resumo_energia['Energia 12m (R$)'].apply(formatar_moeda)

            total_energia = df_resumo_energia['Energia 12m (R$)'].sum()

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total CNPJs com Energia", len(df_resumo_energia))
            with col2:
                st.metric("Consumo Total (soma)", formatar_moeda(total_energia))

            st.dataframe(
                df_resumo_energia.sort_values('Energia 12m (R$)', ascending=False)[['CNPJ', 'Energia Formatada']],
                hide_index=True,
                use_container_width=True
            )

            # Gráfico de evolução
            st.divider()
            st.write("**Evolução do Consumo de Energia:**")
            try:
                meses_disponiveis = [m for m in meses_cols if m in df_nf3e.columns]
                df_chart = df_nf3e.melt(
                    id_vars=['cnpj'],
                    value_vars=meses_disponiveis,
                    var_name='periodo',
                    value_name='consumo'
                )
                df_chart = df_chart[df_chart['consumo'].notna() & (df_chart['consumo'] > 0)]

                if not df_chart.empty:
                    ordem_meses = {'jan2024': 1, 'fev2024': 2, 'mar2024': 3, 'abr2024': 4, 'mai2024': 5, 'jun2024': 6,
                                  'jul2024': 7, 'ago2024': 8, 'set2024': 9, 'out2024': 10, 'nov2024': 11, 'dez2024': 12,
                                  'jan2025': 13, 'fev2025': 14, 'mar2025': 15, 'abr2025': 16, 'mai2025': 17, 'jun2025': 18,
                                  'jul2025': 19, 'ago2025': 20, 'set2025': 21}
                    df_chart['ordem'] = df_chart['periodo'].map(ordem_meses)
                    df_chart = df_chart.sort_values('ordem')

                    # Gráfico de total do grupo
                    df_total = df_chart.groupby('periodo').agg({
                        'consumo': 'sum',
                        'ordem': 'first'
                    }).reset_index().sort_values('ordem')

                    fig = px.line(
                        df_total,
                        x='periodo',
                        y='consumo',
                        title="Consumo Total de Energia do Grupo (acumulado 12 meses)",
                        labels={'consumo': 'Valor (R$)', 'periodo': 'Período'},
                        markers=True
                    )
                    fig.update_traces(line=dict(width=3, color='#f9a825'), marker=dict(size=10))
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Sem dados suficientes para gerar o gráfico.")
            except Exception as e:
                st.warning(f"Não foi possível gerar o gráfico: {e}")

            # Detalhamento mensal
            if 'nf3e_detalhado' in dossie and not dossie['nf3e_detalhado'].empty:
                st.divider()
                st.write("**Detalhamento Mensal:**")
                df_det = dossie['nf3e_detalhado'].copy()
                df_det['Energia Mensal'] = df_det['vl_energia_mensal'].apply(formatar_moeda)
                st.dataframe(
                    df_det[['cnpj', 'ano_emissao', 'mes_emissao', 'Energia Mensal', 'qt_notas', 'qt_fornecedores']].rename(
                        columns={'cnpj': 'CNPJ', 'ano_emissao': 'Ano', 'mes_emissao': 'Mês', 'qt_notas': 'Qtd. Notas', 'qt_fornecedores': 'Fornecedores'}
                    ),
                    hide_index=True,
                    use_container_width=True
                )
        else:
            st.info("Nenhum dado de consumo de energia elétrica (NF3e) disponível para este grupo.")

    # =========================================================================
    # TAB 12: TELECOMUNICAÇÕES (NFCom)
    # =========================================================================
    with tab12:
        st.subheader("📱 Consumo de Telecomunicações")

        # Verificar se há dados de NFCom
        if 'nfcom' in dossie and not dossie['nfcom'].empty:
            df_nfcom = dossie['nfcom'].copy()

            # Métricas do grupo
            if 'nfcom_metricas' in dossie and not dossie['nfcom_metricas'].empty:
                metricas = dossie['nfcom_metricas'].iloc[0]
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Empresas Consumidoras", int(metricas.get('qt_empresas_consumidoras', 0)))
                with col2:
                    st.metric("Valor Total Telecom", formatar_moeda(metricas.get('vl_telecom_grupo', 0)))
                with col3:
                    st.metric("Qtd. Notas", int(metricas.get('qt_notas_grupo', 0)))

                st.divider()

            # Sub-tabs
            sub_tab1, sub_tab2, sub_tab3 = st.tabs(["Por CNPJ", "Por Operadora", "Detalhamento"])

            with sub_tab1:
                st.write("**Consumo de Telecomunicações por CNPJ (acumulado 12 meses):**")

                # Pegar o último valor disponível
                meses_cols = ['set2025', 'ago2025', 'jul2025', 'jun2025', 'mai2025', 'abr2025',
                             'mar2025', 'fev2025', 'jan2025', 'dez2024', 'nov2024', 'out2024']

                def get_ultimo_valor_telecom(row):
                    for mes in meses_cols:
                        if mes in row and pd.notna(row[mes]) and row[mes] > 0:
                            return row[mes]
                    return 0

                df_nfcom['ultimo_valor_12m'] = df_nfcom.apply(get_ultimo_valor_telecom, axis=1)

                # Resumo
                df_resumo_telecom = df_nfcom[['cnpj', 'ultimo_valor_12m']].copy()
                df_resumo_telecom.columns = ['CNPJ', 'Telecom 12m (R$)']
                df_resumo_telecom['Telecom Formatada'] = df_resumo_telecom['Telecom 12m (R$)'].apply(formatar_moeda)

                total_telecom = df_resumo_telecom['Telecom 12m (R$)'].sum()

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total CNPJs com Telecom", len(df_resumo_telecom))
                with col2:
                    st.metric("Consumo Total (soma)", formatar_moeda(total_telecom))

                st.dataframe(
                    df_resumo_telecom.sort_values('Telecom 12m (R$)', ascending=False)[['CNPJ', 'Telecom Formatada']],
                    hide_index=True,
                    use_container_width=True
                )

                # Gráfico de evolução
                st.divider()
                st.write("**Evolução do Consumo de Telecomunicações:**")
                try:
                    meses_disponiveis = [m for m in meses_cols if m in df_nfcom.columns]
                    df_chart = df_nfcom.melt(
                        id_vars=['cnpj'],
                        value_vars=meses_disponiveis,
                        var_name='periodo',
                        value_name='consumo'
                    )
                    df_chart = df_chart[df_chart['consumo'].notna() & (df_chart['consumo'] > 0)]

                    if not df_chart.empty:
                        ordem_meses = {'jan2024': 1, 'fev2024': 2, 'mar2024': 3, 'abr2024': 4, 'mai2024': 5, 'jun2024': 6,
                                      'jul2024': 7, 'ago2024': 8, 'set2024': 9, 'out2024': 10, 'nov2024': 11, 'dez2024': 12,
                                      'jan2025': 13, 'fev2025': 14, 'mar2025': 15, 'abr2025': 16, 'mai2025': 17, 'jun2025': 18,
                                      'jul2025': 19, 'ago2025': 20, 'set2025': 21}
                        df_chart['ordem'] = df_chart['periodo'].map(ordem_meses)
                        df_chart = df_chart.sort_values('ordem')

                        # Gráfico de total do grupo
                        df_total = df_chart.groupby('periodo').agg({
                            'consumo': 'sum',
                            'ordem': 'first'
                        }).reset_index().sort_values('ordem')

                        fig = px.line(
                            df_total,
                            x='periodo',
                            y='consumo',
                            title="Consumo Total de Telecomunicações do Grupo (acumulado 12 meses)",
                            labels={'consumo': 'Valor (R$)', 'periodo': 'Período'},
                            markers=True
                        )
                        fig.update_traces(line=dict(width=3, color='#2196f3'), marker=dict(size=10))
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Sem dados suficientes para gerar o gráfico.")
                except Exception as e:
                    st.warning(f"Não foi possível gerar o gráfico: {e}")

            with sub_tab2:
                # Análise por operadora
                if 'nfcom_operadoras' in dossie and not dossie['nfcom_operadoras'].empty:
                    st.write("**Consumo por Operadora de Telecomunicações:**")
                    df_op = dossie['nfcom_operadoras'].copy()
                    df_op['Valor Total'] = df_op['vl_total'].apply(formatar_moeda)
                    st.dataframe(
                        df_op[['cnpj_operadora', 'nome_operadora', 'qt_empresas_clientes', 'Valor Total', 'qt_notas']].rename(
                            columns={
                                'cnpj_operadora': 'CNPJ Operadora',
                                'nome_operadora': 'Nome Operadora',
                                'qt_empresas_clientes': 'Empresas Clientes',
                                'qt_notas': 'Qtd. Notas'
                            }
                        ),
                        hide_index=True,
                        use_container_width=True
                    )

                    # Gráfico de pizza por operadora
                    try:
                        fig_pie = px.pie(
                            df_op,
                            values='vl_total',
                            names='nome_operadora',
                            title="Distribuição do Consumo por Operadora"
                        )
                        st.plotly_chart(fig_pie, use_container_width=True)
                    except Exception as e:
                        st.warning(f"Não foi possível gerar o gráfico: {e}")
                else:
                    st.info("Nenhum dado de operadoras disponível.")

            with sub_tab3:
                # Detalhamento mensal
                if 'nfcom_detalhado' in dossie and not dossie['nfcom_detalhado'].empty:
                    st.write("**Detalhamento Mensal:**")
                    df_det = dossie['nfcom_detalhado'].copy()
                    df_det['Telecom Mensal'] = df_det['vl_telecom_mensal'].apply(formatar_moeda)
                    st.dataframe(
                        df_det[['cnpj', 'ano_emissao', 'mes_emissao', 'Telecom Mensal', 'qt_notas', 'qt_operadoras']].rename(
                            columns={'cnpj': 'CNPJ', 'ano_emissao': 'Ano', 'mes_emissao': 'Mês', 'qt_notas': 'Qtd. Notas', 'qt_operadoras': 'Operadoras'}
                        ),
                        hide_index=True,
                        use_container_width=True
                    )
                else:
                    st.info("Nenhum detalhamento mensal disponível.")
        else:
            st.info("Nenhum dado de consumo de telecomunicações (NFCom) disponível para este grupo.")

    # =========================================================================
    # TAB 13: ANÁLISE DE SIMILARIDADE - EVIDÊNCIAS DE GRUPO ECONÔMICO
    # =========================================================================
    with tab13:
        st.subheader("🔍 Análise de Similaridade - Evidências de Grupo Econômico")

        st.info("""
        Esta análise verifica se os CNPJs do grupo compartilham informações que indicam
        formação de grupo econômico, conforme metodologia do Sistema GEI.
        """)

        # Inicializar variáveis de controle
        evidencias = {}
        score_similaridade = 0
        max_score_possivel = 0
        cnpjs_grupo = dossie['cnpjs']['cnpj'].tolist() if not dossie['cnpjs'].empty else []

        if len(cnpjs_grupo) < 2:
            st.warning("O grupo precisa ter pelo menos 2 CNPJs para análise de similaridade.")
        else:
            # Criar abas para cada tipo de análise
            tabs_similaridade = st.tabs([
                "📋 Cadastro",
                "👥 Sócios",
                "📊 Receitas",
                "📄 Notas Fiscais",
                "📱 Convênio 115",
                "🏦 Contas Bancárias",
                "👔 Funcionários",
                "💳 Pagamentos",
                "📊 Score Final"
            ])

            # ===================================================================
            # TAB 1: ANÁLISE DE DADOS CADASTRAIS
            # ===================================================================
            with tabs_similaridade[0]:
                st.subheader("Consistência Cadastral")

                if not dossie['cnpjs'].empty and len(dossie['cnpjs']) > 1:
                    cadastro_checks = []

                    # Razão Social
                    max_score_possivel += 2
                    if 'nm_razao_social' in dossie['cnpjs'].columns:
                        razoes = dossie['cnpjs']['nm_razao_social'].dropna().unique()
                        if len(razoes) == 1:
                            cadastro_checks.append({
                                'Atributo': 'Razão Social',
                                'Status': '✅ IDÊNTICA',
                                'Quantidade': '1',
                                'Pontos': 2,
                                'Avaliação': 'CRÍTICO - Forte indício'
                            })
                            evidencias['razao_social'] = True
                            score_similaridade += 2
                        elif len(razoes) > 1:
                            cadastro_checks.append({
                                'Atributo': 'Razão Social',
                                'Status': '❌ DIFERENTES',
                                'Quantidade': str(len(razoes)),
                                'Pontos': 0,
                                'Avaliação': '-'
                            })

                    # Nome Fantasia
                    max_score_possivel += 1
                    if 'nm_fantasia' in dossie['cnpjs'].columns:
                        fantasias = dossie['cnpjs']['nm_fantasia'].dropna().unique()
                        if len(fantasias) == 1 and len(str(fantasias[0])) > 0:
                            cadastro_checks.append({
                                'Atributo': 'Nome Fantasia',
                                'Status': '✅ IDÊNTICO',
                                'Quantidade': '1',
                                'Pontos': 1,
                                'Avaliação': 'Alto indício'
                            })
                            evidencias['fantasia'] = True
                            score_similaridade += 1
                        elif len(fantasias) > 1:
                            cadastro_checks.append({
                                'Atributo': 'Nome Fantasia',
                                'Status': '❌ DIFERENTES',
                                'Quantidade': str(len(fantasias)),
                                'Pontos': 0,
                                'Avaliação': '-'
                            })

                    # CNAE
                    max_score_possivel += 1
                    if 'cd_cnae' in dossie['cnpjs'].columns:
                        cnaes = dossie['cnpjs']['cd_cnae'].dropna().unique()
                        if len(cnaes) == 1:
                            cadastro_checks.append({
                                'Atributo': 'CNAE',
                                'Status': '✅ IDÊNTICO',
                                'Quantidade': '1',
                                'Pontos': 1,
                                'Avaliação': 'Mesmo ramo'
                            })
                            evidencias['cnae'] = True
                            score_similaridade += 1
                        elif len(cnaes) > 1:
                            cadastro_checks.append({
                                'Atributo': 'CNAE',
                                'Status': '❌ DIFERENTES',
                                'Quantidade': str(len(cnaes)),
                                'Pontos': 0,
                                'Avaliação': '-'
                            })

                    # Contador
                    max_score_possivel += 2
                    if 'nm_contador' in dossie['cnpjs'].columns:
                        contadores = dossie['cnpjs']['nm_contador'].dropna().unique()
                        if len(contadores) == 1 and len(str(contadores[0])) > 0:
                            cadastro_checks.append({
                                'Atributo': 'Contador',
                                'Status': '✅ MESMO',
                                'Quantidade': '1',
                                'Pontos': 2,
                                'Avaliação': 'CRÍTICO - Gestão comum'
                            })
                            evidencias['contador'] = True
                            score_similaridade += 2
                        elif len(contadores) > 1:
                            cadastro_checks.append({
                                'Atributo': 'Contador',
                                'Status': '❌ DIFERENTES',
                                'Quantidade': str(len(contadores)),
                                'Pontos': 0,
                                'Avaliação': '-'
                            })

                    # Município
                    max_score_possivel += 0.5
                    if 'nm_municipio' in dossie['cnpjs'].columns:
                        municipios = dossie['cnpjs']['nm_municipio'].dropna().unique()
                        if len(municipios) == 1:
                            cadastro_checks.append({
                                'Atributo': 'Município',
                                'Status': '✅ MESMO',
                                'Quantidade': '1',
                                'Pontos': 0.5,
                                'Avaliação': 'Indício leve'
                            })
                            score_similaridade += 0.5
                        elif len(municipios) > 1:
                            cadastro_checks.append({
                                'Atributo': 'Município',
                                'Status': '❌ DIFERENTES',
                                'Quantidade': str(len(municipios)),
                                'Pontos': 0,
                                'Avaliação': '-'
                            })

                    # Regime de Apuração
                    max_score_possivel += 1
                    if 'nm_reg_apuracao' in dossie['cnpjs'].columns:
                        regimes = dossie['cnpjs']['nm_reg_apuracao'].dropna().unique()
                        if len(regimes) == 1:
                            cadastro_checks.append({
                                'Atributo': 'Regime Tributário',
                                'Status': '✅ MESMO',
                                'Quantidade': str(regimes[0]),
                                'Pontos': 1,
                                'Avaliação': 'Mesmo regime'
                            })
                            score_similaridade += 1
                        elif len(regimes) > 1:
                            cadastro_checks.append({
                                'Atributo': 'Regime Tributário',
                                'Status': '⚠️ MISTO',
                                'Quantidade': str(len(regimes)),
                                'Pontos': 0,
                                'Avaliação': 'Possível planejamento'
                            })

                    if cadastro_checks:
                        df_cadastro = pd.DataFrame(cadastro_checks)
                        st.dataframe(df_cadastro, width='stretch', hide_index=True)

                        pontos_cadastro = df_cadastro['Pontos'].sum()
                        if pontos_cadastro >= 5:
                            st.error(f"🔴 CRÍTICO: {pontos_cadastro:.1f} pontos - Forte evidência de grupo econômico")
                        elif pontos_cadastro >= 3:
                            st.warning(f"🟡 ALTO: {pontos_cadastro:.1f} pontos - Evidência significativa")
                        elif pontos_cadastro >= 1:
                            st.info(f"🟠 MODERADO: {pontos_cadastro:.1f} pontos")
                        else:
                            st.success(f"🟢 BAIXO: {pontos_cadastro:.1f} pontos")
                else:
                    st.warning("Dados cadastrais insuficientes para análise")

            # ===================================================================
            # TAB 2: ANÁLISE DE VÍNCULOS SOCIETÁRIOS
            # ===================================================================
            with tabs_similaridade[1]:
                st.subheader("Análise de Vínculos Societários")

                if not dossie['socios'].empty:
                    socios_checks = []

                    # Sócios compartilhados (já calculados no dossiê)
                    max_score_possivel += 5
                    total_socios = len(dossie['socios'])

                    if total_socios > 0:
                        pontos_socios = min(total_socios * 2, 5)

                        socios_checks.append({
                            'Indicador': 'Sócios Compartilhados',
                            'Quantidade': str(total_socios),
                            'Status': '✅ DETECTADOS',
                            'Pontos': str(pontos_socios),
                            'Avaliação': 'CRÍTICO - Vínculos cruzados'
                        })

                        evidencias['socios_compartilhados'] = True
                        score_similaridade += pontos_socios

                        # Detalhar os sócios compartilhados
                        st.write("**Sócios que participam de múltiplos CNPJs:**")
                        for _, row in dossie['socios'].iterrows():
                            cpf = row.get('cpf_socio', 'N/A')
                            qtd = row.get('qtd_empresas', 0)
                            st.write(f"• **CPF {cpf}**: Presente em {qtd} empresas do grupo")
                    else:
                        socios_checks.append({
                            'Indicador': 'Sócios Compartilhados',
                            'Quantidade': '0',
                            'Status': '❌ NÃO DETECTADO',
                            'Pontos': '0',
                            'Avaliação': '-'
                        })

                    df_socios = pd.DataFrame(socios_checks)
                    for col in df_socios.columns:
                        df_socios[col] = df_socios[col].astype(str)

                    st.dataframe(df_socios, hide_index=True)

                    # Calcular pontos
                    pontos_numericos = df_socios[df_socios['Pontos'] != '-']['Pontos'].astype(float)
                    pontos_socios_total = pontos_numericos.sum() if len(pontos_numericos) > 0 else 0

                    if pontos_socios_total >= 4:
                        st.error(f"🔴 CRÍTICO: {pontos_socios_total:.1f} pontos - Controle societário compartilhado")
                    elif pontos_socios_total >= 2:
                        st.warning(f"🟡 ALTO: {pontos_socios_total:.1f} pontos")
                    else:
                        st.info(f"🟢 BAIXO: {pontos_socios_total:.1f} pontos")
                else:
                    st.warning("Dados de vínculos societários insuficientes")

            # ===================================================================
            # TAB 3: ANÁLISE DE RECEITAS (PGDAS + DIME)
            # ===================================================================
            with tabs_similaridade[2]:
                st.subheader("Análise de Faturamento - PGDAS / DIME")

                if 'faturamento' in dossie and not dossie['faturamento'].empty:
                    df_fat = dossie['faturamento'].copy()
                    receitas_checks = []

                    # Informação sobre fontes de dados
                    fontes_disponiveis = df_fat['fonte'].unique().tolist() if 'fonte' in df_fat.columns else ['PGDAS']
                    st.info(f"**Fontes de dados utilizadas:** {', '.join(fontes_disponiveis)}")

                    # Métricas por fonte
                    col_f1, col_f2 = st.columns(2)
                    with col_f1:
                        cnpjs_pgdas = len(df_fat[df_fat['fonte'] == 'PGDAS']) if 'PGDAS' in fontes_disponiveis else 0
                        st.metric("CNPJs com PGDAS (Simples)", cnpjs_pgdas)
                    with col_f2:
                        cnpjs_dime = len(df_fat[df_fat['fonte'] == 'DIME']) if 'DIME' in fontes_disponiveis else 0
                        st.metric("CNPJs com DIME (Normal)", cnpjs_dime)

                    # Calcular receita máxima por CNPJ
                    meses_cols = ['set2025', 'ago2025', 'jul2025', 'jun2025', 'mai2025', 'abr2025', 'mar2025', 'fev2025', 'jan2025']
                    meses_disponiveis = [m for m in meses_cols if m in df_fat.columns]

                    if meses_disponiveis:
                        # Pegar o último valor não-zero para cada CNPJ
                        def get_ultimo_valor(row):
                            for mes in meses_disponiveis:
                                if mes in row and pd.notna(row[mes]) and row[mes] > 0:
                                    return row[mes]
                            return 0

                        df_fat['receita_max'] = df_fat.apply(get_ultimo_valor, axis=1)
                        receitas_por_cnpj = df_fat.groupby('cnpj')['receita_max'].max()
                        receita_total_grupo = receitas_por_cnpj.sum()
                        receita_media = receitas_por_cnpj.mean() if len(receitas_por_cnpj) > 0 else 0

                        # Receita somada ultrapassa limite
                        max_score_possivel += 5
                        if receita_total_grupo > 4800000:
                            excesso = receita_total_grupo - 4800000
                            pontos_receita = 5
                            receitas_checks.append({
                                'Indicador': 'Receita Total do Grupo',
                                'Valor': formatar_moeda(receita_total_grupo),
                                'Status': '🔴 ACIMA DO LIMITE',
                                'Excesso': formatar_moeda(excesso),
                                'Pontos': str(pontos_receita),
                                'Avaliação': 'CRÍTICO - Fracionamento'
                            })
                            evidencias['receita_excesso'] = True
                            score_similaridade += pontos_receita

                            st.error(f"""
                            **🔴 ALERTA CRÍTICO - LIMITE ULTRAPASSADO**

                            Receita somada (PGDAS + DIME): **{formatar_moeda(receita_total_grupo)}**

                            Excesso: **{formatar_moeda(excesso)}** ({((excesso/4800000)*100):.1f}% acima do limite)
                            """)
                        else:
                            receitas_checks.append({
                                'Indicador': 'Receita Total do Grupo',
                                'Valor': formatar_moeda(receita_total_grupo),
                                'Status': '✅ DENTRO DO LIMITE',
                                'Excesso': '-',
                                'Pontos': '0',
                                'Avaliação': '-'
                            })

                        # Distribuição equilibrada
                        max_score_possivel += 2
                        if len(receitas_por_cnpj) > 1:
                            desvio_padrao = receitas_por_cnpj.std()
                            coef_variacao = (desvio_padrao / receita_media) if receita_media > 0 else 0

                            if coef_variacao < 0.3:
                                receitas_checks.append({
                                    'Indicador': 'Distribuição de Receitas',
                                    'Valor': f"CV: {coef_variacao:.2f}",
                                    'Status': '⚠️ MUITO UNIFORME',
                                    'Excesso': '-',
                                    'Pontos': '2',
                                    'Avaliação': 'Possível divisão planejada'
                                })
                                evidencias['receita_uniforme'] = True
                                score_similaridade += 2
                            else:
                                receitas_checks.append({
                                    'Indicador': 'Distribuição de Receitas',
                                    'Valor': f"CV: {coef_variacao:.2f}",
                                    'Status': '✅ VARIADA',
                                    'Excesso': '-',
                                    'Pontos': '0',
                                    'Avaliação': '-'
                                })

                        # Análise de regimes mistos
                        if 'fonte' in df_fat.columns and len(df_fat['fonte'].unique()) > 1:
                            receitas_checks.append({
                                'Indicador': 'Regimes Tributários',
                                'Valor': f"{len(df_fat['fonte'].unique())} regimes",
                                'Status': '⚠️ MISTO',
                                'Excesso': '-',
                                'Pontos': '1',
                                'Avaliação': 'Possível planejamento tributário'
                            })
                            score_similaridade += 1

                        if receitas_checks:
                            df_receitas = pd.DataFrame(receitas_checks)
                            for col in df_receitas.columns:
                                df_receitas[col] = df_receitas[col].astype(str)
                            st.dataframe(df_receitas, hide_index=True)

                        # Gráfico de distribuição
                        st.write("**Distribuição de Receitas por CNPJ:**")
                        df_bar = df_fat.groupby(['cnpj', 'fonte'])['receita_max'].max().reset_index()

                        fig1 = px.bar(
                            df_bar,
                            x='cnpj',
                            y='receita_max',
                            color='fonte' if 'fonte' in df_bar.columns else None,
                            labels={'cnpj': 'CNPJ', 'receita_max': 'Receita (R$)', 'fonte': 'Fonte'},
                            title="Receita Máxima por CNPJ e Fonte",
                            template=filtros['tema'],
                            barmode='group'
                        )
                        fig1.add_hline(y=4800000, line_dash="dash", line_color="red",
                                     annotation_text="Limite SN")
                        st.plotly_chart(fig1, use_container_width=True)
                else:
                    st.warning("Dados de receitas insuficientes (PGDAS ou DIME)")

            # ===================================================================
            # TAB 4: ANÁLISE DE NOTAS FISCAIS
            # ===================================================================
            with tabs_similaridade[3]:
                st.subheader("Compartilhamento em Notas Fiscais")

                if not dossie['inconsistencias'].empty:
                    nfe_checks = []
                    df_nfe = dossie['inconsistencias']

                    # Verificar inconsistências detectadas
                    tipos_incons = {
                        'ip_transmissao_incons': ('IPs de Transmissão', 3),
                        'cliente_incons': ('Clientes Comuns', 2),
                        'fornecedor_incons': ('Fornecedores Comuns', 2),
                        'codigo_produto_incons': ('Códigos de Produto', 1),
                        'descricao_produto_incons': ('Descrições de Produto', 1),
                        'tel_emit_incons': ('Telefones Emitente', 2),
                        'email_incons': ('E-mails Destinatário', 1),
                        'end_emit_incons': ('Endereço de Emissão', 2),
                        'end_dest_incons': ('Endereço de Destino', 2)
                    }

                    for campo, (label, pontos_max) in tipos_incons.items():
                        if campo in df_nfe.columns:
                            max_score_possivel += pontos_max
                            qtd_incons = len(df_nfe[df_nfe[campo] == 'S'])

                            if qtd_incons > 0:
                                pontos = min(qtd_incons / 10, pontos_max)
                                nfe_checks.append({
                                    'Indicador': label,
                                    'Quantidade': qtd_incons,
                                    'Status': '✅ DETECTADOS',
                                    'Pontos': round(pontos, 1),
                                    'Avaliação': 'Compartilhamento detectado'
                                })
                                evidencias[campo] = True
                                score_similaridade += pontos
                            else:
                                nfe_checks.append({
                                    'Indicador': label,
                                    'Quantidade': 0,
                                    'Status': '❌ NÃO DETECTADOS',
                                    'Pontos': 0,
                                    'Avaliação': '-'
                                })

                    if nfe_checks:
                        df_nfe_check = pd.DataFrame(nfe_checks)
                        st.dataframe(df_nfe_check, width='stretch', hide_index=True)

                        pontos_nfe = df_nfe_check['Pontos'].sum()
                        if pontos_nfe >= 5:
                            st.error(f"🔴 CRÍTICO: {pontos_nfe:.1f} pontos - Operações fortemente interligadas")
                        elif pontos_nfe >= 3:
                            st.warning(f"🟡 ALTO: {pontos_nfe:.1f} pontos")
                        else:
                            st.info(f"🟢 MODERADO: {pontos_nfe:.1f} pontos")
                else:
                    st.warning("Dados de notas fiscais insuficientes")

            # ===================================================================
            # TAB 5: ANÁLISE DE CONVÊNIO 115
            # ===================================================================
            with tabs_similaridade[4]:
                st.subheader("Análise Convênio 115 - Identificadores Compartilhados")

                if not dossie['c115'].empty:
                    c115_checks = []
                    info_c115 = dossie['c115'].iloc[0]

                    # Verificar dados do C115
                    max_score_possivel += 3
                    total_compartilhamentos = info_c115.get('total_compartilhamentos', 0)

                    if pd.notna(total_compartilhamentos) and total_compartilhamentos > 0:
                        pontos_c115 = min(total_compartilhamentos / 5, 3)
                        c115_checks.append({
                            'Indicador': 'Compartilhamentos C115',
                            'Quantidade': int(total_compartilhamentos),
                            'Status': '✅ DETECTADOS',
                            'Pontos': round(pontos_c115, 1),
                            'Avaliação': 'CRÍTICO - Identificadores compartilhados'
                        })
                        evidencias['c115_compartilhamento'] = True
                        score_similaridade += pontos_c115
                    else:
                        c115_checks.append({
                            'Indicador': 'Compartilhamentos C115',
                            'Quantidade': 0,
                            'Status': '❌ NÃO DETECTADOS',
                            'Pontos': 0,
                            'Avaliação': '-'
                        })

                    # Nível de risco C115
                    nivel_risco = info_c115.get('nivel_risco_grupo_economico', 'N/A')
                    indice_risco = info_c115.get('indice_risco_grupo_economico', 0)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Nível de Risco C115", str(nivel_risco))
                    with col2:
                        st.metric("Índice de Risco", f"{indice_risco:.4f}" if pd.notna(indice_risco) else "N/A")

                    if c115_checks:
                        df_c115 = pd.DataFrame(c115_checks)
                        st.dataframe(df_c115, width='stretch', hide_index=True)
                else:
                    st.warning("Dados do Convênio 115 insuficientes")

            # ===================================================================
            # TAB 6: ANÁLISE DE CONTAS BANCÁRIAS (CCS)
            # ===================================================================
            with tabs_similaridade[5]:
                st.subheader("Análise de Contas Bancárias - CCS")

                if not dossie['ccs_compartilhadas'].empty:
                    ccs_checks = []

                    # CPFs compartilhando acesso a contas
                    max_score_possivel += 4
                    total_cpfs = len(dossie['ccs_compartilhadas'])

                    if total_cpfs > 0:
                        pontos_ccs = min(total_cpfs * 2, 4)
                        ccs_checks.append({
                            'Indicador': 'CPFs com Múltiplas Contas',
                            'Quantidade': total_cpfs,
                            'Status': '✅ DETECTADOS',
                            'Pontos': pontos_ccs,
                            'Avaliação': 'CRÍTICO - Gestão financeira comum'
                        })
                        evidencias['ccs_cpf_compartilhado'] = True
                        score_similaridade += pontos_ccs

                        st.write("**CPFs com Acesso a Múltiplas Contas:**")
                        for _, row in dossie['ccs_compartilhadas'].head(10).iterrows():
                            cpf = row.get('nr_cpf', 'N/A')
                            qtd = row.get('qtd_cnpjs_usando_conta', 0)
                            banco = row.get('nm_banco', 'N/A')
                            st.write(f"• CPF {cpf}: {qtd} CNPJs - Banco: {banco}")
                    else:
                        ccs_checks.append({
                            'Indicador': 'CPFs com Múltiplas Contas',
                            'Quantidade': 0,
                            'Status': '❌ NÃO DETECTADOS',
                            'Pontos': 0,
                            'Avaliação': '-'
                        })

                    # Sobreposições de responsáveis
                    max_score_possivel += 2
                    if not dossie['ccs_sobreposicoes'].empty:
                        total_sobreposicoes = len(dossie['ccs_sobreposicoes'])
                        pontos_sob = min(total_sobreposicoes, 2)
                        ccs_checks.append({
                            'Indicador': 'Sobreposições de Responsáveis',
                            'Quantidade': total_sobreposicoes,
                            'Status': '✅ DETECTADOS',
                            'Pontos': pontos_sob,
                            'Avaliação': 'Gestão simultânea'
                        })
                        score_similaridade += pontos_sob

                    if ccs_checks:
                        df_ccs = pd.DataFrame(ccs_checks)
                        st.dataframe(df_ccs, width='stretch', hide_index=True)

                        pontos_ccs_total = df_ccs['Pontos'].sum()
                        if pontos_ccs_total >= 4:
                            st.error(f"🔴 CRÍTICO: {pontos_ccs_total:.1f} pontos - Contas fortemente relacionadas")
                        elif pontos_ccs_total >= 2:
                            st.warning(f"🟡 ALTO: {pontos_ccs_total:.1f} pontos")
                        else:
                            st.info(f"🟢 BAIXO: {pontos_ccs_total:.1f} pontos")
                else:
                    st.warning("Dados de contas bancárias insuficientes")

            # ===================================================================
            # TAB 7: ANÁLISE DE FUNCIONÁRIOS
            # ===================================================================
            with tabs_similaridade[6]:
                st.subheader("Análise de Funcionários - RAIS/CAGED")

                if not dossie['funcionarios'].empty:
                    func_checks = []
                    info_func = dossie['funcionarios'].iloc[0]

                    total_funcionarios = info_func.get('total_funcionarios', 0)
                    cnpjs_com_func = info_func.get('cnpjs_com_funcionarios', 0)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total de Funcionários", int(total_funcionarios) if pd.notna(total_funcionarios) else 0)
                    with col2:
                        st.metric("CNPJs com Funcionários", int(cnpjs_com_func) if pd.notna(cnpjs_com_func) else 0)

                    # Verificar proporção receita vs funcionários
                    max_score_possivel += 3
                    if not dossie['principal'].empty and pd.notna(total_funcionarios) and total_funcionarios > 0:
                        info_principal = dossie['principal'].iloc[0]
                        receita_max = info_principal.get('valor_max', 0)

                        if pd.notna(receita_max) and receita_max > 0:
                            receita_por_func = receita_max / (total_funcionarios + 1)

                            if receita_por_func > 500000:
                                func_checks.append({
                                    'Indicador': 'Receita por Funcionário',
                                    'Valor': formatar_moeda(receita_por_func),
                                    'Status': '⚠️ DESPROPORCIONAL',
                                    'Pontos': 2,
                                    'Avaliação': 'Possível terceirização'
                                })
                                score_similaridade += 2
                            else:
                                func_checks.append({
                                    'Indicador': 'Receita por Funcionário',
                                    'Valor': formatar_moeda(receita_por_func),
                                    'Status': '✅ PROPORCIONAL',
                                    'Pontos': 0,
                                    'Avaliação': '-'
                                })

                    if func_checks:
                        df_func = pd.DataFrame(func_checks)
                        st.dataframe(df_func, width='stretch', hide_index=True)
                    else:
                        st.success("✅ Proporção receita/funcionários dentro do esperado")
                else:
                    st.warning("Dados de funcionários insuficientes")

            # ===================================================================
            # TAB 8: ANÁLISE DE MEIOS DE PAGAMENTO
            # ===================================================================
            with tabs_similaridade[7]:
                st.subheader("Análise de Meios de Pagamento")

                if not dossie['pagamentos'].empty:
                    pag_checks = []
                    info_pag = dossie['pagamentos'].iloc[0]

                    valor_empresas = info_pag.get('valor_meios_pagamento_empresas', 0)
                    valor_socios = info_pag.get('valor_meios_pagamento_socios', 0)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Pagamentos Empresas", formatar_moeda(valor_empresas) if pd.notna(valor_empresas) else "R$ 0,00")
                    with col2:
                        st.metric("Pagamentos Sócios", formatar_moeda(valor_socios) if pd.notna(valor_socios) else "R$ 0,00")

                    # Verificar se sócios têm meios de pagamento
                    max_score_possivel += 2
                    if pd.notna(valor_socios) and valor_socios > 0:
                        pag_checks.append({
                            'Indicador': 'Sócios com Meios Pagamento',
                            'Valor': formatar_moeda(valor_socios),
                            'Status': '✅ DETECTADOS',
                            'Pontos': 2,
                            'Avaliação': 'Gestão financeira comum'
                        })
                        evidencias['socios_meios_pagamento'] = True
                        score_similaridade += 2
                    else:
                        pag_checks.append({
                            'Indicador': 'Sócios com Meios Pagamento',
                            'Valor': 'R$ 0,00',
                            'Status': '❌ NÃO DETECTADOS',
                            'Pontos': 0,
                            'Avaliação': '-'
                        })

                    if pag_checks:
                        df_pag = pd.DataFrame(pag_checks)
                        st.dataframe(df_pag, width='stretch', hide_index=True)
                else:
                    st.warning("Dados de meios de pagamento insuficientes")

            # ===================================================================
            # TAB 9: SCORE FINAL E CONCLUSÃO
            # ===================================================================
            with tabs_similaridade[8]:
                st.subheader("📊 Score Final de Similaridade")

                # Métricas principais
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Score Total", f"{score_similaridade:.1f}",
                             help="Pontuação total baseada em todas as evidências")

                with col2:
                    st.metric("Score Máximo Possível", f"{max_score_possivel:.1f}",
                             help="Pontuação máxima com base nos dados disponíveis")

                with col3:
                    percentual = (score_similaridade / max_score_possivel * 100) if max_score_possivel > 0 else 0
                    st.metric("Percentual", f"{percentual:.1f}%",
                             help="Percentual do score em relação ao máximo")

                with col4:
                    total_evidencias = len([v for v in evidencias.values() if v])
                    st.metric("Evidências", total_evidencias,
                             help="Número de evidências positivas encontradas")

                # Determinação do nível de risco
                st.divider()

                if score_similaridade >= 15:
                    nivel_risco = "🔴 CRÍTICO"
                    cor_risco = "error"
                    conclusao = """
                    **FORTE EVIDÊNCIA DE GRUPO ECONÔMICO**

                    Os CNPJs analisados apresentam múltiplas e graves evidências de pertencerem ao mesmo
                    grupo econômico. As similaridades detectadas em dados cadastrais, vínculos societários,
                    padrões operacionais e indicadores fiscais sugerem fortemente operação coordenada e
                    gestão centralizada.

                    **RECOMENDAÇÃO URGENTE:**
                    - Análise aprofundada de possível planejamento tributário abusivo
                    - Verificação de fraude à lei (fracionamento artificial)
                    - Intimação dos contribuintes para esclarecimentos
                    - Considerar procedimento fiscal conjunto
                    """
                elif score_similaridade >= 10:
                    nivel_risco = "🟡 ALTO"
                    cor_risco = "warning"
                    conclusao = """
                    **EVIDÊNCIA SIGNIFICATIVA DE GRUPO ECONÔMICO**

                    Os CNPJs apresentam várias características compatíveis com grupo econômico.
                    As evidências encontradas justificam investigação mais aprofundada.

                    **RECOMENDAÇÃO:**
                    - Análise complementar com dados adicionais
                    - Solicitar documentação adicional aos contribuintes
                    - Monitoramento reforçado nos próximos períodos
                    - Verificar histórico de alterações cadastrais
                    """
                elif score_similaridade >= 5:
                    nivel_risco = "🟠 MODERADO"
                    cor_risco = "info"
                    conclusao = """
                    **INDÍCIOS MODERADOS DE GRUPO ECONÔMICO**

                    Alguns indícios sugerem possível vinculação entre os CNPJs, mas não são conclusivos.
                    Recomenda-se monitoramento e coleta de evidências adicionais.

                    **RECOMENDAÇÃO:**
                    - Monitoramento periódico dos CNPJs
                    - Atenção a novos indícios que possam surgir
                    - Cruzamento com outras bases de dados
                    - Acompanhar evolução das receitas
                    """
                else:
                    nivel_risco = "🟢 BAIXO"
                    cor_risco = "success"
                    conclusao = """
                    **BAIXA EVIDÊNCIA DE GRUPO ECONÔMICO**

                    Com base nos dados analisados, não foram encontradas evidências significativas de que
                    os CNPJs pertençam ao mesmo grupo econômico. As similaridades detectadas podem ser
                    coincidências ou características comuns do setor.

                    **RECOMENDAÇÃO:**
                    - Monitoramento de rotina conforme procedimentos padrão
                    - Atenção caso surjam novos indícios futuramente
                    """

                # Exibir nível de risco
                if cor_risco == "error":
                    st.error(f"**Nível de Risco: {nivel_risco}**")
                elif cor_risco == "warning":
                    st.warning(f"**Nível de Risco: {nivel_risco}**")
                elif cor_risco == "info":
                    st.info(f"**Nível de Risco: {nivel_risco}**")
                else:
                    st.success(f"**Nível de Risco: {nivel_risco}**")

                # Conclusão detalhada
                st.markdown("### 🎯 Conclusão da Análise")
                st.markdown(conclusao)

                # Tabela resumo de evidências
                if evidencias:
                    st.markdown("### 📋 Resumo das Evidências Encontradas")

                    categorias_evidencias = {
                        'Cadastrais': ['razao_social', 'fantasia', 'cnae', 'contador'],
                        'Societárias': ['socios_compartilhados'],
                        'Fiscais': ['receita_excesso', 'receita_uniforme'],
                        'Operacionais NFe': ['ip_transmissao_incons', 'cliente_incons', 'fornecedor_incons', 'codigo_produto_incons', 'tel_emit_incons', 'email_incons', 'end_emit_incons', 'end_dest_incons'],
                        'C115': ['c115_compartilhamento'],
                        'Financeiras': ['ccs_cpf_compartilhado', 'socios_meios_pagamento']
                    }

                    resumo_evidencias = []
                    for categoria, chaves in categorias_evidencias.items():
                        evidencias_categoria = [k for k in chaves if evidencias.get(k, False)]
                        if evidencias_categoria:
                            resumo_evidencias.append({
                                'Categoria': categoria,
                                'Quantidade': len(evidencias_categoria),
                                'Evidências': ', '.join([k.replace('_', ' ').title() for k in evidencias_categoria])
                            })

                    if resumo_evidencias:
                        df_resumo = pd.DataFrame(resumo_evidencias)
                        st.dataframe(df_resumo, width='stretch', hide_index=True)

                # Gráfico de distribuição de pontos
                st.markdown("### 📈 Distribuição de Pontos por Categoria")

                categorias_pontos = {
                    'Cadastro': sum([2 if evidencias.get('razao_social') else 0,
                                    1 if evidencias.get('fantasia') else 0,
                                    1 if evidencias.get('cnae') else 0,
                                    2 if evidencias.get('contador') else 0]),
                    'Sócios': 5 if evidencias.get('socios_compartilhados') else 0,
                    'Receitas': sum([5 if evidencias.get('receita_excesso') else 0,
                                    2 if evidencias.get('receita_uniforme') else 0]),
                    'NFe': sum([3 if evidencias.get('ip_transmissao_incons') else 0,
                               2 if evidencias.get('cliente_incons') else 0,
                               2 if evidencias.get('fornecedor_incons') else 0,
                               1 if evidencias.get('codigo_produto_incons') else 0]),
                    'C115': 3 if evidencias.get('c115_compartilhamento') else 0,
                    'CCS': 4 if evidencias.get('ccs_cpf_compartilhado') else 0
                }

                df_categorias = pd.DataFrame([
                    {'Categoria': k, 'Pontos': v}
                    for k, v in categorias_pontos.items() if v > 0
                ])

                if not df_categorias.empty:
                    fig = px.bar(df_categorias, x='Categoria', y='Pontos',
                                title="Pontos por Categoria de Evidência",
                                template=filtros['tema'],
                                color='Pontos',
                                color_continuous_scale='Reds')
                    st.plotly_chart(fig, use_container_width=True)

    # =========================================================================
    # TAB 14: EXPORTAÇÃO
    # =========================================================================
    with tab14:
        st.subheader("Exportação de Relatório")
        
        st.write("""
        Clique no botão abaixo para gerar um relatório em PDF com todas as informações 
        consolidadas deste grupo.
        """)
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col2:
            if st.button("📄 Gerar PDF do Dossiê", type="primary", key="gerar_pdf"):
                # Container para o progresso
                progress_container = st.container()
                
                with progress_container:
                    # Barra de progresso
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    try:
                        status_text.text("Iniciando geração do PDF...")
                        progress_bar.progress(10)
                        
                        status_text.text("Coletando dados do grupo...")
                        progress_bar.progress(30)
                        
                        status_text.text("Organizando informações...")
                        progress_bar.progress(50)
                        
                        status_text.text("Gerando documento PDF...")
                        progress_bar.progress(70)
                        
                        pdf_buffer = gerar_pdf_dossie(dossie, grupo_selecionado)
                        
                        progress_bar.progress(90)
                        status_text.text("Finalizando...")
                        
                        progress_bar.progress(100)
                        status_text.text("PDF gerado com sucesso!")
                        
                        st.success("✅ PDF gerado com sucesso!")
                        
                        # Botão de download automático
                        st.download_button(
                            label="⬇️ Download PDF",
                            data=pdf_buffer,
                            file_name=f"dossie_grupo_{grupo_selecionado}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                            mime="application/pdf",
                            key="download_pdf"
                        )
                        
                        # Limpar progresso após sucesso
                        import time
                        time.sleep(2)
                        progress_bar.empty()
                        status_text.empty()
                        
                    except Exception as e:
                        st.error(f"Erro ao gerar PDF: {e}")
                        progress_bar.empty()
                        status_text.empty()
        
        st.divider()
        
        # Informações sobre o relatório
        st.write("**O que inclui o relatório PDF:**")
        st.write("• Informações principais e métricas do grupo")
        st.write("• Lista completa de CNPJs com dados cadastrais")
        st.write("• Vínculos societários detalhados")
        st.write("• Todos os indícios fiscais identificados")
        st.write("• Análise financeira completa")
        st.write("• Dados de funcionários e meios de pagamento")
        st.write("• Informações do Convênio 115")
        st.write("• Informações CCS (Contas Compartilhadas)")
        st.write("• Inconsistências de NFe detalhadas com exemplos por tipo")

def menu_analises(engine, dados, filtros):
    """Análises avançadas e insights estratégicos"""
    st.markdown("<h1 class='main-header'>Análises Avançadas</h1>", unsafe_allow_html=True)
    
    st.info("Consultas analíticas e insights estratégicos do sistema GEI")
    
    score_col = 'score_final_ccs' if 'score_final_ccs' in dados['percent'].columns else 'score_final_avancado'
    
    # ==========================================================================
    # SEÇÃO 1: PANORAMA GERAL DO SISTEMA
    # ==========================================================================
    with st.expander("📊 Panorama Geral do Sistema", expanded=False):
        st.subheader("Panorama Geral do Sistema GEI")
        
        query = f"""
        SELECT 
            'PANORAMA GERAL DO SISTEMA GEI' AS categoria,
            COUNT(DISTINCT num_grupo) AS total_grupos_monitorados,
            COUNT(DISTINCT CASE WHEN qntd_cnpj >= 2 THEN num_grupo END) AS grupos_multiplas_empresas,
            SUM(qntd_cnpj) AS total_cnpjs_monitorados,
            COUNT(DISTINCT CASE WHEN {score_col} >= 20 THEN num_grupo END) AS grupos_risco_critico,
            COUNT(DISTINCT CASE WHEN {score_col} >= 15 AND {score_col} < 20 THEN num_grupo END) AS grupos_risco_alto,
            COUNT(DISTINCT CASE WHEN {score_col} >= 10 AND {score_col} < 15 THEN num_grupo END) AS grupos_risco_medio,
            ROUND(COUNT(DISTINCT CASE WHEN {score_col} >= 15 THEN num_grupo END) * 100.0 / 
                  COUNT(DISTINCT num_grupo), 2) AS perc_grupos_alto_risco,
            SUM(COALESCE(valor_max, 0)) AS receita_bruta_total_monitorada,
            COUNT(DISTINCT CASE WHEN valor_max >= 4800000 THEN num_grupo END) AS grupos_acima_limite_sn,
            AVG(total) AS media_inconsistencias_nfe,
            COUNT(DISTINCT CASE WHEN total >= 5 THEN num_grupo END) AS grupos_alta_inconsistencia,
            AVG(COALESCE(indice_interconexao, 0)) AS indice_interconexao_medio,
            COUNT(DISTINCT CASE WHEN qtd_socios_compartilhados > 0 THEN num_grupo END) AS grupos_socios_compartilhados,
            COUNT(DISTINCT CASE WHEN nivel_risco_grupo_economico IS NOT NULL THEN num_grupo END) AS grupos_com_dados_c115,
            COUNT(DISTINCT CASE WHEN qtd_total_indicios > 0 THEN num_grupo END) AS grupos_com_indicios,
            AVG(COALESCE(qtd_total_indicios, 0)) AS media_indicios_por_grupo,
            COUNT(DISTINCT CASE WHEN indice_risco_ccs > 0 THEN num_grupo END) AS grupos_com_dados_ccs,
            AVG(COALESCE(indice_risco_ccs, 0)) AS media_indice_ccs
        FROM gessimples.gei_percent
        """
        
        df_result = executar_query_analise(engine, "Panorama Geral", query)
        
        if not df_result.empty:
            info = df_result.iloc[0]
            
            # Métricas em cards
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Grupos", f"{int(info['total_grupos_monitorados']):,}")
            with col2:
                st.metric("Total CNPJs", f"{int(info['total_cnpjs_monitorados']):,}")
            with col3:
                st.metric("Grupos Críticos", f"{int(info['grupos_risco_critico']):,}")
            with col4:
                st.metric("% Alto Risco", f"{info['perc_grupos_alto_risco']:.1f}%")
            
            st.divider()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Receita Total", formatar_moeda(info['receita_bruta_total_monitorada']))
            with col2:
                st.metric("Acima Limite SN", f"{int(info['grupos_acima_limite_sn']):,}")
            with col3:
                st.metric("Média Indícios/Grupo", f"{info['media_indicios_por_grupo']:.1f}")
            
            st.divider()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Grupos com Dados CCS", f"{int(info['grupos_com_dados_ccs']):,}")
            with col2:
                st.metric("Média Índice CCS", f"{info['media_indice_ccs']:.4f}")
            
            # Tabela completa
            st.subheader("Detalhamento Completo")
            
            # Converter para formato transposto
            df_transposto = df_result.T.reset_index()
            df_transposto.columns = ['Métrica', 'Valor']
            df_transposto = df_transposto[df_transposto['Métrica'] != 'categoria']
            
            # Formatar nome das métricas
            df_transposto['Métrica'] = df_transposto['Métrica'].str.replace('_', ' ').str.title()
            
            st.dataframe(df_transposto, hide_index=True, use_container_width=True)
    
    # ==========================================================================
    # SEÇÃO 2: DISTRIBUIÇÃO POR FAIXAS DE SCORE
    # ==========================================================================
    with st.expander("📈 Distribuição por Faixas de Score", expanded=False):
        st.subheader("Distribuição por Faixas de Score")
        
        query = f"""
        SELECT 
            'DISTRIBUIÇÃO POR FAIXAS DE SCORE' AS categoria,
            CASE 
                WHEN {score_col} >= 25 THEN '25+ (Crítico Extremo)'
                WHEN {score_col} >= 20 THEN '20-24.99 (Crítico)'
                WHEN {score_col} >= 15 THEN '15-19.99 (Alto)'
                WHEN {score_col} >= 10 THEN '10-14.99 (Médio)'
                WHEN {score_col} >= 5 THEN '5-9.99 (Baixo)'
                ELSE '0-4.99 (Mínimo)'
            END AS faixa_score,
            COUNT(num_grupo) AS quantidade_grupos,
            ROUND(COUNT(num_grupo) * 100.0 / SUM(COUNT(num_grupo)) OVER(), 2) AS percentual,
            SUM(qntd_cnpj) AS total_cnpjs_faixa,
            AVG({score_col}) AS score_medio_faixa,
            MIN({score_col}) AS score_minimo_faixa,
            MAX({score_col}) AS score_maximo_faixa,
            AVG(COALESCE(valor_max, 0)) AS receita_media_faixa,
            COUNT(CASE WHEN valor_max >= 4800000 THEN 1 END) AS grupos_acima_sn_faixa,
            AVG(total) AS media_inconsistencias_faixa
        FROM gessimples.gei_percent
        GROUP BY 
            CASE 
                WHEN {score_col} >= 25 THEN '25+ (Crítico Extremo)'
                WHEN {score_col} >= 20 THEN '20-24.99 (Crítico)'
                WHEN {score_col} >= 15 THEN '15-19.99 (Alto)'
                WHEN {score_col} >= 10 THEN '10-14.99 (Médio)'
                WHEN {score_col} >= 5 THEN '5-9.99 (Baixo)'
                ELSE '0-4.99 (Mínimo)'
            END
        ORDER BY MIN({score_col}) DESC
        """
        
        df_result = executar_query_analise(engine, "Distribuição por Faixas", query)
        
        if not df_result.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(df_result, x='faixa_score', y='quantidade_grupos',
                           title="Grupos por Faixa de Score",
                           template=filtros['tema'],
                           labels={'quantidade_grupos': 'Quantidade', 'faixa_score': 'Faixa'})
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.pie(df_result, values='quantidade_grupos', names='faixa_score',
                           title="Distribuição Percentual",
                           template=filtros['tema'])
                st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Tabela Detalhada")
            
            # Formatar receita_media_faixa
            df_display = df_result.copy()
            df_display['receita_media_faixa'] = df_display['receita_media_faixa'].apply(formatar_moeda)
            
            st.dataframe(df_display, width='stretch', hide_index=True)
    
    # ==========================================================================
    # SEÇÃO 3: ANÁLISE SETORIAL POR CNAE - CORRIGIDA
    # ==========================================================================
    with st.expander("🏢 Análise Setorial por CNAE", expanded=False):
        st.subheader("Análise Setorial por CNAE")
        
        query = f"""
        WITH cnae_grupos AS (
            SELECT
                g.num_grupo,
                c.cd_cnae,
                SUBSTR(CAST(c.cd_cnae AS STRING), 1, 2) AS secao_cnae
            FROM gessimples.gei_cnpj g
            JOIN gessimples.gei_cadastro c ON g.cnpj = c.nu_cnpj
            WHERE c.cd_cnae IS NOT NULL
        ),
        grupos_cnae_principal AS (
            SELECT
                num_grupo,
                secao_cnae,
                ROW_NUMBER() OVER (PARTITION BY num_grupo ORDER BY secao_cnae) AS rn
            FROM cnae_grupos
            GROUP BY num_grupo, secao_cnae
        )
        SELECT
            'ANÁLISE SETORIAL - CNAE' AS categoria,
            gcp.secao_cnae,
            CASE gcp.secao_cnae
                WHEN '01' THEN 'Agricultura, Pecuária'
                WHEN '10' THEN 'Fabricação de Produtos Alimentícios'
                WHEN '46' THEN 'Comércio Atacadista'
                WHEN '47' THEN 'Comércio Varejista'
                WHEN '68' THEN 'Atividades Imobiliárias'
                WHEN '70' THEN 'Atividades de Consultoria'
                WHEN '77' THEN 'Aluguel e Leasing'
                WHEN '82' THEN 'Serviços de Apoio'
                ELSE CONCAT('Seção ', gcp.secao_cnae)
            END AS descricao_setor,
            COUNT(DISTINCT p.num_grupo) AS grupos_no_setor,
            AVG(p.{score_col}) AS score_medio_setor,
            MIN(p.{score_col}) AS score_minimo_setor,
            MAX(p.{score_col}) AS score_maximo_setor,
            AVG(p.valor_max) AS receita_media_setor,
            AVG(p.qntd_cnpj) AS media_empresas_por_grupo,
            COUNT(CASE WHEN p.{score_col} >= 15 THEN 1 END) AS grupos_alto_risco,
            ROUND(COUNT(CASE WHEN p.{score_col} >= 15 THEN 1 END) * 100.0 / COUNT(DISTINCT p.num_grupo), 2) AS perc_alto_risco_setor
        FROM grupos_cnae_principal gcp
        JOIN gessimples.gei_percent p ON gcp.num_grupo = p.num_grupo
        WHERE gcp.rn = 1
        GROUP BY gcp.secao_cnae
        HAVING COUNT(DISTINCT p.num_grupo) >= 5
        ORDER BY AVG(p.{score_col}) DESC
        LIMIT 20
        """
        
        df_result = executar_query_analise(engine, "Análise Setorial", query)
        
        if not df_result.empty:
            # ⚠️ CORRIGIR NaN ANTES DO GRÁFICO
            df_result['receita_media_setor'] = df_result['receita_media_setor'].fillna(0)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(df_result, x='descricao_setor', y='score_medio_setor',
                           title="Score Médio por Setor",
                           template=filtros['tema'],
                           labels={'score_medio_setor': 'Score Médio', 'descricao_setor': 'Setor'})
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.scatter(df_result, x='grupos_no_setor', y='perc_alto_risco_setor',
                               size='receita_media_setor',
                               hover_data=['descricao_setor'],
                               title="Grupos vs % Alto Risco por Setor",
                               template=filtros['tema'],
                               labels={'grupos_no_setor': 'Quantidade de Grupos', 
                                      'perc_alto_risco_setor': '% Alto Risco'})
                st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Tabela Detalhada")
            
            # Formatar receita_media_setor
            df_display = df_result.copy()
            df_display['receita_media_setor'] = df_display['receita_media_setor'].apply(formatar_moeda)
            
            st.dataframe(df_display, width='stretch', hide_index=True)

# =============================================================================
# DICIONÁRIO DE COORDENADAS DOS MUNICÍPIOS DE SC
# =============================================================================

COORDENADAS_MUNICIPIOS_SC = {
    'FLORIANOPOLIS': (-27.5954, -48.5480),
    'JOINVILLE': (-26.3045, -48.8487),
    'BLUMENAU': (-26.9194, -49.0661),
    'SAO JOSE': (-27.6136, -48.6366),
    'CHAPECO': (-27.1006, -52.6156),
    'CRICIUMA': (-28.6775, -49.3697),
    'ITAJAI': (-26.9078, -48.6619),
    'JARAGUA DO SUL': (-26.4853, -49.0689),
    'LAGES': (-27.8157, -50.3264),
    'PALHOCA': (-27.6456, -48.6682),
    'BRUSQUE': (-27.0979, -48.9173),
    'TUBARAO': (-28.4669, -49.0068),
    'SAO BENTO DO SUL': (-26.2503, -49.3786),
    'CACADOR': (-26.7753, -51.0150),
    'CONCORDIA': (-27.2339, -52.0278),
    'CAMBORIU': (-27.0253, -48.6542),
    'BALNEARIO CAMBORIU': (-26.9906, -48.6347),
    'RIO DO SUL': (-27.2142, -49.6431),
    'BIGUACU': (-27.4942, -48.6558),
    'NAVEGANTES': (-26.8986, -48.6544),
    'GASPAR': (-26.9314, -49.1158),
    'CANOINHAS': (-26.1769, -50.3908),
    'MAFRA': (-26.1117, -49.8053),
    'INDAIAL': (-26.8978, -49.2317),
    'ICARA': (-28.7136, -49.2994),
    'ARARANGUA': (-28.9353, -49.4858),
    'TIJUCAS': (-27.2411, -48.6336),
    'XANXERE': (-26.8764, -52.4039),
    'IMBITUBA': (-28.2400, -48.6700),
    'VIDEIRA': (-27.0078, -51.1517),
    'CURITIBANOS': (-27.2831, -50.5847),
    'SAO FRANCISCO DO SUL': (-26.2428, -48.6389),
    'PORTO UNIAO': (-26.2372, -51.0742),
    'LAGUNA': (-28.4828, -48.7819),
    'SAO MIGUEL DO OESTE': (-26.7250, -53.5153),
    'PENHA': (-26.7706, -48.6464),
    'TIMBÓ': (-26.8236, -49.2731),
    'TIMBO': (-26.8236, -49.2731),
    'POMERODE': (-26.7408, -49.1764),
    'JOACABA': (-27.1781, -51.5022),
    'ORLEANS': (-28.3578, -49.2917),
    'URUSSANGA': (-28.5189, -49.3208),
    'SOMBRIO': (-29.1050, -49.6317),
    'TURVO': (-28.9256, -49.6769),
    'FORQUILHINHA': (-28.7464, -49.4728),
    'COCAL DO SUL': (-28.6006, -49.3283),
    'MORRO DA FUMACA': (-28.6533, -49.2186),
    'NOVA VENEZA': (-28.6336, -49.5000),
    'SIDEROPOLIS': (-28.5939, -49.4258),
    'CAPIVARI DE BAIXO': (-28.4500, -48.9583),
    'GRAVATAL': (-28.3222, -49.0444),
    'BRACO DO NORTE': (-28.2750, -49.1658),
    'SAO LUDGERO': (-28.3306, -49.1764),
    'GRÃO PARA': (-28.1833, -49.2250),
    'GRAO PARA': (-28.1833, -49.2250),
    'SANTA ROSA DO SUL': (-29.1333, -49.7167),
    'PRAIA GRANDE': (-29.1917, -49.9500),
    'SAO JOAO DO SUL': (-29.2167, -49.8000),
    'PASSO DE TORRES': (-29.3083, -49.7250),
    'BALNEARIO ARROIO DO SILVA': (-28.9833, -49.4167),
    'BALNEARIO GAIVOTA': (-29.1500, -49.5833),
    'ERMO': (-28.9833, -49.6333),
    'MELEIRO': (-28.8250, -49.6333),
    'MORRO GRANDE': (-28.8000, -49.7167),
    'TREVISO': (-28.5167, -49.4667),
    'LAURO MULLER': (-28.3917, -49.4000),
    'BOM JARDIM DA SERRA': (-28.3333, -49.6333),
    'SAO JOAQUIM': (-28.2944, -49.9319),
    'URUBICI': (-28.0150, -49.5917),
    'URUPEMA': (-28.2917, -49.8750),
    'PAINEL': (-27.9250, -50.1000),
    'BOCAINA DO SUL': (-27.7500, -49.9417),
    'OTACILIO COSTA': (-27.4833, -50.1250),
    'CORREIA PINTO': (-27.5833, -50.3583),
    'PONTE ALTA': (-27.4833, -50.3833),
    'SAO JOSE DO CERRITO': (-27.6583, -50.5750),
    'CAMPO BELO DO SUL': (-27.8917, -50.7583),
    'CERRO NEGRO': (-27.7917, -50.8667),
    'CAPAO ALTO': (-28.2333, -50.5083),
    'ANITA GARIBALDI': (-27.6917, -51.1250),
    'CELSO RAMOS': (-27.6333, -51.3417),
    'ABDON BATISTA': (-27.6083, -51.0250),
    'CAMPOS NOVOS': (-27.4014, -51.2258),
    'MONTE CARLO': (-27.2167, -50.9833),
    'BRUNOPOLIS': (-27.3000, -50.8667),
    'VARGEM': (-27.4833, -50.5500),
    'FRAIBURGO': (-27.0250, -50.9208),
    'TANGARA': (-27.0917, -51.2500),
    'IBICARE': (-27.0917, -51.3750),
    'PIRATUBA': (-27.4250, -51.7667),
    'CAPINZAL': (-27.3500, -51.6083),
    'OURO': (-27.3333, -51.6167),
    'LACERDOPOLIS': (-27.2583, -51.5583),
    'HERVAL DO OESTE': (-27.1917, -51.4917),
    'CATANDUVAS': (-27.0667, -51.6667),
    'AGUA DOCE': (-26.9983, -51.5525),
    'IRANI': (-27.0333, -51.9000),
    'PONTE SERRADA': (-26.8750, -52.0083),
    'VARGEAO': (-26.8583, -52.1583),
    'FAXINAL DOS GUEDES': (-26.8417, -52.2667),
    'OURO VERDE': (-26.6917, -52.3083),
    'BOM JESUS': (-26.7333, -52.3917),
    'IPUACU': (-26.6333, -52.4500),
    'ENTRE RIOS': (-26.7167, -52.5417),
    'ABELARDO LUZ': (-26.5667, -52.3333),
    'SAO DOMINGOS': (-26.5583, -52.5333),
    'GALVAO': (-26.4583, -52.6917),
    'JUPIA': (-26.3917, -52.7333),
    'CORONEL MARTINS': (-26.5083, -52.6750),
    'LAJEADO GRANDE': (-26.8583, -52.5750),
    'PASSOS MAIA': (-26.7833, -52.0583),
    'LUZERNA': (-27.1333, -51.4667),
    'IBIAM': (-27.1833, -51.2333),
    'ZORTEA': (-27.4500, -51.5500),
    'TREZE TILIAS': (-26.9583, -51.4083),
    'SALTO VELOSO': (-26.9000, -51.4000),
    'MACIEIRA': (-26.8583, -51.3667),
    'CALMON': (-26.5917, -51.0917),
    'MATOS COSTA': (-26.4667, -51.1500),
    'TIMBÓ GRANDE': (-26.6167, -50.6583),
    'TIMBO GRANDE': (-26.6167, -50.6583),
    'SANTA CECILIA': (-26.9583, -50.4250),
    'LEBON REGIS': (-26.9250, -50.6917),
    'MONTE CASTELO': (-26.4583, -50.2333),
    'PAPANDUVA': (-26.4333, -50.1417),
    'IRINEÓPOLIS': (-26.2417, -50.7917),
    'IRINEOPOLIS': (-26.2417, -50.7917),
    'TRES BARRAS': (-26.1083, -50.3167),
    'MAJOR VIEIRA': (-26.3667, -50.3250),
    'BELA VISTA DO TOLDO': (-26.2833, -50.4667),
    'ITAIÓPOLIS': (-26.3383, -49.9081),
    'ITAIOPOLIS': (-26.3383, -49.9081),
    'RIO NEGRINHO': (-26.2586, -49.5181),
    'CAMPO ALEGRE': (-26.1928, -49.2661),
    'CORUPÁ': (-26.4247, -49.2447),
    'CORUPA': (-26.4247, -49.2447),
    'SCHROEDER': (-26.4133, -49.0728),
    'GUARAMIRIM': (-26.4692, -49.0011),
    'MASSARANDUBA': (-26.6125, -49.0086),
    'LUIZ ALVES': (-26.7150, -48.9317),
    'ILHOTA': (-26.9028, -48.8247),
    'PENHA': (-26.7706, -48.6464),
    'PICARRAS': (-26.7539, -48.6767),
    'BALNEARIO BARRA DO SUL': (-26.4589, -48.6119),
    'ARAQUARI': (-26.3728, -48.7172),
    'GARUVA': (-26.0247, -48.8539),
    'GUARUVA': (-26.0247, -48.8539),
    'ITAPOA': (-26.1167, -48.6167),
    'BOMBINHAS': (-27.1383, -48.5147),
    'PORTO BELO': (-27.1592, -48.5531),
    'GOVERNADOR CELSO RAMOS': (-27.3167, -48.5583),
    'ANTONIO CARLOS': (-27.5158, -48.7689),
    'ANGELINA': (-27.5708, -48.9883),
    'RANCHO QUEIMADO': (-27.6708, -49.0192),
    'ANITAPOLIS': (-27.9017, -49.1308),
    'ALFREDO WAGNER': (-27.7000, -49.3333),
    'LEOBERTO LEAL': (-27.5083, -49.2750),
    'MAJOR GERCINO': (-27.4167, -49.0333),
    'NOVA TRENTO': (-27.2861, -49.0786),
    'CANELINHA': (-27.2636, -48.7650),
    'SAO JOAO BATISTA': (-27.2761, -48.8489),
    'AGUAS MORNAS': (-27.6958, -48.8236),
    'SANTO AMARO DA IMPERATRIZ': (-27.6897, -48.7797),
    'PAULO LOPES': (-27.9608, -48.6869),
    'GAROPABA': (-28.0269, -48.6183),
    'IMARUI': (-28.3333, -48.8167),
    'SAO MARTINHO': (-28.1667, -48.9833),
    'ARMAZEM': (-28.2417, -49.0167),
    'RIO FORTUNA': (-28.1250, -49.1083),
    'SANTA ROSA DE LIMA': (-28.0333, -49.1333),
    'SANGAO': (-28.6333, -49.1333),
    'JAGUARUNA': (-28.6147, -49.0256),
    'TREZE DE MAIO': (-28.5500, -49.1500),
    'PEDRAS GRANDES': (-28.4333, -49.1917),
    'IBIRAMA': (-27.0567, -49.5175),
    'PRESIDENTE GETULIO': (-27.0500, -49.6250),
    'DONA EMMA': (-26.9833, -49.7167),
    'WITMARSUM': (-26.9250, -49.7917),
    'JOSE BOITEUX': (-26.9583, -49.6250),
    'VITOR MEIRELES': (-26.8833, -49.8333),
    'SALETE': (-26.9750, -49.9917),
    'TAIO': (-27.1167, -49.9917),
    'POUSO REDONDO': (-27.2583, -49.9333),
    'TROMBUDO CENTRAL': (-27.2917, -49.7917),
    'AGRONOMICA': (-27.2667, -49.7083),
    'AURORA': (-27.3083, -49.6333),
    'ATALANTA': (-27.4250, -49.7750),
    'IMBUIA': (-27.4917, -49.4250),
    'VIDAL RAMOS': (-27.3917, -49.3667),
    'LONTRAS': (-27.1667, -49.5333),
    'APIUNA': (-27.0333, -49.3917),
    'ASCURRA': (-26.9500, -49.3667),
    'RODEIO': (-26.9222, -49.3650),
    'BENEDITO NOVO': (-26.7833, -49.3583),
    'DOUTOR PEDRINHO': (-26.7167, -49.4833),
    'RIO DOS CEDROS': (-26.7417, -49.2750),
    'APIUNA': (-27.0333, -49.3917),
    'BOTUVERÁ': (-27.2000, -49.0667),
    'BOTUVERA': (-27.2000, -49.0667),
    'GUABIRUBA': (-27.0833, -48.9833),
    'AGROLÂNDIA': (-27.4083, -49.8250),
    'AGROLANDIA': (-27.4083, -49.8250),
    'PETROLÂNDIA': (-27.5333, -49.6917),
    'PETROLANDIA': (-27.5333, -49.6917),
    'ITUPORANGA': (-27.4106, -49.6031),
    'CHAPADÃO DO LAGEADO': (-27.5917, -49.5500),
    'CHAPADAO DO LAGEADO': (-27.5917, -49.5500),
    'PRESIDENTE NEREU': (-27.2750, -49.3917),
    'LAURENTINO': (-27.2167, -49.7333),
    'MIRIM DOCE': (-27.1917, -50.0583),
    'SANTA TEREZINHA': (-26.7833, -50.0167),
    'MODELO': (-26.7750, -53.0417),
    'SERRA ALTA': (-26.7250, -53.0417),
    'CAIBI': (-27.0750, -53.2500),
    'PALMITOS': (-27.0667, -53.1583),
    'CUNHA PORA': (-26.8917, -53.1667),
    'MARAVILHA': (-26.7639, -53.1714),
    'SAUDADES': (-26.9250, -53.0083),
    'PINHALZINHO': (-26.8500, -52.9917),
    'NOVA ERECHIM': (-26.8917, -52.9083),
    'UNIAO DO OESTE': (-26.7583, -52.8583),
    'JARDINOPOLIS': (-26.7167, -52.8583),
    'CORDILHEIRA ALTA': (-26.9833, -52.6083),
    'GUATAMBU': (-27.1333, -52.7917),
    'PLANALTO ALEGRE': (-27.0667, -52.8667),
    'NOVA ITABERABA': (-26.9417, -52.8083),
    'CAXAMBU DO SUL': (-27.1583, -52.8833),
    'AGUASDECHAPECO': (-27.0750, -52.9833),
    'AGUAS DE CHAPECO': (-27.0750, -52.9833),
    'SAO CARLOS': (-27.0833, -53.0083),
    'QUILOMBO': (-26.7250, -52.7250),
    'FORMOSA DO SUL': (-26.6417, -52.7917),
    'SANTIAGO DO SUL': (-26.6417, -52.6833),
    'IRATI': (-26.6583, -52.8917),
    'ARVOREDO': (-27.0750, -52.4583),
    'SEARA': (-27.1500, -52.3083),
    'XAVANTINA': (-27.0667, -52.3417),
    'LINDOIA DO SUL': (-27.0500, -52.0667),
    'IPUMIRIM': (-27.0750, -52.1333),
    'ITA': (-27.2833, -52.3250),
    'ARABUTA': (-27.1583, -52.3000),
    'ALTO BELA VISTA': (-27.4333, -51.9000),
    'PERITIBA': (-27.3750, -51.9083),
    'IPIRA': (-27.4000, -51.7750),
    'PRESIDENTE CASTELLO BRANCO': (-27.2250, -51.8083),
    'JABORÁ': (-27.1750, -51.7333),
    'JABORA': (-27.1750, -51.7333),
    'ERVAL VELHO': (-27.2750, -51.4417),
    'PINHEIRO PRETO': (-27.0500, -51.2250),
    'IOMERE': (-27.0000, -51.2417),
    'ARROIO TRINTA': (-26.9250, -51.3417),
    'CACADOR': (-26.7753, -51.0150),
    'RIO DAS ANTAS': (-26.8917, -51.0750),
    'CAÇADOR': (-26.7753, -51.0150),
    'SAO LOURENCO DO OESTE': (-26.3583, -52.8500),
    'NOVO HORIZONTE': (-26.4417, -52.8250),
    'CAMPO ERÊ': (-26.3917, -53.0833),
    'CAMPO ERE': (-26.3917, -53.0833),
    'SALTINHO': (-26.6083, -53.0583),
    'SAO BERNARDINO': (-26.4750, -52.9667),
    'CORONEL FREITAS': (-26.9083, -52.7000),
    'AGUAS FRIAS': (-26.8750, -52.8583),
    'SUL BRASIL': (-26.7333, -52.9667),
    'ITAPIRANGA': (-27.1697, -53.7117),
    'SAO JOAO DO OESTE': (-27.0917, -53.5917),
    'TUNAPOLIS': (-26.9667, -53.6417),
    'IPORA DO OESTE': (-26.9833, -53.5333),
    'SANTA HELENA': (-26.9333, -53.6167),
    'MONDAI': (-27.1000, -53.4000),
    'RIQUEZA': (-27.0667, -53.3250),
    'ROMELANDIA': (-26.9250, -53.3167),
    'SAO MIGUEL DA BOA VISTA': (-26.6917, -53.2500),
    'BARRA BONITA': (-26.6500, -53.4417),
    'GUARACIABA': (-26.6000, -53.5250),
    'SAO JOSE DO CEDRO': (-26.4583, -53.4917),
    'PRINCESA': (-26.4417, -53.6000),
    'PARAISO': (-26.6167, -53.6750),
    'ANCHIETA': (-26.5333, -53.3333),
    'BANDEIRANTE': (-26.7667, -53.6417),
    'DESCANSO': (-26.8250, -53.5000),
    'BOM JESUS DO OESTE': (-26.6917, -53.1000),
    'TIGRINHOS': (-26.6833, -53.1583),
    'DIONISIO CERQUEIRA': (-26.2583, -53.6333),
    'GUARUJA DO SUL': (-26.3833, -53.5333),
    'PALMA SOLA': (-26.3500, -53.2750),
    'FLOR DO SERTAO': (-26.7833, -53.3500),
    'IRACEMINHA': (-26.8167, -53.2750),
    'ROMELÂNDIA': (-26.9250, -53.3167),
}

# =============================================================================
# FUNÇÃO MENU MAPA - VISUALIZAÇÃO GEOGRÁFICA DAS EMPRESAS
# =============================================================================

def menu_mapa(engine, dados, filtros):
    """Exibe mapa interativo com localização das empresas por grupo econômico"""

    st.title("Mapa de Empresas por Grupo Econômico")
    st.markdown("""
    Visualize a distribuição geográfica das empresas em Santa Catarina.
    Você pode ver todas as empresas ou filtrar por um grupo econômico específico.
    """)

    # Opções de visualização
    col1, col2 = st.columns([1, 2])

    with col1:
        modo_visualizacao = st.radio(
            "Modo de visualização:",
            ["Todos os Grupos", "Grupo Específico"],
            help="Escolha se quer ver todas as empresas ou apenas de um grupo"
        )

    # Carregar dados de empresas com localização
    @st.cache_data(ttl=3600)
    def carregar_empresas_mapa(_engine, num_grupo=None):
        """Carrega empresas com município para o mapa"""
        if num_grupo:
            query = f"""
            SELECT
                g.num_grupo,
                g.cnpj,
                c.nm_razao_social,
                c.nm_fantasia,
                c.nm_munic as municipio,
                c.cd_cnae
            FROM {DATABASE}.gei_cnpj g
            LEFT JOIN usr_sat_ods.vw_ods_contrib c ON g.cnpj = c.nu_cnpj
            WHERE g.num_grupo = '{num_grupo}'
            """
        else:
            # Limitar para performance
            query = f"""
            SELECT
                g.num_grupo,
                g.cnpj,
                c.nm_razao_social,
                c.nm_fantasia,
                c.nm_munic as municipio,
                c.cd_cnae
            FROM {DATABASE}.gei_cnpj g
            LEFT JOIN usr_sat_ods.vw_ods_contrib c ON g.cnpj = c.nu_cnpj
            LIMIT 10000
            """

        try:
            df = pd.read_sql(query, _engine)
            df.columns = [col.lower() for col in df.columns]
            return df
        except Exception as e:
            st.error(f"Erro ao carregar dados: {e}")
            return pd.DataFrame()

    # Função para obter coordenadas do município
    def obter_coordenadas(municipio):
        """Retorna latitude e longitude do município"""
        if pd.isna(municipio):
            return None, None

        # Normalizar nome do município (remover acentos, upper)
        import unicodedata
        municipio_norm = unicodedata.normalize('NFKD', str(municipio).upper())
        municipio_norm = ''.join(c for c in municipio_norm if not unicodedata.combining(c))
        municipio_norm = municipio_norm.strip()

        # Buscar no dicionário
        if municipio_norm in COORDENADAS_MUNICIPIOS_SC:
            return COORDENADAS_MUNICIPIOS_SC[municipio_norm]

        # Tentar variações
        for key in COORDENADAS_MUNICIPIOS_SC:
            if key in municipio_norm or municipio_norm in key:
                return COORDENADAS_MUNICIPIOS_SC[key]

        return None, None

    # Gerar cores distintas para grupos
    def gerar_cor_grupo(num_grupo):
        """Gera uma cor distinta baseada no número do grupo"""
        cores = [
            'red', 'blue', 'green', 'purple', 'orange', 'darkred',
            'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue',
            'darkpurple', 'pink', 'lightblue', 'lightgreen', 'gray',
            'black', 'lightgray'
        ]
        try:
            idx = hash(str(num_grupo)) % len(cores)
            return cores[idx]
        except:
            return 'blue'

    # Lógica principal baseada no modo de visualização
    if modo_visualizacao == "Grupo Específico":
        with col2:
            # Obter lista de grupos disponíveis
            grupos_disponiveis = sorted(dados['percent']['num_grupo'].unique().tolist())

            grupo_selecionado = st.selectbox(
                "Selecione o Grupo Econômico:",
                grupos_disponiveis,
                format_func=lambda x: f"Grupo {x}"
            )

        if grupo_selecionado:
            with st.spinner(f"Carregando empresas do Grupo {grupo_selecionado}..."):
                df_empresas = carregar_empresas_mapa(engine, grupo_selecionado)
    else:
        with col2:
            st.info("Exibindo até 10.000 empresas para melhor performance")

        with st.spinner("Carregando empresas..."):
            df_empresas = carregar_empresas_mapa(engine)

    if df_empresas.empty:
        st.warning("Nenhuma empresa encontrada com os filtros selecionados.")
        return

    # Adicionar coordenadas
    df_empresas['lat'] = df_empresas['municipio'].apply(lambda x: obter_coordenadas(x)[0])
    df_empresas['lon'] = df_empresas['municipio'].apply(lambda x: obter_coordenadas(x)[1])

    # Filtrar apenas empresas com coordenadas válidas
    df_com_coords = df_empresas.dropna(subset=['lat', 'lon'])

    # Estatísticas
    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)

    with col_stat1:
        st.metric("Total de Empresas", f"{len(df_empresas):,}")

    with col_stat2:
        st.metric("Com Localização", f"{len(df_com_coords):,}")

    with col_stat3:
        st.metric("Municípios", f"{df_com_coords['municipio'].nunique():,}")

    with col_stat4:
        if modo_visualizacao == "Todos os Grupos":
            st.metric("Grupos", f"{df_com_coords['num_grupo'].nunique():,}")
        else:
            st.metric("Grupo", f"{grupo_selecionado}")

    if df_com_coords.empty:
        st.warning("Nenhuma empresa possui localização válida para exibir no mapa.")
        return

    # Criar mapa centrado em SC
    mapa = folium.Map(
        location=[-27.5954, -49.0000],  # Centro de SC
        zoom_start=7,
        tiles='cartodbpositron'
    )

    # Adicionar marcadores
    if modo_visualizacao == "Grupo Específico":
        # Todos da mesma cor para o grupo específico
        for _, row in df_com_coords.iterrows():
            popup_html = f"""
            <div style='width: 250px'>
                <b>CNPJ:</b> {row['cnpj']}<br>
                <b>Razão Social:</b> {row.get('nm_razao_social', 'N/A')}<br>
                <b>Fantasia:</b> {row.get('nm_fantasia', 'N/A')}<br>
                <b>Município:</b> {row['municipio']}<br>
                <b>CNAE:</b> {row.get('cd_cnae', 'N/A')}<br>
                <b>Grupo:</b> {row['num_grupo']}
            </div>
            """

            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{row.get('nm_fantasia', row['cnpj'])}",
                icon=folium.Icon(color='red', icon='building', prefix='fa')
            ).add_to(mapa)
    else:
        # Cores diferentes por grupo
        # Usar MarkerCluster para melhor performance
        from folium.plugins import MarkerCluster
        marker_cluster = MarkerCluster().add_to(mapa)

        for _, row in df_com_coords.iterrows():
            popup_html = f"""
            <div style='width: 250px'>
                <b>CNPJ:</b> {row['cnpj']}<br>
                <b>Razão Social:</b> {row.get('nm_razao_social', 'N/A')}<br>
                <b>Fantasia:</b> {row.get('nm_fantasia', 'N/A')}<br>
                <b>Município:</b> {row['municipio']}<br>
                <b>CNAE:</b> {row.get('cd_cnae', 'N/A')}<br>
                <b>Grupo:</b> {row['num_grupo']}
            </div>
            """

            cor = gerar_cor_grupo(row['num_grupo'])

            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"Grupo {row['num_grupo']} - {row.get('nm_fantasia', row['cnpj'])}",
                icon=folium.Icon(color=cor, icon='building', prefix='fa')
            ).add_to(marker_cluster)

    # Exibir mapa
    st.subheader("Mapa de Localização")
    st_folium(mapa, width=None, height=600, use_container_width=True)

    # Tabela de empresas por município
    st.subheader("Distribuição por Município")

    df_municipios = df_com_coords.groupby('municipio').agg({
        'cnpj': 'count',
        'num_grupo': 'nunique'
    }).reset_index()
    df_municipios.columns = ['Município', 'Qtd Empresas', 'Qtd Grupos']
    df_municipios = df_municipios.sort_values('Qtd Empresas', ascending=False)

    col_chart, col_table = st.columns([1, 1])

    with col_chart:
        # Gráfico de barras dos top municípios
        fig = px.bar(
            df_municipios.head(15),
            x='Município',
            y='Qtd Empresas',
            color='Qtd Grupos',
            title='Top 15 Municípios por Número de Empresas',
            template=filtros.get('tema', 'plotly_white')
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    with col_table:
        st.dataframe(
            df_municipios,
            use_container_width=True,
            hide_index=True,
            height=400
        )

    # Lista de empresas (opcional, expandível)
    with st.expander("Ver Lista de Empresas"):
        df_display = df_com_coords[['cnpj', 'nm_razao_social', 'nm_fantasia', 'municipio', 'num_grupo']].copy()
        df_display.columns = ['CNPJ', 'Razão Social', 'Nome Fantasia', 'Município', 'Grupo']
        st.dataframe(df_display, use_container_width=True, hide_index=True)


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    """Função principal do sistema"""
    
    # Sidebar com navegação
    st.sidebar.title("Sistema GEI v3.0")
    
    paginas = [
        "Dashboard Executivo",
        "Ranking",
        "Análise Pontual",
        "Contadores",
        "Meios de Pagamento",
        "Funcionários",
        "Convênio 115",
        "Procuração Bancária (CCS)",
        "Financeiro",
        "Inconsistências NFe",
        "Indícios Fiscais",
        "Vínculos Societários",
        "Dossiê do Grupo",
        "🗺️ Mapa",
        "🤖 Machine Learning",
        "Análises"
    ]
    
    pag = st.sidebar.radio("Navegação:", paginas)  # USE APENAS UMA VARIÁVEL
    
    # Filtros
    filtros = criar_filtros_sidebar()
    
    # Conexão com o banco
    engine = get_impala_engine()
    
    if engine is None:
        st.stop()
    
    st.sidebar.success("✅ Conectado ao Impala")
    
    # Carregamento dos dados
    dados = carregar_todos_os_dados(engine)
    
    if not dados or dados['percent'].empty:
        st.error("Erro ao carregar dados principais")
        return
    
    st.sidebar.info(f"📊 {len(dados['percent']):,} grupos carregados")
    
    # Roteamento das páginas
    if pag == "Dashboard Executivo":
        dashboard_executivo(dados, filtros)
    elif pag == "Ranking":
        ranking_grupos(dados, filtros)
    elif pag == "Análise Pontual":
        analise_pontual(engine, dados, filtros)    
    elif pag == "Contadores":
        menu_contadores(engine, dados, filtros)
    elif pag == "Meios de Pagamento":
        menu_pagamentos(engine, dados, filtros)
    elif pag == "Funcionários":
        menu_funcionarios(engine, dados, filtros)
    elif pag == "Convênio 115":
        menu_c115(engine, dados, filtros)
    elif pag == "Procuração Bancária (CCS)":
        menu_ccs(engine, dados, filtros)
    elif pag == "Financeiro":
        menu_financeiro(engine, dados, filtros)
    elif pag == "Inconsistências NFe":
        inconsistencias_nfe(engine, dados, filtros)
    elif pag == "Indícios Fiscais":
        indicios_fiscais(dados, filtros)
    elif pag == "Vínculos Societários":
        vinculos_societarios(dados, filtros)
    elif pag == "Dossiê do Grupo":
        dossie_grupo(engine, dados, filtros)
    elif pag == "🗺️ Mapa":
        menu_mapa(engine, dados, filtros)
    elif pag == "🤖 Machine Learning":
        analise_machine_learning(engine, dados, filtros)
    elif pag == "Análises":
        menu_analises(engine, dados, filtros)
    
    # Rodapé
    st.markdown("---")
    st.markdown(f"""
    <div style='text-align: center; color: #666; font-size: 0.8em;'>
        Sistema GEI v3.0 | Receita Estadual SC | {datetime.now().strftime('%d/%m/%Y %H:%M')}
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# EXECUÇÃO DO PROGRAMA
# =============================================================================
if __name__ == "__main__":
    main()