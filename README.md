# GEI - Sistema de Detecção de Grupos Econômicos

**Versão 3.0** | Receita Estadual de Santa Catarina

Sistema de inteligência fiscal para identificação e monitoramento de grupos econômicos, com foco na detecção de fragmentação empresarial para evasão do limite do Simples Nacional.

---

## Sumário

- [Visão Geral](#visão-geral)
- [Tecnologias](#tecnologias)
- [Requisitos](#requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Funcionalidades](#funcionalidades)
  - [Menu Principal](#menu-principal)
  - [Dimensões de Análise](#dimensões-de-análise)
- [Machine Learning](#machine-learning)
- [Fontes de Dados](#fontes-de-dados)
- [Métricas e Indicadores](#métricas-e-indicadores)
- [Segurança](#segurança)
- [Contribuição](#contribuição)
- [Licença](#licença)

---

## Visão Geral

O **GEI (Sistema de Detecção de Grupos Econômicos)** é uma plataforma de inteligência fiscal desenvolvida para a Receita Estadual de Santa Catarina. O sistema identifica e monitora grupos econômicos - empresas interconectadas que podem estar fragmentando operações para permanecer abaixo do limite de R$ 4,8 milhões do Simples Nacional.

### Objetivos Principais

- **Detecção de Fragmentação**: Identificar empresas que se dividem artificialmente para evadir impostos
- **Análise de Risco**: Classificar grupos econômicos por nível de risco fiscal
- **Inteligência Fiscal**: Fornecer insights acionáveis para fiscalização
- **Machine Learning**: Aplicar algoritmos de clustering para detecção automática de padrões

### Problema Endereçado

Empresas que ultrapassam o limite de faturamento do Simples Nacional (R$ 4,8 milhões/ano) devem migrar para o regime Normal, com alíquotas de ICMS significativamente maiores. Algumas empresas fragmentam suas operações em múltiplos CNPJs para manter cada unidade abaixo do limite, evadindo assim a tributação adequada.

O GEI detecta esses grupos através de múltiplas dimensões de análise, incluindo:
- Vínculos societários compartilhados
- Padrões de pagamento similares
- Uso comum de infraestrutura (energia, telecom)
- Contadores e procuradores em comum
- Inconsistências em documentos fiscais

---

## Tecnologias

### Stack Principal

| Categoria | Tecnologia |
|-----------|------------|
| **Framework Web** | Streamlit |
| **Linguagem** | Python 3.x |
| **Banco de Dados** | Apache Impala |
| **Autenticação BD** | LDAP com SSL/TLS |
| **Visualização** | Plotly, Folium |
| **Machine Learning** | scikit-learn |
| **Análise de Dados** | pandas, numpy, scipy |
| **Geração de PDFs** | ReportLab |
| **Exportação Excel** | OpenPyXL |

### Bibliotecas Python

```
streamlit
pandas
numpy
plotly
folium
streamlit-folium
scipy
sqlalchemy
scikit-learn
openpyxl
reportlab
impyla
```

---

## Requisitos

### Sistema

- Python 3.8 ou superior
- Acesso à rede interna da SEF/SC
- Credenciais LDAP válidas

### Hardware Recomendado

- **RAM**: 8GB mínimo (16GB recomendado)
- **CPU**: 4 cores ou superior
- **Armazenamento**: 1GB para aplicação

### Conectividade

- Acesso ao cluster Impala: `bdaworkernode02.sef.sc.gov.br:21050`
- Porta 21050 liberada no firewall
- Certificados SSL/TLS configurados

---

## Instalação

### 1. Clone o repositório

```bash
git clone https://github.com/tiagossevero/GEI_New.git
cd GEI_New
```

### 2. Crie um ambiente virtual (recomendado)

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows
```

### 3. Instale as dependências

```bash
pip install streamlit pandas numpy plotly folium streamlit-folium scipy sqlalchemy scikit-learn openpyxl reportlab impyla
```

### 4. Configure as credenciais

Crie o arquivo `.streamlit/secrets.toml`:

```toml
[impala]
user = "seu_usuario_ldap"
password = "sua_senha_ldap"
```

### 5. Execute a aplicação

```bash
streamlit run "GEI (3).py"
```

A aplicação estará disponível em `http://localhost:8501`

---

## Configuração

### Arquivo de Secrets (`.streamlit/secrets.toml`)

```toml
[impala]
user = "usuario_ldap"
password = "senha_ldap"
```

### Parâmetros de Conexão

Os parâmetros de conexão estão definidos no arquivo principal:

```python
IMPALA_HOST = "bdaworkernode02.sef.sc.gov.br"
IMPALA_PORT = 21050
DATABASE = "gessimples"
```

### Configuração de Cache

O sistema utiliza cache do Streamlit para otimizar performance:

- **Dados principais**: TTL de 1 hora
- **Consultas específicas**: TTL de 5 minutos

---

## Estrutura do Projeto

```
GEI_New/
├── GEI (3).py              # Aplicação principal (~12.000 linhas)
├── GEI.json                # Configurações e metadados
├── README.md               # Esta documentação
├── sql/                    # Scripts SQL
│   ├── gei_dime.sql        # Dados financeiros (Regime Normal)
│   ├── gei_nfcom.sql       # Dados de telecomunicações
│   ├── gei_nf3e.sql        # Dados de energia elétrica
│   └── gei_percent_update.sql  # Cálculos de percentuais
└── .streamlit/
    └── secrets.toml        # Credenciais (não versionado)
```

### Arquitetura da Aplicação

```
┌─────────────────────────────────────────────────────────┐
│                    Interface Streamlit                   │
├─────────────────────────────────────────────────────────┤
│  Menu Principal  │  Dimensões de Análise  │  ML Module  │
├─────────────────────────────────────────────────────────┤
│              Camada de Visualização (Plotly/Folium)     │
├─────────────────────────────────────────────────────────┤
│              Processamento de Dados (pandas/numpy)       │
├─────────────────────────────────────────────────────────┤
│              Machine Learning (scikit-learn)             │
├─────────────────────────────────────────────────────────┤
│              Conexão com Banco (SQLAlchemy/Impyla)       │
├─────────────────────────────────────────────────────────┤
│                    Apache Impala                         │
└─────────────────────────────────────────────────────────┘
```

---

## Funcionalidades

### Menu Principal

O sistema oferece 9 funcionalidades principais:

#### 1. Dashboard Executivo

Visão consolidada de todos os KPIs e métricas principais do sistema.

- Total de grupos econômicos identificados
- Scores médios (GEI e ML)
- Grupos em situação crítica
- Distribuição por nível de risco
- Indicadores de tendência

#### 2. Ranking

Classificação dos grupos econômicos por score de risco.

- Ordenação por múltiplos critérios
- Filtros avançados
- Paginação de resultados
- Visualização de faturamento máximo e quantidade de CNPJs

#### 3. Impacto Fiscal

Estimativa do impacto financeiro da fragmentação empresarial.

- Cálculo do ICMS potencialmente evadido
- Diferencial de alíquota (média de 7%)
- Identificação de grupos acima do limite
- Parâmetros de risco customizáveis

#### 4. Análise Pontual

Análise detalhada de CNPJs específicos ou grupos.

- Pesquisa por CNPJ ou grupo econômico
- Análise multidimensional completa
- Geração de relatórios PDF
- Avaliação de risco detalhada

#### 5. Contadores

Monitoramento de escritórios contábeis associados a grupos suspeitos.

- Ranking de contadores por risco
- Distribuição geográfica (GERFEs)
- Concentração de risco por profissional
- Análise de padrões

#### 6. Dossiê do Grupo

Perfil completo de um grupo econômico selecionado.

- Todas as empresas interconectadas
- Histórico financeiro
- Relacionamentos societários
- Métricas consolidadas
- Exportação em PDF

#### 7. Mapa

Visualização geográfica das empresas e grupos.

- Mapa interativo com Folium
- Distribuição municipal
- Clusters geográficos
- Filtros por região/GERFE

#### 8. Machine Learning

Detecção automática de grupos através de algoritmos de clustering.

- K-Means (agrupamento por similaridade)
- DBSCAN (clustering baseado em densidade)
- Isolation Forest (detecção de anomalias)
- Modo consenso (concordância entre algoritmos)
- Métricas de qualidade do clustering

#### 9. Análises

Módulo de análises estatísticas avançadas.

- Estatísticas descritivas
- Distribuições de risco
- Métricas detalhadas por dimensão
- Comparativos temporais

---

### Dimensões de Análise

O sistema analisa grupos econômicos através de 10 dimensões especializadas:

#### 1. Meios de Pagamento (DIMP)

Análise de pagamentos eletrônicos (cartões, PIX, transferências).

- Padrões de pagamento corporativos vs. sócios
- Concentração de recebimentos
- Índice de risco baseado em anomalias

#### 2. Funcionários (RAIS/CAGED)

Análise da estrutura de empregados.

- Quantidade e distribuição de funcionários
- Receita por funcionário
- Detecção de anomalias trabalhistas
- Integração com bases RAIS/CAGED

#### 3. Convênio 115

Monitoramento de créditos fiscais de energia e telecomunicações.

- Compartilhamento de créditos entre empresas
- Padrões incomuns de redistribuição
- Ranking de risco C115

#### 4. Procuração Bancária (CCS)

Análise de vínculos no sistema financeiro.

- Contas bancárias compartilhadas
- Responsáveis em comum
- Abertura coordenada de contas
- Interconexão financeira

#### 5. Financeiro (PGDAS/DIME)

Dados financeiros dos dois regimes tributários.

- Receita do Simples Nacional (PGDAS)
- Receita do Regime Normal (DIME)
- Cálculos de receita acumulada 12 meses
- Créditos e débitos de ICMS

#### 6. Energia Elétrica (NF3e)

Consumo de energia das empresas.

- Custo de energia vs. faturamento
- Detecção de anomalias de consumo
- Comparação com padrões do setor

#### 7. Telecomunicações (NFCom)

Consumo de serviços de telecom.

- Custos de telecom vs. receita
- Padrões de uso
- Detecção de anomalias

#### 8. Inconsistências NFe/NFCe

Análise de notas fiscais eletrônicas.

- Inconsistências de email, telefone, IP, endereço
- Indicadores de controle centralizado
- Padrões de emissão suspeitos

#### 9. Indícios Fiscais (NEAF)

Indicadores do Núcleo de Estudos em Análise Fiscal.

- Red flags de conformidade fiscal
- Scoring baseado em indícios
- Histórico de irregularidades

#### 10. Vínculos Societários (JUCESC)

Análise de relacionamentos empresariais.

- Sócios compartilhados entre empresas
- Concentração societária
- Padrões de participação
- Dados da Junta Comercial

---

## Machine Learning

### Algoritmos Implementados

#### K-Means

Agrupa empresas por similaridade de características.

```python
# Configuração padrão
n_clusters = auto  # Determinado pelo método do cotovelo
random_state = 42
```

#### DBSCAN

Detecta clusters baseado em densidade, identificando outliers.

```python
# Configuração padrão
eps = auto  # Determinado por análise de k-distância
min_samples = 5
```

#### Isolation Forest

Detecta anomalias isolando observações atípicas.

```python
# Configuração padrão
contamination = 'auto'
random_state = 42
```

### Modo Consenso

Combina os três algoritmos para maior confiabilidade:

- **Consenso Forte**: 3/3 algoritmos concordam
- **Consenso Moderado**: 2/3 algoritmos concordam
- **Consenso Fraco**: 1/3 algoritmo identifica
- **Não-grupo**: Nenhum algoritmo identifica

### Métricas de Qualidade

| Métrica | Descrição | Interpretação |
|---------|-----------|---------------|
| **Silhouette Score** | Coesão e separação dos clusters | -1 a 1 (maior = melhor) |
| **Davies-Bouldin Index** | Similaridade entre clusters | Menor = melhor |
| **Calinski-Harabasz Index** | Razão de dispersão | Maior = melhor |

---

## Fontes de Dados

### Tabelas Principais

| Tabela | Descrição | Limite |
|--------|-----------|--------|
| `gei_percent` | Métricas principais | Sem limite |
| `gei_cnpj` | Catálogo de CNPJs | 50.000 |
| `gei_cadastro` | Cadastro de empresas | 50.000 |
| `gei_contador` | Dados de contadores | Sem limite |
| `gei_socios_compartilhados` | Sócios em comum | 30.000 |
| `gei_c115_ranking_risco_grupo_economico` | Ranking C115 | Sem limite |
| `gei_funcionarios_metricas_grupo` | Métricas de funcionários | Sem limite |
| `gei_pagamentos_metricas_grupo` | Métricas de pagamentos | Sem limite |
| `gei_c115_metricas_grupos` | Métricas C115 | Sem limite |
| `gei_ccs_metricas_grupo` | Métricas bancárias | Sem limite |
| `gei_ccs_ranking_risco` | Ranking CCS | Sem limite |
| `gei_nf3e_metricas_grupo` | Métricas de energia | Sem limite |
| `gei_nfcom_metricas_grupo` | Métricas de telecom | Sem limite |

### Fontes Externas Integradas

- **NFe/NFCe**: Notas fiscais eletrônicas (SPED)
- **RAIS/CAGED**: Registros de emprego (MTE)
- **DIMP**: Declaração de meios de pagamento
- **JUCESC**: Junta Comercial de SC
- **NEAF**: Núcleo de Estudos em Análise Fiscal
- **CCS**: Cadastro de Clientes do Sistema Financeiro

---

## Métricas e Indicadores

### Níveis de Risco

| Nível | Cor | Score |
|-------|-----|-------|
| **Crítico** | Vermelho | > 80 |
| **Alto** | Laranja | 60 - 80 |
| **Médio** | Amarelo | 40 - 60 |
| **Baixo** | Verde | < 40 |

### Indicadores Principais

- **Score GEI**: Pontuação consolidada do sistema
- **Score ML**: Pontuação dos algoritmos de machine learning
- **Índice de Interconexão**: Grau de vínculo entre empresas
- **Impacto Fiscal Estimado**: ICMS potencialmente evadido
- **Índice de Risco C115**: Risco relacionado a créditos fiscais
- **Índice de Risco CCS**: Risco de vínculos bancários
- **Índice de Risco DIMP**: Risco de meios de pagamento

---

## Segurança

### Autenticação

- **Aplicação**: Senha de acesso na entrada
- **Banco de Dados**: Autenticação LDAP
- **Conexão**: SSL/TLS criptografado

### Boas Práticas

- Credenciais armazenadas em `secrets.toml` (não versionado)
- Conexões criptografadas com o banco
- Cache com TTL para dados sensíveis
- Logs de acesso e auditoria

---

## Contribuição

### Como Contribuir

1. Faça um fork do repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/nova-funcionalidade`)
5. Abra um Pull Request

### Padrões de Código

- Siga PEP 8 para código Python
- Documente funções com docstrings
- Mantenha comentários em português
- Teste localmente antes de submeter PR

### Reportando Problemas

Abra uma issue no GitHub com:
- Descrição clara do problema
- Passos para reproduzir
- Comportamento esperado vs. atual
- Screenshots se aplicável

---

## Licença

Este projeto é de uso interno da Secretaria de Estado da Fazenda de Santa Catarina (SEF/SC). Todos os direitos reservados.

---

## Contato

**Receita Estadual de Santa Catarina**
Secretaria de Estado da Fazenda

---

*Última atualização: Janeiro 2026*
