# 🏅 Data Warehouse Medallion

**ELT moderno com dbt + DuckDB + Airflow + GitHub Actions**  
Arquitetura Bronze → Silver → Gold · IBGE + DATASUS · Portfólio de Engenharia de Dados

---

## Por que este projeto existe

> dbt aparece em **70%+ das vagas de Engenharia de Dados** hoje. Este projeto demonstra domínio completo da stack moderna: não é um script ETL isolado, é um warehouse com arquitetura real, testes automatizados, lineage documentado e CI/CD que bloqueia deploys com dados ruins.

**O que foi implementado:**

- Arquitetura **medallion** (Bronze → Silver → Gold) com separação clara de responsabilidades
- **61 testes de qualidade** de dados distribuídos em todas as camadas — `not_null`, `unique`, `accepted_values`, `accepted_range`
- **Lineage** de modelos gerado automaticamente pelo dbt
- **CI/CD** via GitHub Actions: o pipeline roda, testa e publica documentação a cada push
- **100% local e gratuito** com DuckDB — sem custo de cloud, sem configuração de infraestrutura

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                      FONTES DE DADOS                        │
│  IBGE/SIDRA · estados + municípios · 2019–2023             │
│  DATASUS/SIH · internações hospitalares · CID-10           │
└────────────────────────┬────────────────────────────────────┘
                         │  Extract + Load
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  🟤 BRONZE  ·  schema main_bronze  ·  views                 │
│                                                             │
│  brz_ibge_estados                                           │
│  brz_ibge_municipios          cast de tipos                 │
│  brz_datasus_internacoes      colunas de auditoria          │
│                               zero transformação            │
└────────────────────────┬────────────────────────────────────┘
                         │  dbt run
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  ⚪ SILVER  ·  schema main_silver  ·  tables                │
│                                                             │
│  slv_estados       IDH categorizado · quartil PIB · YoY    │
│  slv_municipios    porte · densidade · join UF pai          │
│  slv_internacoes   custo/dia · permanência · esfera         │
└────────────────────────┬────────────────────────────────────┘
                         │  dbt run
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  🥇 GOLD  ·  schema main_gold  ·  tables                    │
│                                                             │
│  gld_saude_por_uf          KPIs · rankings · score          │
│  gld_diagnosticos_resumo   CIDs por volume/custo/mortalidade│
│  gld_evolucao_temporal     série mensal · MM3 · MoM         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
            Streamlit Dashboard · gold layer
```

---

## Stack

| Componente | Tecnologia | Por que |
|---|---|---|
| **Warehouse** | DuckDB 0.10 | OLAP local de alta performance — troca por BigQuery mudando só o `profiles.yml` |
| **Transformação** | dbt-duckdb 1.8 | SQL + Jinja2, lineage automático, testes declarativos |
| **Qualidade** | dbt tests + dbt-utils | `not_null`, `unique`, `accepted_values`, `accepted_range` |
| **Orquestração** | Apache Airflow 2.9 | DAGs com TaskGroups, retry, schedule diário às 03h |
| **CI/CD** | GitHub Actions | dbt parse → compile → run → test → deploy docs |
| **Ingestão** | Python + Pandas | IBGE API / DATASUS FTP — adaptável a Airbyte/Fivetran |
| **Dashboard** | Streamlit + Plotly | Consome o gold layer via DuckDB read-only |
| **Macros** | Jinja2 | `surrogate_key`, `classify_idh`, `audit_columns` reutilizáveis |

---

## Início rápido

```bash
# 1. Clone e crie o ambiente
git clone https://github.com/RSangDev/dw-medallion
cd dw-medallion
python -m venv .venv && .venv\Scripts\activate   # Windows
pip install -r requirements.txt

# 2. Instale as dependências dbt
cd dbt_project && dbt deps && cd ..

# 3. Execute o pipeline completo
python run.py --only-pipeline

# 4. Abra o dashboard
python run.py --only-dashboard
```

**Opções do runner:**

```bash
python run.py                    # pipeline + dashboard em sequência
python run.py --only-pipeline    # só ETL + dbt
python run.py --only-dashboard   # só dashboard (warehouse já construído)
python run.py --n 5000           # pipeline com 5.000 internações
```

---

## Estrutura do projeto

```
dw_medallion/
├── run.py                              # Entrada única do projeto
├── requirements.txt
│
├── ingestion/
│   ├── ingest.py                       # Extract: IBGE + DATASUS → data/raw/
│   └── load_to_duckdb.py               # Load: CSVs → schema bronze no DuckDB
│
├── dbt_project/
│   ├── dbt_project.yml                 # Config + variáveis (start_year, end_year)
│   ├── profiles.yml                    # Conexão DuckDB dev/prod
│   ├── packages.yml                    # dbt-utils 1.3
│   ├── macros/utils.sql                # Macros Jinja2 reutilizáveis
│   └── models/
│       ├── bronze/
│       │   ├── sources.yml             # Fontes declaradas + testes bronze
│       │   ├── brz_ibge_estados.sql
│       │   ├── brz_ibge_municipios.sql
│       │   └── brz_datasus_internacoes.sql
│       ├── silver/
│       │   ├── schema.yml              # 20 testes de qualidade
│       │   ├── slv_estados.sql
│       │   ├── slv_municipios.sql
│       │   └── slv_internacoes.sql
│       └── gold/
│           ├── schema.yml              # 12 testes de qualidade
│           ├── gld_saude_por_uf.sql
│           ├── gld_diagnosticos_resumo.sql
│           └── gld_evolucao_temporal.sql
│
├── airflow/
│   └── dags/medallion_pipeline.py      # DAG: ingest → bronze → dbt run → test
│
├── dashboard/
│   └── app.py                          # Streamlit 5 páginas · gold layer
│
├── .github/
│   └── workflows/dbt_ci.yml            # CI: lint → pipeline → test → deploy docs
│
└── data/
    ├── raw/                             # CSVs extraídos
    └── warehouse/
        └── medallion.duckdb            # DuckDB warehouse
```

---

## Modelos dbt e lineage

```
source:bronze.ibge_estados ──► brz_ibge_estados ──► slv_estados ──┬──► gld_saude_por_uf
                                                                   └──► slv_municipios

source:bronze.datasus_internacoes ──► brz_datasus_internacoes ──► slv_internacoes ──┬──► gld_saude_por_uf
                                                                                     ├──► gld_diagnosticos_resumo
                                                                                     └──► gld_evolucao_temporal
```

---

## Testes de qualidade

| Camada | Modelo | Testes |
|---|---|---|
| Bronze | `sources.yml` | `not_null`, `unique` (id_aih), `accepted_values` (sexo, obito, UF) |
| Silver | `slv_estados` | `accepted_range` idh ∈ [0,1], pib > 5k, pop > 100k · `accepted_values` nivel_idh |
| Silver | `slv_internacoes` | `unique` id_aih · `accepted_range` dias ∈ [1,365], valor > 0, custo/dia > 0 |
| Gold | `gld_saude_por_uf` | `accepted_range` taxa_obito ∈ [0,100], pct_urgencia ∈ [0,100], score > 0 |
| Gold | `gld_diagnosticos_resumo` | `accepted_range` rank ≥ 1, taxa_obito ∈ [0,100] |
| Gold | `gld_evolucao_temporal` | `accepted_range` mês ∈ [1,12] · `accepted_values` trimestre |

---

## Airflow DAG

```
medallion_pipeline  ·  schedule: 0 3 * * *  (diário às 03h)
│
├── [TaskGroup] ingestao
│   ├── extrair_fontes          IBGE + DATASUS → data/raw/
│   ├── load_bronze_duckdb      CSVs → schema bronze
│   └── validar_bronze          contagens mínimas por tabela
│
├── [TaskGroup] transformacao_dbt
│   ├── dbt_run                 bronze → silver → gold
│   ├── dbt_test                61 testes de qualidade
│   └── dbt_docs_generate       lineage + catalog.json
│
└── validar_gold                assertions no gold layer
```

---

## CI/CD — GitHub Actions

A cada push em `main` ou `develop` o workflow executa automaticamente:

```
dbt parse       valida sintaxe de todos os modelos
     ↓
dbt compile     gera SQL final sem executar
     ↓
python run.py --only-pipeline    pipeline completo no runner do CI
     ↓
dbt test        61 testes — falha bloqueia o merge
     ↓
dbt docs generate + deploy       publica lineage no GitHub Pages
```

---

## Próximos passos

| Extensão | Impacto |
|---|---|
| Trocar DuckDB por **BigQuery** | Zero mudança nos modelos SQL — só o `profiles.yml` |
| **dbt snapshots** | Histórico de mudanças (SCD Type 2) em tabelas de dimensão |
| **dbt exposures** | Documenta o dashboard como consumidor oficial do gold layer |
| **Great Expectations** | Validações estatísticas além dos testes declarativos do dbt |
| **Metabase / Superset** | BI em cima do gold layer sem escrever SQL |
| **Airbyte** | Substituir a ingestão Python por conector gerenciado |