# 🏅 Data Warehouse Medallion

> **dbt + DuckDB + Airflow + GitHub Actions**  
> Arquitetura Bronze → Silver → Gold · IBGE + DATASUS · Portfólio de Engenharia de Dados

---

## 🎯 Narrativa do Projeto

> *"Implementei um data warehouse com arquitetura medallion usando dbt para transformações, com testes de qualidade de dados automatizados, lineage documentado e orquestração via Airflow — 100% local e gratuito com DuckDB."*

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                         FONTES DE DADOS                         │
│   IBGE/SIDRA (estados + municípios)  ·  DATASUS/SIH (CID-10)  │
└───────────────────────────┬─────────────────────────────────────┘
                            │ ingestion/
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  🟤 BRONZE  (DuckDB · schema bronze · views)                    │
│  brz_ibge_estados · brz_ibge_municipios · brz_datasus_internac.│
│  → Cast de tipos · colunas de auditoria · sem transformação     │
└───────────────────────────┬─────────────────────────────────────┘
                            │ dbt run
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  ⚪ SILVER  (DuckDB · schema silver · tables)                   │
│  slv_estados — IDH categorizado · quartil PIB · YoY             │
│  slv_municipios — porte · densidade · join UF pai               │
│  slv_internacoes — limpeza · custo/dia · categoria permanência  │
└───────────────────────────┬─────────────────────────────────────┘
                            │ dbt run
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥇 GOLD  (DuckDB · schema gold · tables)                       │
│  gld_saude_por_uf — KPIs + rankings + score performance         │
│  gld_diagnosticos_resumo — CIDs ranqueados (volume/custo/mort.) │
│  gld_evolucao_temporal — série mensal + MM3 + variação MoM      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
              Streamlit Dashboard (dashboard/app.py)
```

---

## 🚀 Início Rápido

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Executar pipeline completo + dashboard
python run.py

# Opções adicionais
python run.py --only-pipeline      # só pipeline
python run.py --only-dashboard     # só dashboard
python run.py --n 5000             # 5.000 internações
```

---

## 🛠️ Stack Técnico

| Componente | Tecnologia |
|---|---|
| **Warehouse** | DuckDB 0.10 (local) / BigQuery (cloud-ready) |
| **Transformação** | dbt-duckdb 1.8 · SQL + Jinja2 |
| **Camadas** | Bronze (views) → Silver (tables) → Gold (tables) |
| **Qualidade** | dbt tests: `not_null`, `unique`, `accepted_values`, `accepted_range` |
| **Orquestração** | Apache Airflow 2.9 · DAGs com TaskGroups |
| **CI/CD** | GitHub Actions · dbt parse → compile → run → test → deploy docs |
| **Dashboard** | Streamlit · Plotly · DuckDB read_only |
| **Macros** | Jinja2 customizados: `surrogate_key`, `classify_idh`, `audit_columns` |

---

## 📁 Estrutura do Projeto

```
dw_medallion/
├── run.py                          # Runner único (pipeline + dashboard)
├── requirements.txt
│
├── ingestion/
│   ├── ingest.py                   # Extrai IBGE + DATASUS → data/raw/
│   └── load_to_duckdb.py           # Carrega CSVs → bronze schema
│
├── dbt_project/
│   ├── dbt_project.yml             # Config principal + variáveis
│   ├── profiles.yml                # Conexão DuckDB (dev/prod)
│   ├── macros/utils.sql            # Macros Jinja2 reutilizáveis
│   └── models/
│       ├── bronze/
│       │   ├── sources.yml         # Declaração das fontes + testes
│       │   ├── brz_ibge_estados.sql
│       │   ├── brz_ibge_municipios.sql
│       │   └── brz_datasus_internacoes.sql
│       ├── silver/
│       │   ├── schema.yml          # Testes de qualidade
│       │   ├── slv_estados.sql
│       │   ├── slv_municipios.sql
│       │   └── slv_internacoes.sql
│       └── gold/
│           ├── schema.yml
│           ├── gld_saude_por_uf.sql
│           ├── gld_diagnosticos_resumo.sql
│           └── gld_evolucao_temporal.sql
│
├── airflow/
│   └── dags/medallion_pipeline.py  # DAG principal (ingest→bronze→dbt→test)
│
├── dashboard/
│   └── app.py                      # Streamlit consumindo gold layer
│
├── .github/
│   └── workflows/dbt_ci.yml        # CI: lint → pipeline → test → deploy docs
│
└── data/
    ├── raw/                         # CSVs ingeridos
    └── warehouse/
        └── medallion.duckdb        # DuckDB warehouse
```

---

## 📊 Modelos dbt — Lineage

```
source:bronze.ibge_estados
    └── brz_ibge_estados
            └── slv_estados
                    ├── slv_municipios
                    │       └── (referência)
                    ├── slv_internacoes
                    │       ├── gld_saude_por_uf ◄── slv_estados
                    │       ├── gld_diagnosticos_resumo
                    │       └── gld_evolucao_temporal
                    └── gld_saude_por_uf

source:bronze.datasus_internacoes
    └── brz_datasus_internacoes
            └── slv_internacoes (ver acima)
```

---

## ✅ Testes de Qualidade (dbt test)

| Camada | Modelo | Testes |
|---|---|---|
| Bronze | `sources.yml` | `not_null`, `unique` (id_aih), `accepted_values` (sexo, obito, UF) |
| Silver | `slv_estados` | `accepted_range` (idh 0-1, pib > 5k, pop > 100k), `accepted_values` (nivel_idh) |
| Silver | `slv_internacoes` | `unique` (id_aih), `accepted_range` (dias 1-365, valor > 0) |
| Gold | `gld_saude_por_uf` | `accepted_range` (taxa_obito 0-100, score > 0) |
| Gold | `gld_diagnosticos_resumo` | `accepted_range` (rank ≥ 1, taxa_obito 0-100) |

---

## 🔄 Airflow DAG

```
medallion_pipeline (schedule: 0 3 * * *)
│
├── [TaskGroup] ingestao
│   ├── extrair_fontes        → IBGE + DATASUS → data/raw/
│   ├── load_bronze_duckdb    → CSVs → bronze schema
│   └── validar_bronze        → contagens mínimas
│
├── [TaskGroup] transformacao_dbt
│   ├── dbt_run               → bronze → silver → gold
│   ├── dbt_test              → qualidade de dados
│   └── dbt_docs_generate     → lineage + catalog
│
└── validar_gold              → assertions no gold layer
```

---

## 🔧 Extensões Sugeridas

- Trocar DuckDB por **BigQuery** (mudar profile, zero código SQL)
- Adicionar **dbt snapshots** para capturar mudanças históricas (SCD Type 2)
- Incluir **dbt exposures** para documentar o dashboard como consumidor
- **Great Expectations** para validações mais complexas
- **dbt-osmosis** para propagação automática de descrições de colunas
- Conectar ao **Metabase** ou **Superset** para BI em cima do gold layer
#   D a t a - W a r e h o u s e - M e d a l l i o n -  
 