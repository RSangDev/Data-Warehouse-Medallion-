"""
airflow/dags/medallion_pipeline.py
DAG principal do pipeline medallion.
Orquestra: Ingestão → Load DuckDB → dbt run → dbt test → notificação.

Executar Airflow localmente:
    pip install apache-airflow
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow db init
    airflow standalone
"""

from __future__ import annotations

import os
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
DBT_DIR      = PROJECT_ROOT / "dbt_project"
INGEST_DIR   = PROJECT_ROOT / "ingestion"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


# ─── Funções Python para os operadores ──────────────────────────
def task_ingest(**context):
    """Extrai dados do IBGE e DATASUS e salva em data/raw/."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from ingestion.ingest import run_ingestion
    result = run_ingestion(n_internacoes=3000)
    rows = {k: len(v) for k, v in result.items()}
    logger.info(f"Ingestão concluída: {rows}")
    context["ti"].xcom_push(key="row_counts", value=rows)


def task_load_bronze(**context):
    """Carrega CSVs raw no schema bronze do DuckDB."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from ingestion.load_to_duckdb import load_raw_to_bronze
    load_raw_to_bronze()
    logger.info("Bronze carregado com sucesso.")


def task_validate_bronze(**context):
    """Valida contagens mínimas no bronze antes de rodar o dbt."""
    import duckdb
    db_path = PROJECT_ROOT / "data" / "warehouse" / "medallion.duckdb"
    conn = duckdb.connect(str(db_path))

    checks = {
        "bronze.ibge_estados":       50,
        "bronze.ibge_municipios":    100,
        "bronze.datasus_internacoes": 500,
    }
    failed = []
    for table, min_rows in checks.items():
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if n < min_rows:
            failed.append(f"{table}: {n} < {min_rows}")
        else:
            logger.info(f"  ✓ {table}: {n:,} linhas")

    conn.close()
    if failed:
        raise ValueError(f"Validação bronze falhou: {failed}")
    logger.info("Todas as validações bronze passaram.")


def task_dbt_run(**context):
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--project-dir", "."],
        cwd=str(DBT_DIR),
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run falhou:\n{result.stderr}")


def task_dbt_test(**context):
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", ".", "--project-dir", "."],
        cwd=str(DBT_DIR),
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test falhou:\n{result.stderr}")


def task_dbt_docs(**context):
    """Gera documentação dbt (lineage graph)."""
    result = subprocess.run(
        ["dbt", "docs", "generate", "--profiles-dir", ".", "--project-dir", "."],
        cwd=str(DBT_DIR),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.warning(f"dbt docs generate falhou (não crítico): {result.stderr}")
    else:
        logger.info("Documentação dbt gerada.")


def task_validate_gold(**context):
    """Valida camada gold após dbt run."""
    import duckdb
    db_path = PROJECT_ROOT / "data" / "warehouse" / "medallion.duckdb"
    conn = duckdb.connect(str(db_path))

    checks = [
        ("SELECT COUNT(*) FROM gold.gld_saude_por_uf",     10),
        ("SELECT COUNT(*) FROM gold.gld_diagnosticos_resumo", 5),
        ("SELECT COUNT(*) FROM gold.gld_evolucao_temporal",  10),
        ("SELECT COUNT(*) FROM gold.gld_saude_por_uf WHERE score_performance < 0", 0),
    ]

    for query, expected_min in checks:
        n = conn.execute(query).fetchone()[0]
        assert n >= expected_min, f"Falha: {query} → {n} < {expected_min}"
        logger.info(f"  ✓ {query.split('FROM')[1].strip()}: {n}")

    conn.close()
    logger.info("Gold layer validado.")


# ─── DAG ─────────────────────────────────────────────────────────
with DAG(
    dag_id="medallion_pipeline",
    description="Pipeline Bronze→Silver→Gold: IBGE + DATASUS via dbt + DuckDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *",   # diário às 03h00
    catchup=False,
    tags=["medallion", "dbt", "duckdb", "ibge", "datasus"],
    doc_md="""
## 🏅 Medallion Pipeline

Pipeline de dados em arquitetura medallion usando **dbt + DuckDB**.

### Fontes
- **IBGE/SIDRA**: dados socioeconômicos por UF e município (2019–2023)
- **DATASUS/SIH**: internações hospitalares (CID-10)

### Camadas
| Camada | Schema | Descrição |
|--------|--------|-----------|
| Bronze | `bronze` | Dados brutos — ingestão pura |
| Silver | `silver` | Dados limpos, tipados e enriquecidos |
| Gold   | `gold`   | Agregações analíticas — consumo por dashboards |

### Modelos dbt
- `slv_estados`, `slv_municipios`, `slv_internacoes`
- `gld_saude_por_uf`, `gld_diagnosticos_resumo`, `gld_evolucao_temporal`
    """,
) as dag:

    # ── Grupo: Ingestão ──────────────────────────────────────
    with TaskGroup("ingestao", tooltip="Extrai e carrega dados brutos") as tg_ingest:
        t_ingest = PythonOperator(
            task_id="extrair_fontes",
            python_callable=task_ingest,
            doc_md="Extrai IBGE + DATASUS → CSV em data/raw/",
        )
        t_bronze = PythonOperator(
            task_id="load_bronze_duckdb",
            python_callable=task_load_bronze,
            doc_md="Carrega CSVs → bronze schema no DuckDB",
        )
        t_validate_bronze = PythonOperator(
            task_id="validar_bronze",
            python_callable=task_validate_bronze,
            doc_md="Verifica contagens mínimas no bronze",
        )
        t_ingest >> t_bronze >> t_validate_bronze

    # ── Grupo: Transformação dbt ─────────────────────────────
    with TaskGroup("transformacao_dbt", tooltip="dbt run + test") as tg_dbt:
        t_dbt_run = PythonOperator(
            task_id="dbt_run",
            python_callable=task_dbt_run,
            doc_md="Executa todos os modelos dbt (bronze→silver→gold)",
        )
        t_dbt_test = PythonOperator(
            task_id="dbt_test",
            python_callable=task_dbt_test,
            doc_md="Roda os testes de qualidade de dados do dbt",
        )
        t_dbt_docs = PythonOperator(
            task_id="dbt_docs_generate",
            python_callable=task_dbt_docs,
            doc_md="Gera documentação e lineage graph do dbt",
        )
        t_dbt_run >> t_dbt_test >> t_dbt_docs

    # ── Validação final ──────────────────────────────────────
    t_validate_gold = PythonOperator(
        task_id="validar_gold",
        python_callable=task_validate_gold,
        doc_md="Valida integridade da camada gold",
    )

    # ── Dependências ─────────────────────────────────────────
    tg_ingest >> tg_dbt >> t_validate_gold
