"""
run.py — Script de execução único do pipeline completo.
Alternativa ao Airflow para desenvolvimento local.

Uso:
    python run.py                    # pipeline + dashboard
    python run.py --only-pipeline    # só pipeline
    python run.py --only-dashboard   # só dashboard (requer warehouse existente)
    python run.py --n 5000           # pipeline com 5000 internações
"""

import os
import sys
import subprocess
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT    = os.path.dirname(__file__)
DBT_DIR = os.path.join(ROOT, "dbt_project")


def run_pipeline(n_internacoes: int = 3000) -> bool:
    logger.info("═" * 50)
    logger.info("STEP 1/3 — Ingestão de dados")
    logger.info("═" * 50)
    sys.path.insert(0, ROOT)
    from ingestion.ingest import run_ingestion
    run_ingestion(n_internacoes=n_internacoes)

    logger.info("═" * 50)
    logger.info("STEP 2/3 — Carregando Bronze layer")
    logger.info("═" * 50)
    from ingestion.load_to_duckdb import load_raw_to_bronze
    load_raw_to_bronze()

    logger.info("═" * 50)
    logger.info("STEP 3/3 — dbt run + test")
    logger.info("═" * 50)
    dbt_env = {**os.environ, "DW_PATH": os.path.join(ROOT, "data", "warehouse", "medallion.duckdb")}

    for cmd in [["dbt", "run"], ["dbt", "test"]]:
        result = subprocess.run(
            cmd + ["--profiles-dir", ".", "--project-dir", "."],
            cwd=DBT_DIR, env=dbt_env, capture_output=False,
        )
        if result.returncode != 0:
            logger.error(f"{' '.join(cmd)} falhou com código {result.returncode}")
            return False

    logger.info("✅ Pipeline completo!")
    return True


def run_dashboard():
    logger.info("Iniciando dashboard Streamlit...")
    subprocess.run([
        "streamlit", "run",
        os.path.join(ROOT, "dashboard", "app.py"),
        "--server.port", "8501",
    ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medallion DW Pipeline")
    parser.add_argument("--only-pipeline",  action="store_true")
    parser.add_argument("--only-dashboard", action="store_true")
    parser.add_argument("--n", type=int, default=3000, help="Nº de internações")
    args = parser.parse_args()

    if args.only_dashboard:
        run_dashboard()
    elif args.only_pipeline:
        ok = run_pipeline(args.n)
        sys.exit(0 if ok else 1)
    else:
        ok = run_pipeline(args.n)
        if ok:
            run_dashboard()
