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

# ── Garante que o root do projeto está no path ANTES de qualquer import ──────
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

DBT_DIR = os.path.join(ROOT, "dbt_project")
DW_PATH = os.path.join(ROOT, "data", "warehouse", "medallion.duckdb")

# ── Resolve o executável correto (respeita venv ativo) ────────────────────────
PYTHON = sys.executable   # ex: .venv\Scripts\python.exe  ou  .venv/bin/python
IS_WIN = sys.platform == "win32"


def _venv_bin(name: str) -> str:
    """
    Retorna o caminho absoluto do executável dentro do mesmo venv
    que está executando este script.  Isso evita usar o dbt/streamlit
    globais quando há um venv ativo.
    """
    venv_scripts = os.path.dirname(PYTHON)       # Scripts/ (Win) ou bin/ (Unix)
    exe = name + (".exe" if IS_WIN else "")
    full = os.path.join(venv_scripts, exe)
    if os.path.exists(full):
        return full
    # fallback: deixa o PATH resolver (funciona se o venv estiver ativado)
    return name


DBT       = _venv_bin("dbt")
STREAMLIT = _venv_bin("streamlit")


def run_pipeline(n_internacoes: int = 3000) -> bool:

    # ── STEP 1: Ingestão ─────────────────────────────────────────────────────
    logger.info("=" * 52)
    logger.info("STEP 1/3 -- Ingestao de dados")
    logger.info("=" * 52)
    from ingestion.ingest import run_ingestion
    run_ingestion(n_internacoes=n_internacoes)

    # ── STEP 2: Bronze ───────────────────────────────────────────────────────
    logger.info("=" * 52)
    logger.info("STEP 2/3 -- Carregando Bronze layer")
    logger.info("=" * 52)
    from ingestion.load_to_duckdb import load_raw_to_bronze
    load_raw_to_bronze()

    # ── STEP 3: dbt ──────────────────────────────────────────────────────────
    logger.info("=" * 52)
    logger.info("STEP 3/3 -- dbt run + dbt test")
    logger.info("=" * 52)

    # Normaliza separadores para evitar problemas no Windows
    dw_path_norm = DW_PATH.replace("\\", "/")
    dbt_env = {**os.environ, "DW_PATH": dw_path_norm}

    for subcmd in ["run", "test"]:
        cmd = [DBT, subcmd, "--profiles-dir", ".", "--project-dir", "."]
        logger.info("Executando: %s", " ".join(cmd))
        result = subprocess.run(cmd, cwd=DBT_DIR, env=dbt_env)
        if result.returncode != 0:
            logger.error("dbt %s falhou com codigo %d", subcmd, result.returncode)
            return False

    logger.info("Pipeline completo!")
    return True


def run_dashboard():
    logger.info("Iniciando dashboard Streamlit...")
    cmd = [
        STREAMLIT, "run",
        os.path.join(ROOT, "dashboard", "app.py"),
        "--server.port", "8501",
    ]
    logger.info("Executando: %s", " ".join(cmd))
    subprocess.run(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medallion DW Pipeline")
    parser.add_argument("--only-pipeline",  action="store_true",
                        help="Executa apenas o pipeline ETL+dbt")
    parser.add_argument("--only-dashboard", action="store_true",
                        help="Executa apenas o dashboard (warehouse ja existente)")
    parser.add_argument("--n", type=int, default=3000,
                        help="Numero de internacoes a gerar (default: 3000)")
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