"""
ingestion/load_to_duckdb.py
Carrega os CSVs de data/raw/ no DuckDB como schema 'bronze' (camada de ingestão bruta).
Este script substitui um loader de Airbyte/Fivetran em ambiente local.
"""

import os
import duckdb
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_ROOT  = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(_ROOT, "data", "raw")
DB_PATH = os.path.join(_ROOT, "data", "warehouse", "medallion.duckdb")


def load_raw_to_bronze(db_path: str = DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)

    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")

    sources = {
        "ibge_estados":       "ibge_estados.csv",
        "ibge_municipios":    "ibge_municipios.csv",
        "datasus_internacoes": "datasus_internacoes.csv",
    }

    for table, filename in sources.items():
        path = os.path.join(RAW_DIR, filename)
        if not os.path.exists(path):
            logger.warning(f"Arquivo não encontrado: {path} — pulando.")
            continue

        conn.execute(f"DROP TABLE IF EXISTS bronze.{table}")
        conn.execute(f"""
            CREATE TABLE bronze.{table} AS
            SELECT
                *,
                current_timestamp AS _loaded_at,
                '{filename}'       AS _source_file
            FROM read_csv_auto('{path}', header=true)
        """)
        n = conn.execute(f"SELECT COUNT(*) FROM bronze.{table}").fetchone()[0]
        logger.info(f"  bronze.{table}: {n:,} linhas carregadas")

    conn.close()
    logger.info(f"Bronze schema populado em: {db_path}")


if __name__ == "__main__":
    load_raw_to_bronze()
