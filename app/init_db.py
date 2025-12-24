"""
Initialize the Snowflake Postgres demo database:
- Apply schema
- Seed data
- Create transactional functions
"""

import os
from pathlib import Path

from .db import db_cursor


BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "sql"


def run_sql_file(path: Path):
    with path.open("r", encoding="utf-8") as f:
        sql = f.read()
    with db_cursor() as cur:
        cur.execute(sql)


def main():
    print("Using database:", os.getenv("PGDATABASE", "postgres"))
    files = [
        SQL_DIR / "01_schema.sql",
        SQL_DIR / "02_seed_data.sql",
        SQL_DIR / "03_functions.sql",
        SQL_DIR / "04_kpi_functions.sql",
    ]

    for file in files:
        if not file.exists():
            raise FileNotFoundError(f"Missing SQL file: {file}")
        print(f"Applying {file.name} ...")
        run_sql_file(file)
        print(f"Done: {file.name}")

    print("Database initialization complete.")


if __name__ == "__main__":
    main()


