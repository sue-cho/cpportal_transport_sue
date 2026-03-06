"""
Create the database tables by running supabase_schema.sql.
Uses the same .env as the ingestion scripts (PGHOST/... or SUPABASE_DB_URL).

Run once: python run_schema.py
"""
from pathlib import Path

from db import get_connection

SCHEMA_FILE = Path(__file__).resolve().parent / "supabase_schema.sql"


def main():
    sql = SCHEMA_FILE.read_text()
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    content = "\n".join(lines)
    conn = get_connection()
    try:
        conn.cursor().execute(content)
        conn.commit()
        print("Schema applied successfully.")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
