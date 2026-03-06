"""
Postgres connection for congestion-pricing app.
Uses either (1) PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE from .env,
or (2) SUPABASE_DB_URL. Load .env before calling get_connection().
"""
import os
from typing import Optional
from urllib.parse import urlparse, unquote

# Optional: load_dotenv() can be called by callers (ingestion scripts / app)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def get_connection():
    """Return a live psycopg2 connection to Postgres."""
    import psycopg2
    # Prefer standard Postgres env vars (e.g. from professor / Supabase connection info)
    host = os.getenv("PGHOST")
    if host:
        port = os.getenv("PGPORT", "5432")
        try:
            port = int(port)
        except (TypeError, ValueError):
            port = 5432
        return psycopg2.connect(
            host=host,
            port=port,
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", ""),
            dbname=os.getenv("PGDATABASE", "postgres"),
        )
    # Fallback: single URL (SUPABASE_DB_URL)
    url = os.getenv("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError(
            "Set either PGHOST, PGUSER, PGPASSWORD, PGDATABASE (and optionally PGPORT) "
            "or SUPABASE_DB_URL in .env."
        )
    parsed = urlparse(url)
    if parsed.hostname in (None, "postgres"):
        raise RuntimeError(
            "SUPABASE_DB_URL was parsed incorrectly (host is %r). "
            "If your password contains @ or #, encode them: @ -> %%40, # -> %%23."
            % (parsed.hostname,)
        )
    password = unquote(parsed.password or "", encoding="latin-1")
    try:
        port = int(parsed.port) if parsed.port else 5432
    except (TypeError, ValueError):
        port = 5432
    return psycopg2.connect(
        host=parsed.hostname,
        port=port,
        user=parsed.username,
        password=password,
        dbname=(parsed.path or "/postgres").lstrip("/") or "postgres",
    )


def get_connection_string() -> Optional[str]:
    """Return SUPABASE_DB_URL for use with libraries that take a URL (e.g. SQLAlchemy)."""
    return os.getenv("SUPABASE_DB_URL")
