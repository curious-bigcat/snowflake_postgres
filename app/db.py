import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    """
    Create a new connection using environment variables:

    - PGHOST
    - PGPORT
    - PGUSER
    - PGPASSWORD
    - PGDATABASE (defaults to 'postgres')
    """
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        dbname=os.getenv("PGDATABASE", "postgres"),
    )
    return conn


@contextmanager
def db_cursor(dict_cursor: bool = False):
    """
    Context manager that yields a cursor and commits/rolls back appropriately.
    """
    conn = get_connection()
    try:
        cursor_factory = RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()


