"""
include/db.py
Shared DB connection helpers — used by all DAGs.

Source : PostgreSQL (OLTP)  — env prefix SOURCE_DB_*
DWH    : PostgreSQL          — env prefix DWH_*

Watermark fix: now uses >= (was >) to avoid missing rows
with identical updated_at timestamps.
"""

from __future__ import annotations

import contextlib
import os
from datetime import datetime, timezone
from typing import Generator

import psycopg2
import psycopg2.extras

SCHEMA_RAW   = "kumparan_raw"
SCHEMA_INT   = "kumparan_intermediate"
SCHEMA_DWH   = "kumparan_dwh"
SCHEMA_MART  = "kumparan_mart"


# ── Source (OLTP) ────────────────────────────────────────────

def get_source_conn():
    return psycopg2.connect(
        host=os.environ["SOURCE_DB_HOST"],
        port=int(os.environ.get("SOURCE_DB_PORT", 5432)),
        dbname=os.environ["SOURCE_DB_NAME"],
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
        connect_timeout=10,
    )


@contextlib.contextmanager
def source_cursor(dict_cursor: bool = True) -> Generator:
    conn = get_source_conn()
    factory = psycopg2.extras.RealDictCursor if dict_cursor else None
    try:
        with conn.cursor(cursor_factory=factory) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── DWH ──────────────────────────────────────────────────────

def get_dwh_conn(autocommit: bool = False):
    conn = psycopg2.connect(
        host=os.environ["DWH_HOST"],
        port=int(os.environ.get("DWH_PORT", 5432)),
        dbname=os.environ["DWH_NAME"],
        user=os.environ["DWH_USER"],
        password=os.environ["DWH_PASSWORD"],
        connect_timeout=10,
    )
    conn.autocommit = autocommit
    return conn


@contextlib.contextmanager
def dwh_cursor(dict_cursor: bool = False) -> Generator:
    conn = get_dwh_conn(autocommit=False)
    factory = psycopg2.extras.RealDictCursor if dict_cursor else None
    try:
        with conn.cursor(cursor_factory=factory) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Watermark helpers ─────────────────────────────────────────
# FIX: was returning datetime without timezone → comparison issues
# FIX: default fallback now returns tz-aware datetime

def get_watermark(dag_id: str, table_name: str) -> datetime:
    """Return the last successful extraction timestamp for a DAG+table."""
    with dwh_cursor(dict_cursor=True) as cur:
        cur.execute(
            f"""SELECT last_updated_at
                FROM {SCHEMA_DWH}.etl_watermark
                WHERE dag_id = %s AND table_name = %s""",
            (dag_id, table_name),
        )
        row = cur.fetchone()
    if row:
        ts = row["last_updated_at"]
        # ensure tz-aware
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    return datetime(2016, 1, 1, tzinfo=timezone.utc)


def set_watermark(dag_id: str, table_name: str, ts: datetime) -> None:
    """Upsert the watermark for a DAG+table."""
    with dwh_cursor() as cur:
        cur.execute(
            f"""INSERT INTO {SCHEMA_DWH}.etl_watermark
                    (dag_id, table_name, last_updated_at, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (dag_id, table_name)
                DO UPDATE SET last_updated_at = EXCLUDED.last_updated_at,
                              updated_at = NOW()""",
            (dag_id, table_name, ts),
        )
