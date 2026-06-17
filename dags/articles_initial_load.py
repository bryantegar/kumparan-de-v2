"""
dags/articles_initial_load.py
ONE-TIME historical backfill from 2016-01-01 → now.

Params:
  start_date : "2016-01-01"  (default)
  end_date   : ""  → uses today

Processing:
  - Populates dim_date (2016-2036)
  - Backfills authors, readers, articles, impressions
    month-by-month to avoid memory spikes
  - Sets watermarks so hourly DAG continues from here
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models.param import Param

sys.path.insert(0, "/opt/airflow/include")
from db import (
    get_source_conn, get_dwh_conn,
    set_watermark,
    SCHEMA_RAW, SCHEMA_INT, SCHEMA_DWH, SCHEMA_MART,
)
# FIX: shared helpers from utils.py — no more duplication
from utils import age_group as _age_group, follower_tier as _follower_tier
from utils import edu_level as _edu_level, device_class as _device_class
import psycopg2.extras

log        = logging.getLogger(__name__)
CHUNK      = 5_000
HOURLY_DAG = "articles_etl_hourly"

def _month_ranges(start: datetime, end: datetime):
    cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur < end:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield cur, min(nxt, end)
        cur = nxt


@dag(
    dag_id="articles_initial_load",
    schedule=None,               # triggered manually once
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param("2016-01-01", type="string",
                            description="Backfill start date (YYYY-MM-DD)"),
        "end_date":   Param("", type="string",
                            description="Backfill end date — leave empty for today"),
    },
    tags=["kumparan", "backfill", "one-time"],
)
def articles_initial_load():

    # ── 1. dim_date ──────────────────────────────────────────────────────────
    @task()
    def populate_dim_date() -> None:
        start = datetime(2016, 1, 1).date()
        end   = datetime(2036, 1, 1).date()
        rows, d = [], start
        while d < end:
            dow = d.isoweekday()
            rows.append((
                int(d.strftime("%Y%m%d")), d, dow, d.strftime("%A"),
                d.day, d.timetuple().tm_yday, int(d.strftime("%W")),
                d.month, d.strftime("%B"), (d.month-1)//3+1, d.year, dow >= 6,
            ))
            d += timedelta(days=1)

        dwh = get_dwh_conn()
        try:
            with dwh.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {SCHEMA_DWH}.dim_date CASCADE")
                for i in range(0, len(rows), CHUNK):
                    cur.executemany(
                        f"""INSERT INTO {SCHEMA_DWH}.dim_date
                            (date_key, full_date, day_of_week, day_name,
                             day_of_month, day_of_year, week_of_year,
                             month_num, month_name, quarter, year, is_weekend)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (date_key) DO NOTHING""",
                        rows[i:i+CHUNK],
                    )
            dwh.commit()
        except Exception:
            dwh.rollback(); raise
        finally:
            dwh.close()
        log.info("dim_date: %d rows (2016–2036)", len(rows))


    # ── 2. authors full load ─────────────────────────────────────────────────
    @task()
    def load_authors() -> int:
        src = get_source_conn()
        dwh = get_dwh_conn()
        try:
            with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT author_id, author_name, author_age, author_city,
                              author_province, author_education, specialty,
                              is_verified, follower_count, joined_at
                       FROM authors ORDER BY author_id"""
                )
                rows = cur.fetchall()

            with dwh.cursor() as cur:
                for i in range(0, len(rows), CHUNK):
                    cur.executemany(
                        f"""INSERT INTO {SCHEMA_DWH}.dim_author
                            (author_id, author_name, author_age, age_group,
                             author_city, author_province, author_education, edu_level,
                             specialty, is_verified, follower_count, follower_tier,
                             joined_at, dw_created_at, dw_updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                            ON CONFLICT (author_id) DO UPDATE SET
                                author_name=EXCLUDED.author_name,
                                follower_count=EXCLUDED.follower_count,
                                follower_tier=EXCLUDED.follower_tier,
                                dw_updated_at=NOW()""",
                        [(r["author_id"], r["author_name"], r["author_age"],
                          _age_group(r["author_age"]),
                          r["author_city"], r["author_province"],
                          r["author_education"], _edu_level(r["author_education"]),
                          r["specialty"], r["is_verified"], r["follower_count"],
                          _follower_tier(r["follower_count"]), r["joined_at"])
                         for r in rows[i:i+CHUNK]],
                    )
            dwh.commit()
            log.info("Loaded %d authors → dim_author", len(rows))
            return len(rows)
        except Exception:
            dwh.rollback(); raise
        finally:
            src.close(); dwh.close()


    # ── 3. readers full load ─────────────────────────────────────────────────
    @task()
    def load_readers() -> int:
        src = get_source_conn()
        dwh = get_dwh_conn()
        try:
            with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT reader_id, reader_age, reader_city, reader_province,
                              membership_type, preferred_device, registered_at
                       FROM readers ORDER BY reader_id"""
                )
                rows = cur.fetchall()

            with dwh.cursor() as cur:
                for i in range(0, len(rows), CHUNK):
                    cur.executemany(
                        f"""INSERT INTO {SCHEMA_DWH}.dim_reader
                            (reader_id, reader_age, age_group, reader_city, reader_province,
                             membership_type, preferred_device, device_class,
                             registered_at, dw_created_at, dw_updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                            ON CONFLICT (reader_id) DO UPDATE SET
                                reader_age=EXCLUDED.reader_age,
                                membership_type=EXCLUDED.membership_type,
                                dw_updated_at=NOW()""",
                        [(r["reader_id"], r["reader_age"], _age_group(r["reader_age"]),
                          r["reader_city"], r["reader_province"],
                          r["membership_type"], r["preferred_device"],
                          _device_class(r["preferred_device"]), r["registered_at"])
                         for r in rows[i:i+CHUNK]],
                    )
            dwh.commit()
            log.info("Loaded %d readers → dim_reader", len(rows))
            return len(rows)
        except Exception:
            dwh.rollback(); raise
        finally:
            src.close(); dwh.close()


    # ── 4. articles backfill month-by-month ──────────────────────────────────
    @task()
    def backfill_articles(**context) -> dict:
        p         = context["params"]
        start     = datetime.fromisoformat(p["start_date"]).replace(tzinfo=timezone.utc)
        end_raw   = (p.get("end_date") or "").strip()
        end       = (datetime.fromisoformat(end_raw).replace(tzinfo=timezone.utc)
                     if end_raw else datetime.now(tz=timezone.utc))
        log.info("Articles backfill: %s → %s", start.date(), end.date())

        src   = get_source_conn()
        dwh   = get_dwh_conn()
        total = 0
        try:
            for b_start, b_end in _month_ranges(start, end):
                with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """SELECT id, title, content, category, published_at, author_id,
                                  created_at, updated_at, deleted_at
                           FROM articles
                           WHERE updated_at >= %s AND updated_at < %s
                           ORDER BY updated_at""",
                        (b_start, b_end),
                    )
                    rows = cur.fetchall()

                if not rows:
                    continue

                with dwh.cursor() as cur:
                    for i in range(0, len(rows), CHUNK):
                        chunk = rows[i:i+CHUNK]
                        # raw append
                        cur.executemany(
                            f"""INSERT INTO {SCHEMA_RAW}.articles
                                (id, title, content, category, published_at, author_id,
                                 created_at, updated_at, deleted_at, _batch_id)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [(r["id"],r["title"],r["content"],r["category"],
                              r["published_at"],r["author_id"],r["created_at"],
                              r["updated_at"],r["deleted_at"],
                              b_start.strftime("%Y%m")) for r in chunk],
                        )
                        # intermediate
                        cur.executemany(
                            f"""INSERT INTO {SCHEMA_INT}.articles
                                (id, title, content, category, word_count, published_at,
                                 author_id, created_at, updated_at, deleted_at,
                                 days_to_publish)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (id) DO UPDATE SET
                                    updated_at=EXCLUDED.updated_at,
                                    deleted_at=EXCLUDED.deleted_at,
                                    _dw_updated_at=NOW()""",
                            [(r["id"],r["title"],r["content"],r["category"],
                              len((r["content"] or "").split()),
                              r["published_at"],r["author_id"],r["created_at"],
                              r["updated_at"],r["deleted_at"],
                              (r["published_at"]-r["created_at"]).days
                              if r["published_at"] and r["created_at"] else None)
                             for r in chunk],
                        )
                        # gold dim_article
                        cur.executemany(
                            f"""INSERT INTO {SCHEMA_DWH}.dim_article
                                (article_id, title, category, word_count, author_key,
                                 published_date_key, created_date_key, days_to_publish,
                                 is_deleted, deleted_at, dw_created_at, dw_updated_at)
                                SELECT %s,%s,%s,%s,da.author_key,
                                    TO_CHAR(%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                                    TO_CHAR(%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                                    %s, %s::BOOLEAN, %s::TIMESTAMPTZ, NOW(), NOW()
                                FROM {SCHEMA_DWH}.dim_author da WHERE da.author_id=%s
                                ON CONFLICT (article_id) DO UPDATE SET
                                    title=EXCLUDED.title, category=EXCLUDED.category,
                                    is_deleted=EXCLUDED.is_deleted,
                                    deleted_at=EXCLUDED.deleted_at, dw_updated_at=NOW()""",
                            [(r["id"],r["title"],r["category"],
                              len((r["content"] or "").split()),
                              r["published_at"],r["created_at"],
                              (r["published_at"]-r["created_at"]).days
                              if r["published_at"] and r["created_at"] else None,
                              r["deleted_at"] is not None, r["deleted_at"],
                              r["author_id"]) for r in chunk],
                        )
                        # gold fact_article_activity
                        cur.executemany(
                            f"""INSERT INTO {SCHEMA_DWH}.fact_article_activity
                                (article_key, author_key, published_date_key, created_date_key,
                                 updated_date_key, content_length, word_count, days_to_publish,
                                 is_published, is_deleted, dw_created_at, dw_updated_at)
                                SELECT da.article_key, dau.author_key,
                                    TO_CHAR(%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                                    TO_CHAR(%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                                    TO_CHAR(%s::TIMESTAMPTZ AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                                    %s,%s,%s,%s,%s,NOW(),NOW()
                                FROM {SCHEMA_DWH}.dim_article da
                                JOIN {SCHEMA_DWH}.dim_author  dau ON dau.author_id=%s
                                WHERE da.article_id=%s""",
                            [(r["published_at"],r["created_at"],r["updated_at"],
                              len((r["content"] or "")),
                              len((r["content"] or "").split()),
                              (r["published_at"]-r["created_at"]).days
                              if r["published_at"] and r["created_at"] else None,
                              r["published_at"] is not None,
                              r["deleted_at"] is not None,
                              r["author_id"],r["id"]) for r in chunk],
                        )
                    dwh.commit()
                total += len(rows)
                log.info("Batch %s→%s: %d rows (total %d)", b_start.date(), b_end.date(), len(rows), total)

        except Exception:
            dwh.rollback(); raise
        finally:
            src.close(); dwh.close()

        return {"total": total, "end_ts": end.isoformat()}


    # ── 5. impressions backfill month-by-month ───────────────────────────────
    @task()
    def backfill_impressions(**context) -> dict:
        p       = context["params"]
        start   = datetime.fromisoformat(p["start_date"]).replace(tzinfo=timezone.utc)
        end_raw = (p.get("end_date") or "").strip()
        end     = (datetime.fromisoformat(end_raw).replace(tzinfo=timezone.utc)
                   if end_raw else datetime.now(tz=timezone.utc))
        log.info("Impressions backfill: %s → %s", start.date(), end.date())

        src   = get_source_conn()
        dwh   = get_dwh_conn()
        total = 0
        try:
            for b_start, b_end in _month_ranges(start, end):
                with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """SELECT impression_id, article_id, reader_id, author_id,
                                  read_date, read_at, device_type, platform,
                                  read_duration_sec, is_completed
                           FROM article_impressions
                           WHERE read_at >= %s AND read_at < %s
                           ORDER BY read_at""",
                        (b_start, b_end),
                    )
                    rows = cur.fetchall()

                if not rows:
                    continue

                with dwh.cursor() as cur:
                    for i in range(0, len(rows), CHUNK):
                        chunk = rows[i:i+CHUNK]
                        # raw
                        cur.executemany(
                            f"""INSERT INTO {SCHEMA_RAW}.article_impressions
                                (impression_id, article_id, reader_id, author_id,
                                 read_date, read_at, device_type, platform,
                                 read_duration_sec, is_completed, _batch_id)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [(r["impression_id"],r["article_id"],r["reader_id"],r["author_id"],
                              r["read_date"],r["read_at"],r["device_type"],r["platform"],
                              r["read_duration_sec"],r["is_completed"],
                              b_start.strftime("%Y%m")) for r in chunk],
                        )
                        # intermediate
                        cur.executemany(
                            f"""INSERT INTO {SCHEMA_INT}.article_impressions
                                (impression_id, article_id, reader_id, author_id,
                                 read_date, read_at, device_type, device_class, platform,
                                 read_duration_sec, read_duration_min, is_completed,
                                 hour_of_day, day_of_week)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (impression_id) DO NOTHING""",
                            [(r["impression_id"],r["article_id"],r["reader_id"],r["author_id"],
                              r["read_date"],r["read_at"],r["device_type"],
                              _device_class(r["device_type"]),r["platform"],
                              r["read_duration_sec"],
                              round(r["read_duration_sec"]/60,2) if r["read_duration_sec"] else None,
                              r["is_completed"],
                              r["read_at"].hour if r["read_at"] else None,
                              r["read_at"].isoweekday() if r["read_at"] else None)
                             for r in chunk],
                        )
                        # gold fact
                        cur.executemany(
                            f"""INSERT INTO {SCHEMA_DWH}.fact_article_impression
                                (impression_id, article_key, reader_key, author_key,
                                 read_date_key, read_at, device_type, device_class, platform,
                                 read_duration_sec, read_duration_min, is_completed,
                                 hour_of_day, day_of_week, dw_created_at, dw_updated_at)
                                SELECT %s, da.article_key, dr.reader_key, dau.author_key,
                                    TO_CHAR(%s::DATE,'YYYYMMDD')::INT,
                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW()
                                FROM {SCHEMA_DWH}.dim_article da
                                JOIN {SCHEMA_DWH}.dim_reader  dr  ON dr.reader_id=%s
                                JOIN {SCHEMA_DWH}.dim_author  dau ON dau.author_id=%s
                                WHERE da.article_id=%s
                                ON CONFLICT (impression_id) DO NOTHING""",
                            [(r["impression_id"],r["read_date"],r["read_at"],
                              r["device_type"],_device_class(r["device_type"]),r["platform"],
                              r["read_duration_sec"],
                              round(r["read_duration_sec"]/60,2) if r["read_duration_sec"] else None,
                              r["is_completed"],
                              r["read_at"].hour if r["read_at"] else None,
                              r["read_at"].isoweekday() if r["read_at"] else None,
                              r["reader_id"],r["author_id"],r["article_id"]) for r in chunk],
                        )
                    dwh.commit()
                total += len(rows)
                log.info("Batch %s→%s: %d impression rows", b_start.date(), b_end.date(), len(rows))

        except Exception:
            dwh.rollback(); raise
        finally:
            src.close(); dwh.close()

        return {"total": total, "end_ts": end.isoformat()}


    # ── 6. build mart from scratch ───────────────────────────────────────────
    @task()
    def build_mart_initial(imp_meta: dict) -> None:
        log.info("Building mart_article from scratch...")
        dwh = get_dwh_conn()
        try:
            with dwh.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {SCHEMA_MART}.mart_article")
                cur.execute(
                    f"""INSERT INTO {SCHEMA_MART}.mart_article
                        (read_date, read_date_key, category, author_province,
                         reader_province, device_class, platform, membership_type,
                         total_reads, unique_articles, unique_readers,
                         avg_read_duration_sec, completed_reads, completion_rate,
                         dw_created_at, dw_updated_at)
                        SELECT
                            dd.full_date, f.read_date_key,
                            da.category, dau.author_province, dr.reader_province,
                            f.device_class, f.platform, dr.membership_type,
                            COUNT(*),
                            COUNT(DISTINCT f.article_key),
                            COUNT(DISTINCT f.reader_key),
                            ROUND(AVG(f.read_duration_sec),2),
                            SUM(CASE WHEN f.is_completed THEN 1 ELSE 0 END),
                            ROUND(SUM(CASE WHEN f.is_completed THEN 1 ELSE 0 END)::NUMERIC
                                  / NULLIF(COUNT(*),0), 4),
                            NOW(), NOW()
                        FROM {SCHEMA_DWH}.fact_article_impression f
                        JOIN {SCHEMA_DWH}.dim_date    dd  ON dd.date_key    = f.read_date_key
                        JOIN {SCHEMA_DWH}.dim_article da  ON da.article_key = f.article_key
                        JOIN {SCHEMA_DWH}.dim_author  dau ON dau.author_key = f.author_key
                        JOIN {SCHEMA_DWH}.dim_reader  dr  ON dr.reader_key  = f.reader_key
                        GROUP BY 1,2,3,4,5,6,7,8"""
                )
            dwh.commit()
            log.info("mart_article built.")
        except Exception:
            dwh.rollback(); raise
        finally:
            dwh.close()


    # ── 7. set watermarks ────────────────────────────────────────────────────
    @task()
    def finalize(art_meta: dict, imp_meta: dict) -> None:
        now = datetime.now(tz=timezone.utc)
        set_watermark(HOURLY_DAG, "articles",            now)
        set_watermark(HOURLY_DAG, "article_impressions", now)
        log.info("Initial load done. Articles: %d, Impressions: %d. Watermarks set → %s",
                 art_meta["total"], imp_meta["total"], now)


    # ── DEPENDENCIES ─────────────────────────────────────────────────────────

    dim_date = populate_dim_date()
    authors  = load_authors()
    readers  = load_readers()

    art = backfill_articles()
    imp = backfill_impressions()

    # authors & readers must exist before articles/impressions gold load
    dim_date >> [authors, readers]
    [dim_date, authors] >> art
    [dim_date, authors, readers] >> imp

    mart = build_mart_initial(imp)
    [art, imp] >> mart >> finalize(art, imp)


articles_initial_load()