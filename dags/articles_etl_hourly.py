"""
dags/articles_etl_hourly.py
Hourly incremental ELT:
  Source PostgreSQL → RAW → INTERMEDIATE → GOLD (dim/fact) → MART

Schedule : @hourly  (00:00 - 01:00 WIB window, etc.)
Watermark : updated_at >= last_wm AND updated_at < extraction_ts
Catchup   : False (backfill handled by articles_initial_load DAG)
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/include")
from db import (
    get_source_conn, get_dwh_conn,
    get_watermark, set_watermark,
    SCHEMA_RAW, SCHEMA_INT, SCHEMA_DWH, SCHEMA_MART,
)
# FIX: helpers now imported from shared utils.py (no more copy-paste)
from utils import age_group as _age_group, follower_tier as _follower_tier
from utils import edu_level as _edu_level, device_class as _device_class

import psycopg2.extras

log    = logging.getLogger(__name__)
DAG_ID = "articles_etl_hourly"


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id=DAG_ID,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["kumparan", "incremental", "hourly"],
    doc_md="""
## articles_etl_hourly
Incremental ELT pipeline running every hour.
- Extracts articles & impressions changed since last watermark
- Loads raw → intermediate (transforms + enrichment)
- Loads gold dimensional model (dim_author, dim_reader, dim_article, facts)
- Refreshes mart_article aggregation for affected dates
""",
)
def articles_etl_hourly():

    # ── STEP 1: EXTRACT → RAW ────────────────────────────────────────────────

    @task()
    def extract_articles() -> dict:
        watermark    = get_watermark(DAG_ID, "articles")
        extraction_ts = datetime.now(tz=timezone.utc)
        log.info("Articles watermark: %s", watermark)

        src = get_source_conn()
        dwh = get_dwh_conn()
        try:
            with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT id, title, content, category, published_at,
                              author_id, created_at, updated_at, deleted_at
                       FROM articles
                       WHERE updated_at >= %s AND updated_at < %s
                       ORDER BY updated_at""",
                    (watermark, extraction_ts),
                )
                rows = cur.fetchall()

            log.info("Extracted %d article rows", len(rows))
            if not rows:
                return {"row_count": 0, "extraction_ts": extraction_ts.isoformat()}

            batch_id = extraction_ts.strftime("%Y%m%d%H%M%S")
            with dwh.cursor() as cur:
                # append to raw (immutable audit log)
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_RAW}.articles
                        (id, title, content, category, published_at, author_id,
                         created_at, updated_at, deleted_at, _extracted_at, _batch_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)""",
                    [(r["id"], r["title"], r["content"], r["category"],
                      r["published_at"], r["author_id"], r["created_at"],
                      r["updated_at"], r["deleted_at"], batch_id) for r in rows],
                )
                # refresh staging for downstream tasks
                cur.execute(f"TRUNCATE TABLE {SCHEMA_RAW}.stg_articles")
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_RAW}.stg_articles
                        (id, title, content, category, published_at, author_id,
                         created_at, updated_at, deleted_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [(r["id"], r["title"], r["content"], r["category"],
                      r["published_at"], r["author_id"], r["created_at"],
                      r["updated_at"], r["deleted_at"]) for r in rows],
                )
            dwh.commit()
        except Exception:
            dwh.rollback(); raise
        finally:
            src.close(); dwh.close()

        return {"row_count": len(rows), "extraction_ts": extraction_ts.isoformat()}


    @task()
    def extract_impressions() -> dict:
        watermark     = get_watermark(DAG_ID, "article_impressions")
        extraction_ts = datetime.now(tz=timezone.utc)
        log.info("Impressions watermark: %s", watermark)

        src = get_source_conn()
        dwh = get_dwh_conn()
        try:
            with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT impression_id, article_id, reader_id, author_id,
                              read_date, read_at, device_type, platform,
                              read_duration_sec, is_completed
                       FROM article_impressions
                       WHERE read_at > %s
                       ORDER BY read_at""",
                    (watermark,),
                )
                rows = cur.fetchall()

            log.info("Extracted %d impression rows", len(rows))
            if not rows:
                return {"row_count": 0, "extraction_ts": extraction_ts.isoformat()}

            batch_id = extraction_ts.strftime("%Y%m%d%H%M%S")
            with dwh.cursor() as cur:
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_RAW}.article_impressions
                        (impression_id, article_id, reader_id, author_id,
                         read_date, read_at, device_type, platform,
                         read_duration_sec, is_completed, _extracted_at, _batch_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)""",
                    [(r["impression_id"], r["article_id"], r["reader_id"], r["author_id"],
                      r["read_date"], r["read_at"], r["device_type"], r["platform"],
                      r["read_duration_sec"], r["is_completed"], batch_id) for r in rows],
                )
                cur.execute(f"TRUNCATE TABLE {SCHEMA_RAW}.stg_impressions")
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_RAW}.stg_impressions
                        (impression_id, article_id, reader_id, author_id,
                         read_date, read_at, device_type, platform,
                         read_duration_sec, is_completed)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [(r["impression_id"], r["article_id"], r["reader_id"], r["author_id"],
                      r["read_date"], r["read_at"], r["device_type"], r["platform"],
                      r["read_duration_sec"], r["is_completed"]) for r in rows],
                )
            dwh.commit()
        except Exception:
            dwh.rollback(); raise
        finally:
            src.close(); dwh.close()

        return {"row_count": len(rows), "extraction_ts": extraction_ts.isoformat()}


    # ── STEP 2: TRANSFORM → INTERMEDIATE ────────────────────────────────────

    @task()
    def transform_articles(meta: dict) -> None:
        if meta["row_count"] == 0:
            log.info("No articles to transform."); return

        dwh = get_dwh_conn()
        try:
            with dwh.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"SELECT * FROM {SCHEMA_RAW}.stg_articles")
                rows = cur.fetchall()

            enriched = []
            for r in rows:
                content    = r["content"] or ""
                word_count = len(content.split())
                created    = r["created_at"]
                published  = r["published_at"]
                days_pub   = (published - created).days if published and created else None
                enriched.append((
                    r["id"], r["title"], content, r["category"], word_count,
                    published, r["author_id"], created, r["updated_at"],
                    r["deleted_at"], days_pub,
                ))

            with dwh.cursor() as cur:
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_INT}.articles
                        (id, title, content, category, word_count, published_at,
                         author_id, created_at, updated_at, deleted_at,
                         days_to_publish, _dw_updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            title=EXCLUDED.title, content=EXCLUDED.content,
                            category=EXCLUDED.category, word_count=EXCLUDED.word_count,
                            published_at=EXCLUDED.published_at,
                            updated_at=EXCLUDED.updated_at, deleted_at=EXCLUDED.deleted_at,
                            days_to_publish=EXCLUDED.days_to_publish,
                            _dw_updated_at=NOW()""",
                    enriched,
                )
            dwh.commit()
            log.info("Transformed %d articles → intermediate", len(enriched))
        except Exception:
            dwh.rollback(); raise
        finally:
            dwh.close()


    @task()
    def transform_impressions(meta: dict) -> None:
        if meta["row_count"] == 0:
            log.info("No impressions to transform."); return

        dwh = get_dwh_conn()
        try:
            with dwh.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(f"SELECT * FROM {SCHEMA_RAW}.stg_impressions")
                rows = cur.fetchall()

            enriched = []
            for r in rows:
                read_at  = r["read_at"]
                enriched.append((
                    r["impression_id"], r["article_id"], r["reader_id"], r["author_id"],
                    r["read_date"], read_at,
                    r["device_type"], _device_class(r["device_type"]),
                    r["platform"],
                    r["read_duration_sec"],
                    round(r["read_duration_sec"] / 60, 2) if r["read_duration_sec"] else None,
                    r["is_completed"],
                    read_at.hour if read_at else None,
                    read_at.isoweekday() if read_at else None,
                ))

            with dwh.cursor() as cur:
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_INT}.article_impressions
                        (impression_id, article_id, reader_id, author_id,
                         read_date, read_at, device_type, device_class, platform,
                         read_duration_sec, read_duration_min, is_completed,
                         hour_of_day, day_of_week, _dw_updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (impression_id) DO UPDATE SET
                            device_type=EXCLUDED.device_type,
                            device_class=EXCLUDED.device_class,
                            _dw_updated_at=NOW()""",
                    enriched,
                )
            dwh.commit()
            log.info("Transformed %d impressions → intermediate", len(enriched))
        except Exception:
            dwh.rollback(); raise
        finally:
            dwh.close()


    # ── STEP 3: LOAD → GOLD (Dims + Facts) ───────────────────────────────────

    @task()
    def load_dim_author(article_meta: dict) -> None:
        if article_meta["row_count"] == 0:
            log.info("Skip dim_author."); return

        src = get_source_conn()
        dwh = get_dwh_conn()
        try:
            # get author_ids from staging, then fetch full detail from source
            with dwh.cursor() as cur:
                cur.execute(f"SELECT DISTINCT author_id FROM {SCHEMA_RAW}.stg_articles")
                author_ids = [r[0] for r in cur.fetchall()]

            if not author_ids:
                return

            placeholders = ",".join(["%s"] * len(author_ids))
            with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""SELECT author_id, author_name, author_age, author_city,
                               author_province, author_education, specialty,
                               is_verified, follower_count, joined_at
                        FROM authors WHERE author_id IN ({placeholders})""",
                    author_ids,
                )
                authors = cur.fetchall()

            with dwh.cursor() as cur:
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_DWH}.dim_author
                        (author_id, author_name, author_age, age_group,
                         author_city, author_province, author_education, edu_level,
                         specialty, is_verified, follower_count, follower_tier,
                         joined_at, dw_created_at, dw_updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                        ON CONFLICT (author_id) DO UPDATE SET
                            author_name=EXCLUDED.author_name,
                            author_age=EXCLUDED.author_age,
                            age_group=EXCLUDED.age_group,
                            follower_count=EXCLUDED.follower_count,
                            follower_tier=EXCLUDED.follower_tier,
                            is_verified=EXCLUDED.is_verified,
                            dw_updated_at=NOW()""",
                    [(a["author_id"], a["author_name"], a["author_age"],
                      _age_group(a["author_age"]),
                      a["author_city"], a["author_province"], a["author_education"],
                      _edu_level(a["author_education"]),
                      a["specialty"], a["is_verified"], a["follower_count"],
                      _follower_tier(a["follower_count"]), a["joined_at"]) for a in authors],
                )
            dwh.commit()
            log.info("Upserted %d authors → dim_author", len(authors))
        except Exception:
            dwh.rollback(); raise
        finally:
            src.close(); dwh.close()


    @task()
    def load_dim_reader(imp_meta: dict) -> None:
        if imp_meta["row_count"] == 0:
            log.info("Skip dim_reader."); return

        src = get_source_conn()
        dwh = get_dwh_conn()
        try:
            with dwh.cursor() as cur:
                cur.execute(f"SELECT DISTINCT reader_id FROM {SCHEMA_RAW}.stg_impressions")
                reader_ids = [r[0] for r in cur.fetchall()]

            if not reader_ids:
                return

            placeholders = ",".join(["%s"] * len(reader_ids))
            with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""SELECT reader_id, reader_age, reader_city, reader_province,
                               membership_type, preferred_device, registered_at
                        FROM readers WHERE reader_id IN ({placeholders})""",
                    reader_ids,
                )
                readers = cur.fetchall()

            with dwh.cursor() as cur:
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_DWH}.dim_reader
                        (reader_id, reader_age, age_group, reader_city, reader_province,
                         membership_type, preferred_device, device_class,
                         registered_at, dw_created_at, dw_updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                        ON CONFLICT (reader_id) DO UPDATE SET
                            reader_age=EXCLUDED.reader_age,
                            age_group=EXCLUDED.age_group,
                            membership_type=EXCLUDED.membership_type,
                            preferred_device=EXCLUDED.preferred_device,
                            device_class=EXCLUDED.device_class,
                            dw_updated_at=NOW()""",
                    [(r["reader_id"], r["reader_age"], _age_group(r["reader_age"]),
                      r["reader_city"], r["reader_province"], r["membership_type"],
                      r["preferred_device"], _device_class(r["preferred_device"]),
                      r["registered_at"]) for r in readers],
                )
            dwh.commit()
            log.info("Upserted %d readers → dim_reader", len(readers))
        except Exception:
            dwh.rollback(); raise
        finally:
            src.close(); dwh.close()


    @task()
    def load_dim_article(article_meta: dict) -> None:
        if article_meta["row_count"] == 0:
            log.info("Skip dim_article."); return

        dwh = get_dwh_conn()
        try:
            with dwh.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA_DWH}.dim_article
                        (article_id, title, category, word_count, author_key,
                         published_date_key, created_date_key, days_to_publish,
                         is_deleted, deleted_at, dw_created_at, dw_updated_at)
                        SELECT
                            i.id, i.title, i.category, i.word_count,
                            da.author_key,
                            TO_CHAR(i.published_at AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                            TO_CHAR(i.created_at   AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                            i.days_to_publish,
                            i.is_deleted, i.deleted_at,
                            NOW(), NOW()
                        FROM {SCHEMA_INT}.articles i
                        JOIN {SCHEMA_DWH}.dim_author da ON da.author_id = i.author_id
                        WHERE i.id IN (SELECT id FROM {SCHEMA_RAW}.stg_articles)
                        ON CONFLICT (article_id) DO UPDATE SET
                            title=EXCLUDED.title, category=EXCLUDED.category,
                            word_count=EXCLUDED.word_count,
                            days_to_publish=EXCLUDED.days_to_publish,
                            is_deleted=EXCLUDED.is_deleted,
                            deleted_at=EXCLUDED.deleted_at,
                            dw_updated_at=NOW()"""
                )
            dwh.commit()
            log.info("Upserted articles → dim_article")
        except Exception:
            dwh.rollback(); raise
        finally:
            dwh.close()


    @task()
    def load_fact_activity(article_meta: dict) -> None:
        if article_meta["row_count"] == 0:
            log.info("Skip fact_article_activity."); return

        dwh = get_dwh_conn()
        try:
            with dwh.cursor() as cur:
                # delete-insert pattern (idempotent)
                cur.execute(
                    f"""DELETE FROM {SCHEMA_DWH}.fact_article_activity
                        WHERE article_key IN (
                            SELECT article_key FROM {SCHEMA_DWH}.dim_article
                            WHERE article_id IN (SELECT id FROM {SCHEMA_RAW}.stg_articles)
                        )"""
                )
                cur.execute(
                    f"""INSERT INTO {SCHEMA_DWH}.fact_article_activity
                        (article_key, author_key, published_date_key, created_date_key,
                         updated_date_key, content_length, word_count, days_to_publish,
                         is_published, is_deleted, dw_created_at, dw_updated_at)
                        SELECT
                            da.article_key, dau.author_key,
                            TO_CHAR(i.published_at AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                            TO_CHAR(i.created_at   AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                            TO_CHAR(i.updated_at   AT TIME ZONE 'Asia/Jakarta','YYYYMMDD')::INT,
                            LENGTH(i.content), i.word_count, i.days_to_publish,
                            i.is_published, i.is_deleted,
                            NOW(), NOW()
                        FROM {SCHEMA_INT}.articles i
                        JOIN {SCHEMA_DWH}.dim_article da  ON da.article_id = i.id
                        JOIN {SCHEMA_DWH}.dim_author  dau ON dau.author_id = i.author_id
                        WHERE i.id IN (SELECT id FROM {SCHEMA_RAW}.stg_articles)"""
                )
            dwh.commit()
            log.info("Loaded fact_article_activity")
        except Exception:
            dwh.rollback(); raise
        finally:
            dwh.close()


    @task()
    def load_fact_impression(imp_meta: dict) -> None:
        if imp_meta["row_count"] == 0:
            log.info("Skip fact_article_impression."); return

        dwh = get_dwh_conn()
        try:
            with dwh.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA_DWH}.fact_article_impression
                        (impression_id, article_key, reader_key, author_key,
                         read_date_key, read_at, device_type, device_class, platform,
                         read_duration_sec, read_duration_min, is_completed,
                         hour_of_day, day_of_week, dw_created_at, dw_updated_at)
                        SELECT
                            ii.impression_id,
                            da.article_key, dr.reader_key, dau.author_key,
                            TO_CHAR(ii.read_date,'YYYYMMDD')::INT,
                            ii.read_at, ii.device_type, ii.device_class, ii.platform,
                            ii.read_duration_sec, ii.read_duration_min, ii.is_completed,
                            ii.hour_of_day, ii.day_of_week,
                            NOW(), NOW()
                        FROM {SCHEMA_INT}.article_impressions ii
                        JOIN {SCHEMA_DWH}.dim_article da  ON da.article_id = ii.article_id
                        JOIN {SCHEMA_DWH}.dim_reader  dr  ON dr.reader_id  = ii.reader_id
                        JOIN {SCHEMA_DWH}.dim_author  dau ON dau.author_id = ii.author_id
                        WHERE ii.impression_id IN (
                            SELECT impression_id FROM {SCHEMA_RAW}.stg_impressions
                        )
                        ON CONFLICT (impression_id) DO NOTHING"""
                )
            dwh.commit()
            log.info("Loaded fact_article_impression")
        except Exception:
            dwh.rollback(); raise
        finally:
            dwh.close()


    # ── STEP 4: REFRESH MART ─────────────────────────────────────────────────

    @task()
    def refresh_mart(imp_meta: dict, article_meta: dict) -> None:
        """
        Refresh mart_article for dates affected in this batch.
        Answers: "berapa banyak artikel yang dibaca per hari?"
        """
        if imp_meta["row_count"] == 0 and article_meta["row_count"] == 0:
            log.info("Skip mart refresh."); return

        dwh = get_dwh_conn()
        try:
            with dwh.cursor() as cur:
                # find affected dates from this batch
                cur.execute(
                    f"SELECT DISTINCT read_date FROM {SCHEMA_RAW}.stg_impressions"
                )
                affected_dates = [r[0] for r in cur.fetchall()]

            if not affected_dates:
                log.info("No affected dates for mart."); return

            placeholders = ",".join(["%s"] * len(affected_dates))
            with dwh.cursor() as cur:
                # delete stale mart rows for affected dates
                cur.execute(
                    f"DELETE FROM {SCHEMA_MART}.mart_article WHERE read_date IN ({placeholders})",
                    affected_dates,
                )
                # re-aggregate
                cur.execute(
                    f"""INSERT INTO {SCHEMA_MART}.mart_article
                        (read_date, read_date_key, category, author_province,
                         reader_province, device_class, platform, membership_type,
                         total_reads, unique_articles, unique_readers,
                         avg_read_duration_sec, completed_reads, completion_rate,
                         dw_created_at, dw_updated_at)
                        SELECT
                            dd.full_date                            AS read_date,
                            f.read_date_key,
                            da.category,
                            dau.author_province,
                            dr.reader_province,
                            f.device_class,
                            f.platform,
                            dr.membership_type,
                            COUNT(*)                               AS total_reads,
                            COUNT(DISTINCT f.article_key)          AS unique_articles,
                            COUNT(DISTINCT f.reader_key)           AS unique_readers,
                            ROUND(AVG(f.read_duration_sec),2)      AS avg_read_duration_sec,
                            SUM(CASE WHEN f.is_completed THEN 1 ELSE 0 END) AS completed_reads,
                            ROUND(
                                SUM(CASE WHEN f.is_completed THEN 1 ELSE 0 END)::NUMERIC
                                / NULLIF(COUNT(*),0), 4
                            )                                      AS completion_rate,
                            NOW(), NOW()
                        FROM {SCHEMA_DWH}.fact_article_impression f
                        JOIN {SCHEMA_DWH}.dim_date    dd  ON dd.date_key    = f.read_date_key
                        JOIN {SCHEMA_DWH}.dim_article da  ON da.article_key = f.article_key
                        JOIN {SCHEMA_DWH}.dim_author  dau ON dau.author_key = f.author_key
                        JOIN {SCHEMA_DWH}.dim_reader  dr  ON dr.reader_key  = f.reader_key
                        WHERE dd.full_date IN ({placeholders})
                        GROUP BY 1,2,3,4,5,6,7,8""",
                    affected_dates + affected_dates,
                )
            dwh.commit()
            log.info("Refreshed mart_article for %d dates", len(affected_dates))
        except Exception:
            dwh.rollback(); raise
        finally:
            dwh.close()


    # ── STEP 5: UPDATE WATERMARKS ─────────────────────────────────────────────

    @task()
    def update_watermarks(article_meta: dict, imp_meta: dict) -> None:
        if article_meta["row_count"] > 0:
            ts = datetime.fromisoformat(article_meta["extraction_ts"])
            set_watermark(DAG_ID, "articles", ts)
            log.info("Article watermark → %s", ts)

        if imp_meta["row_count"] > 0:
            ts = datetime.fromisoformat(imp_meta["extraction_ts"])
            set_watermark(DAG_ID, "article_impressions", ts)
            log.info("Impression watermark → %s", ts)


    # ── TASK DEPENDENCIES ─────────────────────────────────────────────────────

    art_meta = extract_articles()
    imp_meta = extract_impressions()

    t_art = transform_articles(art_meta)
    t_imp = transform_impressions(imp_meta)

    art_meta >> t_art
    imp_meta >> t_imp

    d_author  = load_dim_author(art_meta)
    d_reader  = load_dim_reader(imp_meta)

    t_art >> d_author
    t_imp >> d_reader

    d_article = load_dim_article(art_meta)
    d_author >> d_article

    f_activity   = load_fact_activity(art_meta)
    f_impression = load_fact_impression(imp_meta)

    d_article >> f_activity
    [d_article, d_reader] >> f_impression

    mart = refresh_mart(imp_meta, art_meta)
    [f_activity, f_impression] >> mart

    mart >> update_watermarks(art_meta, imp_meta)


articles_etl_hourly()
