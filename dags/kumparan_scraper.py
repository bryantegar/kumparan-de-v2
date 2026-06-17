"""
dags/kumparan_scraper.py
Hourly DAG: scrape real articles from kumparan.com → source DB → DQ report.

Flow:
  scrape_articles → data_quality_check → load_to_source_db → dq_summary_log

Schedule: @hourly (runs BEFORE the ETL pipeline so fresh data is always available)
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/include")
from db import get_source_conn, get_dwh_conn, get_watermark, set_watermark, SCHEMA_DWH
from scraper import scrape_headlines

import psycopg2.extras

log    = logging.getLogger(__name__)
DAG_ID = "kumparan_scraper"


@dag(
    dag_id=DAG_ID,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["kumparan", "scraper", "hourly"],
    doc_md="""
## kumparan_scraper
Scrapes real articles from kumparan.com GraphQL API and loads them
into the source PostgreSQL DB (replacing dummy data).

### Data Quality checks performed:
- Missing title, content, published_at, author_id
- Future-dated published_at
- Duplicate article IDs
- Already-deleted articles
- Invalid/empty category

DQ results are stored in `kumparan_raw.dq_report` for analyst visibility.
""",
)
def kumparan_scraper():

    # ── STEP 1: SCRAPE ────────────────────────────────────────

    @task()
    def scrape_articles() -> dict:
        """
        Fetch articles from kumparan GraphQL API.
        Uses watermark so each hourly run only fetches new/updated articles.
        """
        since = get_watermark(DAG_ID, "kumparan_articles")
        log.info("Scraping articles published after: %s", since)

        articles = list(scrape_headlines(since=since))
        log.info("Scraped %d articles from kumparan.com", len(articles))

        # serialise datetimes for XCom (Airflow can't serialize datetime)
        def _serialise(row: dict) -> dict:
            out = {}
            for k, v in row.items():
                if isinstance(v, datetime):
                    out[k] = v.isoformat()
                else:
                    out[k] = v
            return out

        return {
            "articles":      [_serialise(a) for a in articles],
            "scraped_at":    datetime.now(tz=timezone.utc).isoformat(),
            "total_scraped": len(articles),
        }


    # ── STEP 2: DATA QUALITY CHECK ────────────────────────────

    @task()
    def data_quality_check(scrape_meta: dict) -> dict:
        """
        Run DQ rules on scraped data.
        Records pass/fail counts and persists per-article flags to dq_report table.
        Raises ValueError if critical failure rate > 20%.
        """
        articles    = scrape_meta["articles"]
        scraped_at  = scrape_meta["scraped_at"]
        total       = len(articles)

        if total == 0:
            log.info("No articles to check — skipping DQ.")
            return {"total": 0, "passed": 0, "failed": 0, "failure_rate": 0.0}

        passed  = sum(1 for a in articles if a.get("dq_ok"))
        failed  = total - passed

        # DQ dimension counts
        missing_title        = sum(1 for a in articles if a.get("dq_missing_title"))
        missing_content      = sum(1 for a in articles if a.get("dq_missing_content"))
        missing_published_at = sum(1 for a in articles if a.get("dq_missing_published_at"))
        missing_author       = sum(1 for a in articles if a.get("dq_missing_author_id"))
        future_published     = sum(1 for a in articles if a.get("dq_future_published_at"))
        already_deleted      = sum(1 for a in articles if a.get("dq_is_deleted"))
        missing_category     = sum(1 for a in articles if a.get("dq_missing_category"))

        failure_rate = failed / total if total else 0.0

        log.info(
            "DQ Summary | total=%d passed=%d failed=%d (%.1f%%)",
            total, passed, failed, failure_rate * 100,
        )
        log.info(
            "DQ Breakdown | missing_title=%d missing_content=%d "
            "missing_published_at=%d missing_author=%d "
            "future_published=%d already_deleted=%d missing_category=%d",
            missing_title, missing_content, missing_published_at,
            missing_author, future_published, already_deleted, missing_category,
        )

        # Persist DQ results to raw schema
        dwh = get_dwh_conn()
        try:
            with dwh.cursor() as cur:
                cur.executemany(
                    f"""INSERT INTO {SCHEMA_DWH.replace('kumparan_dwh','kumparan_raw')
                        if False else 'kumparan_raw'}.dq_report
                        (article_id, scraped_at, dq_ok,
                         dq_missing_title, dq_missing_content,
                         dq_missing_published_at, dq_missing_author_id,
                         dq_missing_category, dq_future_published_at,
                         dq_is_deleted)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (article_id, scraped_at) DO UPDATE SET
                            dq_ok=EXCLUDED.dq_ok,
                            dq_missing_title=EXCLUDED.dq_missing_title,
                            dq_missing_content=EXCLUDED.dq_missing_content,
                            dq_missing_published_at=EXCLUDED.dq_missing_published_at,
                            dq_missing_author_id=EXCLUDED.dq_missing_author_id,
                            dq_missing_category=EXCLUDED.dq_missing_category,
                            dq_future_published_at=EXCLUDED.dq_future_published_at,
                            dq_is_deleted=EXCLUDED.dq_is_deleted""",
                    [(
                        a["id"], scraped_at,
                        a.get("dq_ok", False),
                        a.get("dq_missing_title", False),
                        a.get("dq_missing_content", False),
                        a.get("dq_missing_published_at", False),
                        a.get("dq_missing_author_id", False),
                        a.get("dq_missing_category", False),
                        a.get("dq_future_published_at", False),
                        a.get("dq_is_deleted", False),
                    ) for a in articles if a.get("id")],
                )
            dwh.commit()
        except Exception:
            dwh.rollback()
            raise
        finally:
            dwh.close()

        # Hard stop if too many critical failures
        CRITICAL_THRESHOLD = 0.20
        if failure_rate > CRITICAL_THRESHOLD:
            raise ValueError(
                f"DQ failure rate {failure_rate:.1%} exceeds "
                f"threshold {CRITICAL_THRESHOLD:.0%}. "
                f"Aborting load to prevent bad data in source DB."
            )

        return {
            "total":           total,
            "passed":          passed,
            "failed":          failed,
            "failure_rate":    failure_rate,
            "missing_title":   missing_title,
            "missing_content": missing_content,
            "already_deleted": already_deleted,
            "future_published":future_published,
        }


    # ── STEP 3: LOAD TO SOURCE DB ─────────────────────────────

    @task()
    def load_to_source_db(scrape_meta: dict, dq_meta: dict) -> dict:
        """
        Upsert scraped articles into source PostgreSQL (OLTP) DB.
        Only loads articles that passed DQ (dq_ok=True).
        Records that fail DQ are skipped here but visible in dq_report.
        """
        articles = scrape_meta["articles"]
        if not articles:
            log.info("No articles to load.")
            return {"loaded": 0, "skipped_dq": 0}

        # Filter: only DQ-passed articles go into source DB
        clean    = [a for a in articles if a.get("dq_ok")]
        skipped  = len(articles) - len(clean)
        log.info("Loading %d articles (%d skipped due to DQ)", len(clean), skipped)

        if not clean:
            return {"loaded": 0, "skipped_dq": skipped}

        def _dt(val):
            """Deserialise ISO string back to datetime."""
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            return datetime.fromisoformat(val)

        src = get_source_conn()
        try:
            with src.cursor() as cur:
                # FIX: insert authors FIRST to satisfy FK on articles.author_id
                cur.executemany(
                    """INSERT INTO authors
                        (author_id, author_name, is_verified, joined_at)
                       VALUES (%s,%s,%s,NOW())
                       ON CONFLICT (author_id) DO UPDATE SET
                           author_name = EXCLUDED.author_name,
                           is_verified = EXCLUDED.is_verified""",
                    list({a["author_id"]: (
                        a["author_id"],
                        a.get("author_name"),
                        a.get("is_verified_author", False),
                    ) for a in clean if a.get("author_id")}.values()),
                )

                cur.executemany(
                    """INSERT INTO articles
                        (id, title, content, category, slug,
                         published_at, author_id, created_at,
                         updated_at, deleted_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (id) DO UPDATE SET
                           title        = EXCLUDED.title,
                           content      = EXCLUDED.content,
                           category     = EXCLUDED.category,
                           published_at = EXCLUDED.published_at,
                           updated_at   = EXCLUDED.updated_at,
                           deleted_at   = EXCLUDED.deleted_at""",
                    [(
                        a["id"],
                        a["title"],
                        a["content"],
                        a["category"],
                        a.get("slug"),
                        _dt(a.get("published_at")),
                        a["author_id"],
                        _dt(a.get("created_at")),
                        _dt(a.get("updated_at")),
                        _dt(a.get("deleted_at")),
                    ) for a in clean],
                )

            src.commit()
            log.info("Committed %d articles to source DB", len(clean))
        except Exception:
            src.rollback()
            raise
        finally:
            src.close()

        return {"loaded": len(clean), "skipped_dq": skipped}


    # ── STEP 4: UPDATE WATERMARK & LOG SUMMARY ────────────────

    @task()
    def finalize(scrape_meta: dict, load_meta: dict, dq_meta: dict) -> None:
        scraped_at = datetime.fromisoformat(scrape_meta["scraped_at"])
        if scrape_meta["total_scraped"] > 0:
            set_watermark(DAG_ID, "kumparan_articles", scraped_at)
            log.info("Watermark updated → %s", scraped_at)

        log.info(
            "Run complete | scraped=%d  loaded=%d  dq_failed=%d  failure_rate=%.1f%%",
            scrape_meta["total_scraped"],
            load_meta["loaded"],
            dq_meta.get("failed", 0),
            dq_meta.get("failure_rate", 0) * 100,
        )


    # ── DEPENDENCIES ──────────────────────────────────────────
    scrape_meta = scrape_articles()
    dq_meta     = data_quality_check(scrape_meta)
    load_meta   = load_to_source_db(scrape_meta, dq_meta)
    finalize(scrape_meta, load_meta, dq_meta)


kumparan_scraper()