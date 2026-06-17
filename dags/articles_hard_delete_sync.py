"""
dags/articles_hard_delete_sync.py

BONUS: Hard delete reconciliation DAG.

Problem:
  Hard delete = row physically removed from source DB.
  The hourly ETL tracks changes via `updated_at` — but a hard-deleted row
  simply vanishes. No updated_at change. ETL never knows it's gone.

Solution:
  Daily full ID reconciliation:
    1. Fetch ALL article IDs from source
    2. Fetch all non-deleted article IDs from DWH
    3. IDs in DWH but NOT in source → mark is_deleted=TRUE in DWH

Schedule: Daily at 00:00 UTC (07:00 WIB).
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/include")
from db import source_cursor, dwh_cursor  # noqa: E402

log     = logging.getLogger(__name__)
SCHEMA  = "kumparan_dwh"


@dag(
    dag_id="articles_hard_delete_sync",
    schedule="0 0 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["kumparan", "articles", "hard-delete"],
    doc_md="""
    ## articles_hard_delete_sync
    Daily reconciliation for hard deletes.
    Compares source IDs vs DWH active IDs → marks orphans as deleted.
    """,
)
def articles_hard_delete_sync():

    @task()
    def fetch_source_ids() -> list[int]:
        with source_cursor() as cur:
            cur.execute("SELECT id FROM articles ORDER BY id")
            ids = [r["id"] for r in cur.fetchall()]
        log.info("Source: %d articles", len(ids))
        return ids


    @task()
    def fetch_dwh_active_ids() -> list[int]:
        with dwh_cursor() as cur:
            cur.execute(
                f"SELECT article_id FROM {SCHEMA}.dim_article WHERE is_deleted = FALSE"
            )
            ids = [r["article_id"] for r in cur.fetchall()]
        log.info("DWH active (non-deleted): %d articles", len(ids))
        return ids


    @task()
    def reconcile(source_ids: list[int], dwh_ids: list[int]) -> None:
        hard_deleted = list(set(dwh_ids) - set(source_ids))

        if not hard_deleted:
            log.info("No hard deletes detected. DWH is fully in sync.")
            return

        log.warning("Hard-deleted IDs detected: %d articles → %s ...",
                    len(hard_deleted), hard_deleted[:10])

        # Build safe SQL IN clause
        id_list = ",".join(str(i) for i in hard_deleted)
        now     = datetime.now(tz=timezone.utc)

        with dwh_cursor() as cur:
            # Mark in dim_article
            cur.execute(
                f"""
                UPDATE {SCHEMA}.dim_article
                SET    is_deleted    = TRUE,
                       deleted_at   = %s,
                       dw_updated_at = NOW()
                WHERE  article_id IN ({id_list})
                  AND  is_deleted = FALSE
                """,
                (now,),
            )

            # Reflect in fact table
            cur.execute(
                f"""
                UPDATE {SCHEMA}.fact_article_activity f
                SET    is_deleted    = TRUE,
                       dw_updated_at = NOW()
                FROM   {SCHEMA}.dim_article da
                WHERE  f.article_key = da.article_key
                  AND  da.article_id IN ({id_list})
                  AND  f.is_deleted  = FALSE
                """
            )

        log.info("Marked %d hard-deleted articles in DWH", len(hard_deleted))


    src = fetch_source_ids()
    dwh = fetch_dwh_active_ids()
    reconcile(src, dwh)


articles_hard_delete_sync()
