-- =================================================================
-- 03_dwh_ddl.sql  —  Data Warehouse Database
-- Database: kumparan_dwh
-- Schemas:  kumparan_raw | kumparan_intermediate | kumparan_dwh | kumparan_mart
-- =================================================================

CREATE SCHEMA IF NOT EXISTS kumparan_raw;
CREATE SCHEMA IF NOT EXISTS kumparan_intermediate;
CREATE SCHEMA IF NOT EXISTS kumparan_dwh;
CREATE SCHEMA IF NOT EXISTS kumparan_mart;

-- =================================================================
-- RAW LAYER  (immutable audit log + staging)
-- =================================================================

CREATE TABLE IF NOT EXISTS kumparan_raw.articles (
    _row_id        BIGSERIAL   PRIMARY KEY,
    id             TEXT        NOT NULL,
    title          TEXT,
    content        TEXT,
    category       TEXT,
    slug           TEXT,
    published_at   TIMESTAMPTZ,
    author_id      TEXT,
    created_at     TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ,
    deleted_at     TIMESTAMPTZ,
    _extracted_at  TIMESTAMPTZ DEFAULT NOW(),
    _batch_id      TEXT
);
CREATE INDEX IF NOT EXISTS idx_raw_articles_id ON kumparan_raw.articles(id);
CREATE INDEX IF NOT EXISTS idx_raw_articles_batch ON kumparan_raw.articles(_batch_id);

CREATE TABLE IF NOT EXISTS kumparan_raw.stg_articles (
    id           TEXT PRIMARY KEY,
    title        TEXT,
    content      TEXT,
    category     TEXT,
    slug         TEXT,
    published_at TIMESTAMPTZ,
    author_id    TEXT,
    created_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    deleted_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS kumparan_raw.article_impressions (
    _row_id           BIGSERIAL   PRIMARY KEY,
    impression_id     TEXT        NOT NULL,
    article_id        TEXT,
    reader_id         TEXT,
    author_id         TEXT,
    read_date         DATE,
    read_at           TIMESTAMPTZ,
    device_type       TEXT,
    platform          TEXT,
    read_duration_sec INT,
    is_completed      BOOLEAN,
    _extracted_at     TIMESTAMPTZ DEFAULT NOW(),
    _batch_id         TEXT
);

CREATE TABLE IF NOT EXISTS kumparan_raw.stg_impressions (
    impression_id     TEXT PRIMARY KEY,
    article_id        TEXT,
    reader_id         TEXT,
    author_id         TEXT,
    read_date         DATE,
    read_at           TIMESTAMPTZ,
    device_type       TEXT,
    platform          TEXT,
    read_duration_sec INT,
    is_completed      BOOLEAN
);

-- ── Data Quality Report ──────────────────────────────────────────
-- Populated by kumparan_scraper DAG on every run.
-- Allows analysts to monitor data quality over time.
CREATE TABLE IF NOT EXISTS kumparan_raw.dq_report (
    article_id               TEXT        NOT NULL,
    scraped_at               TIMESTAMPTZ NOT NULL,
    dq_ok                    BOOLEAN     DEFAULT FALSE,
    dq_missing_title         BOOLEAN     DEFAULT FALSE,
    dq_missing_content       BOOLEAN     DEFAULT FALSE,
    dq_missing_published_at  BOOLEAN     DEFAULT FALSE,
    dq_missing_author_id     BOOLEAN     DEFAULT FALSE,
    dq_missing_category      BOOLEAN     DEFAULT FALSE,
    dq_future_published_at   BOOLEAN     DEFAULT FALSE,
    dq_is_deleted            BOOLEAN     DEFAULT FALSE,
    created_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (article_id, scraped_at)
);
CREATE INDEX IF NOT EXISTS idx_dq_report_scraped_at ON kumparan_raw.dq_report(scraped_at);
CREATE INDEX IF NOT EXISTS idx_dq_report_ok ON kumparan_raw.dq_report(dq_ok);

-- =================================================================
-- INTERMEDIATE LAYER  (cleaned + enriched, upsertable)
-- =================================================================

CREATE TABLE IF NOT EXISTS kumparan_intermediate.articles (
    id               TEXT        PRIMARY KEY,
    title            TEXT,
    content          TEXT,
    category         TEXT,
    slug             TEXT,
    word_count       INT,
    published_at     TIMESTAMPTZ,
    author_id        TEXT,
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    deleted_at       TIMESTAMPTZ,
    is_deleted       BOOLEAN     GENERATED ALWAYS AS (deleted_at IS NOT NULL) STORED,
    is_published     BOOLEAN     GENERATED ALWAYS AS (published_at IS NOT NULL) STORED,
    days_to_publish  INT,
    _dw_updated_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kumparan_intermediate.article_impressions (
    impression_id     TEXT        PRIMARY KEY,
    article_id        TEXT,
    reader_id         TEXT,
    author_id         TEXT,
    read_date         DATE,
    read_at           TIMESTAMPTZ,
    device_type       TEXT,
    device_class      TEXT,
    platform          TEXT,
    read_duration_sec INT,
    read_duration_min NUMERIC(10,2),
    is_completed      BOOLEAN,
    hour_of_day       INT,
    day_of_week       INT,
    _dw_updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- =================================================================
-- GOLD LAYER  —  Dimensional Model
-- =================================================================

-- ── dim_date ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kumparan_dwh.dim_date (
    date_key      INT         PRIMARY KEY,  -- YYYYMMDD
    full_date     DATE        NOT NULL,
    day_of_week   INT,
    day_name      TEXT,
    day_of_month  INT,
    day_of_year   INT,
    week_of_year  INT,
    month_num     INT,
    month_name    TEXT,
    quarter       INT,
    year          INT,
    is_weekend    BOOLEAN
);

-- ── dim_author ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kumparan_dwh.dim_author (
    author_key        BIGSERIAL   PRIMARY KEY,
    author_id         TEXT        NOT NULL UNIQUE,
    author_name       TEXT,
    author_age        INT,
    age_group         TEXT,
    author_city       TEXT,
    author_province   TEXT,
    author_education  TEXT,
    edu_level         INT,
    specialty         TEXT,
    is_verified       BOOLEAN     DEFAULT FALSE,
    follower_count    INT,
    follower_tier     TEXT,
    joined_at         TIMESTAMPTZ,
    dw_created_at     TIMESTAMPTZ DEFAULT NOW(),
    dw_updated_at     TIMESTAMPTZ DEFAULT NOW()
);

-- ── dim_reader ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kumparan_dwh.dim_reader (
    reader_key       BIGSERIAL   PRIMARY KEY,
    reader_id        TEXT        NOT NULL UNIQUE,
    reader_age       INT,
    age_group        TEXT,
    reader_city      TEXT,
    reader_province  TEXT,
    membership_type  TEXT,
    preferred_device TEXT,
    device_class     TEXT,
    registered_at    TIMESTAMPTZ,
    dw_created_at    TIMESTAMPTZ DEFAULT NOW(),
    dw_updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ── dim_article ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kumparan_dwh.dim_article (
    article_key         BIGSERIAL   PRIMARY KEY,
    article_id          TEXT        NOT NULL UNIQUE,
    title               TEXT,
    category            TEXT,
    slug                TEXT,
    word_count          INT,
    author_key          BIGINT      REFERENCES kumparan_dwh.dim_author(author_key),
    published_date_key  INT         REFERENCES kumparan_dwh.dim_date(date_key),
    created_date_key    INT         REFERENCES kumparan_dwh.dim_date(date_key),
    days_to_publish     INT,
    is_deleted          BOOLEAN     DEFAULT FALSE,
    deleted_at          TIMESTAMPTZ,
    dw_created_at       TIMESTAMPTZ DEFAULT NOW(),
    dw_updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ── fact_article_activity ─────────────────────────────────────────
-- One row per article (grain = article lifecycle event)
CREATE TABLE IF NOT EXISTS kumparan_dwh.fact_article_activity (
    activity_key        BIGSERIAL   PRIMARY KEY,
    article_key         BIGINT      REFERENCES kumparan_dwh.dim_article(article_key),
    author_key          BIGINT      REFERENCES kumparan_dwh.dim_author(author_key),
    published_date_key  INT         REFERENCES kumparan_dwh.dim_date(date_key),
    created_date_key    INT         REFERENCES kumparan_dwh.dim_date(date_key),
    updated_date_key    INT         REFERENCES kumparan_dwh.dim_date(date_key),
    content_length      INT,
    word_count          INT,
    days_to_publish     INT,
    like_count          INT         DEFAULT 0,
    comment_count       INT         DEFAULT 0,
    is_published        BOOLEAN     DEFAULT FALSE,
    is_deleted          BOOLEAN     DEFAULT FALSE,
    dw_created_at       TIMESTAMPTZ DEFAULT NOW(),
    dw_updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ── fact_article_impression ───────────────────────────────────────
-- One row per reader impression (grain = one read event)
CREATE TABLE IF NOT EXISTS kumparan_dwh.fact_article_impression (
    impression_id     TEXT        PRIMARY KEY,
    article_key       BIGINT      REFERENCES kumparan_dwh.dim_article(article_key),
    reader_key        BIGINT      REFERENCES kumparan_dwh.dim_reader(reader_key),
    author_key        BIGINT      REFERENCES kumparan_dwh.dim_author(author_key),
    read_date_key     INT         REFERENCES kumparan_dwh.dim_date(date_key),
    read_at           TIMESTAMPTZ,
    device_type       TEXT,
    device_class      TEXT,
    platform          TEXT,
    read_duration_sec INT,
    read_duration_min NUMERIC(10,2),
    is_completed      BOOLEAN     DEFAULT FALSE,
    hour_of_day       INT,
    day_of_week       INT,
    dw_created_at     TIMESTAMPTZ DEFAULT NOW(),
    dw_updated_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fact_imp_article ON kumparan_dwh.fact_article_impression(article_key);
CREATE INDEX IF NOT EXISTS idx_fact_imp_date    ON kumparan_dwh.fact_article_impression(read_date_key);

-- ── ETL Watermark ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kumparan_dwh.etl_watermark (
    dag_id          TEXT        NOT NULL,
    table_name      TEXT        NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dag_id, table_name)
);

-- =================================================================
-- MART LAYER  —  Pre-aggregated for BI/dashboards
-- =================================================================

-- mart_article: daily article read summary
-- Answers: "berapa artikel yang dibaca per hari, per kategori?"
CREATE TABLE IF NOT EXISTS kumparan_mart.mart_article (
    read_date            DATE        NOT NULL,
    read_date_key        INT,
    category             TEXT,
    author_province      TEXT,
    reader_province      TEXT,
    device_class         TEXT,
    platform             TEXT,
    membership_type      TEXT,
    total_reads          BIGINT,
    unique_articles      BIGINT,
    unique_readers       BIGINT,
    avg_read_duration_sec NUMERIC(10,2),
    completed_reads      BIGINT,
    completion_rate      NUMERIC(6,4),
    dw_created_at        TIMESTAMPTZ DEFAULT NOW(),
    dw_updated_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (read_date, category, author_province, reader_province,
                 device_class, platform, membership_type)
);

-- mart_dq_daily: daily DQ pass/fail summary for monitoring
-- Allows tracking data quality trends over time
CREATE TABLE IF NOT EXISTS kumparan_mart.mart_dq_daily (
    scrape_date        DATE        NOT NULL PRIMARY KEY,
    total_scraped      INT         DEFAULT 0,
    total_passed       INT         DEFAULT 0,
    total_failed       INT         DEFAULT 0,
    failure_rate       NUMERIC(5,4) DEFAULT 0,
    missing_title      INT         DEFAULT 0,
    missing_content    INT         DEFAULT 0,
    missing_author     INT         DEFAULT 0,
    future_published   INT         DEFAULT 0,
    already_deleted    INT         DEFAULT 0,
    dw_updated_at      TIMESTAMPTZ DEFAULT NOW()
);
