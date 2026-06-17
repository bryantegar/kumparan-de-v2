-- =================================================================
-- 01_source_ddl.sql  —  OLTP Source Database
-- Database: kumparan_source
-- =================================================================

-- ── Authors ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS authors (
    author_id       TEXT        PRIMARY KEY,
    author_name     TEXT,
    author_age      INT,
    author_city     TEXT,
    author_province TEXT,
    author_education TEXT,
    specialty       TEXT,
    is_verified     BOOLEAN     DEFAULT FALSE,
    follower_count  INT         DEFAULT 0,
    joined_at       TIMESTAMPTZ DEFAULT NOW(),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Readers ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS readers (
    reader_id       TEXT        PRIMARY KEY,
    reader_age      INT,
    reader_city     TEXT,
    reader_province TEXT,
    membership_type TEXT        DEFAULT 'free',
    preferred_device TEXT,
    registered_at   TIMESTAMPTZ DEFAULT NOW(),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Articles ─────────────────────────────────────────────────────
-- Real data comes from kumparan scraper (kumparan_scraper DAG).
-- Schema matches kumparan GraphQL response fields.
CREATE TABLE IF NOT EXISTS articles (
    id           TEXT        PRIMARY KEY,
    title        TEXT        NOT NULL,
    content      TEXT,
    category     TEXT,
    slug         TEXT,
    published_at TIMESTAMPTZ,
    author_id    TEXT        REFERENCES authors(author_id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    deleted_at   TIMESTAMPTZ DEFAULT NULL  -- soft delete flag (from kumparan API)
);

CREATE INDEX IF NOT EXISTS idx_articles_updated_at   ON articles(updated_at);
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_author_id    ON articles(author_id);
CREATE INDEX IF NOT EXISTS idx_articles_category     ON articles(category);

-- ── Article Impressions ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS article_impressions (
    impression_id      TEXT        PRIMARY KEY,
    article_id         TEXT        REFERENCES articles(id) ON DELETE CASCADE,
    reader_id          TEXT        REFERENCES readers(reader_id) ON DELETE SET NULL,
    author_id          TEXT        REFERENCES authors(author_id) ON DELETE SET NULL,
    read_date          DATE        NOT NULL,
    read_at            TIMESTAMPTZ NOT NULL,
    device_type        TEXT,
    platform           TEXT,
    read_duration_sec  INT,
    is_completed       BOOLEAN     DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_impressions_article_id ON article_impressions(article_id);
CREATE INDEX IF NOT EXISTS idx_impressions_read_at    ON article_impressions(read_at);

-- ── Hard-Delete Tracking Table ───────────────────────────────────
-- Populated by trigger below.
-- The hard_delete_sync DAG reads from this to propagate deletions to DWH.
CREATE TABLE IF NOT EXISTS article_deleted (
    id          TEXT        NOT NULL,
    deleted_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, deleted_at)
);

-- ── Trigger: capture hard deletes ───────────────────────────────
-- When a row is physically deleted from articles,
-- record its ID in article_deleted so the DWH can sync.
CREATE OR REPLACE FUNCTION capture_article_delete()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO article_deleted(id, deleted_at)
    VALUES (OLD.id, NOW())
    ON CONFLICT DO NOTHING;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS trg_article_hard_delete ON articles;
CREATE TRIGGER trg_article_hard_delete
    BEFORE DELETE ON articles
    FOR EACH ROW
    EXECUTE FUNCTION capture_article_delete();

-- ── auto-update updated_at on articles ──────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_articles_updated_at ON articles;
CREATE TRIGGER trg_articles_updated_at
    BEFORE UPDATE ON articles
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
