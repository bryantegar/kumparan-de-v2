"""
include/scraper.py
Kumparan GraphQL scraper — fetches real articles from the public
cdn-graphql-v4.kumparan.com endpoint discovered via DevTools.

Features:
  - Cursor-based pagination (fetches all available articles per run)
  - Configurable rate limiting (default 1 req/sec, gentle on server)
  - Retry with exponential backoff on transient failures
  - Returns clean, normalised dicts ready for source DB insertion
  - Data Quality flags inline (see _dq_flags)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# ── Endpoint & persisted-query hash (from DevTools) ──────────
GRAPHQL_URL  = "https://cdn-graphql-v4.kumparan.com/query"
QUERY_HASH   = "eb503c3f2ef2f7f7ffb36ce34b1c928bdefdc87e6f178527f388ce4b5e3ceb16"
OPERATION    = "FindAllActiveHeadlines"
PAGE_SIZE    = 20          # articles per request
RATE_LIMIT_S = 1.2         # seconds between requests (be polite)
MAX_PAGES    = 50          # safety cap → max 1 000 articles per run


# ── HTTP session with retry ───────────────────────────────────

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,       # 2s, 4s, 8s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; kumparan-research-scraper/1.0)",
        "Accept":     "application/json",
        "Referer":    "https://kumparan.com/",
    })
    return session


# ── GraphQL request ───────────────────────────────────────────

def _fetch_page(session: requests.Session, cursor: str) -> dict:
    """Fire one GraphQL persisted-query GET request."""
    import json
    params = {
        "operationName": OPERATION,
        "variables": json.dumps({
            "size":      PAGE_SIZE,
            "placement": "HOMEPAGE",
            "cursor":    cursor,
        }),
        "extensions": json.dumps({
            "persistedQuery": {
                "version":    1,
                "sha256Hash": QUERY_HASH,
            }
        }),
        "deduplicate": "1",
    }
    resp = session.get(GRAPHQL_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ── Field extraction helpers ──────────────────────────────────

def _safe_str(val, max_len: int = None) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    if max_len:
        s = s[:max_len]
    return s or None


def _parse_dt(val) -> datetime | None:
    if not val:
        return None
    try:
        # kumparan returns ISO-8601 with Z
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return None


def _extract_story(edge: dict) -> dict | None:
    """
    Map one GraphQL edge → flat dict matching the source DB articles schema.
    Returns None if the story node is missing (malformed response).
    """
    story = edge.get("story")
    if not story:
        return None

    author  = story.get("author") or {}
    channel = story.get("channel") or {}
    pub     = story.get("publisher") or {}
    stat    = story.get("statistic") or {}

    # category: prefer channel slug, fall back to channel id mapping
    from utils import channel_to_category
    category = channel_to_category(
        channel.get("id"),
        channel.get("slug"),
    )

    # content: kumparan returns leadText + metaDescription
    # We join them as a proxy for article body (full content needs
    # a separate per-article request — add later if needed)
    lead_text   = _safe_str(story.get("leadText"),        max_len=2000)
    meta_desc   = _safe_str(story.get("metaDescription"), max_len=1000)
    content_raw = "\n\n".join(filter(None, [lead_text, meta_desc])) or None

    return {
        # core fields matching DB schema
        "id":           _safe_str(story.get("id")),
        "title":        _safe_str(story.get("title"), max_len=500),
        "content":      content_raw,
        "category":     category,
        "slug":         _safe_str(story.get("slug"), max_len=300),
        "published_at": _parse_dt(story.get("publishedAt")),
        "created_at":   _parse_dt(story.get("createdAt")),
        "updated_at":   _parse_dt(story.get("updatedAt")),
        "deleted_at":   _parse_dt(story.get("deletedAt")),

        # author fields
        "author_id":    _safe_str(author.get("id")),
        "author_name":  _safe_str(author.get("name"), max_len=200),
        "author_username": _safe_str(author.get("username"), max_len=100),
        "is_verified_author": author.get("isVerified", False),

        # publisher (kumparan brand channel, e.g. kumparanNEWS)
        "publisher_id":   _safe_str(pub.get("id")),
        "publisher_name": _safe_str(pub.get("name"), max_len=100),

        # engagement stats
        "like_count":    int(stat.get("likeCount")    or 0),
        "comment_count": int(stat.get("commentCount") or 0),

        # meta
        "source": _safe_str(story.get("source"), max_len=50),
        "is_private": bool(story.get("isPrivate", False)),
        "is_age_restricted": bool(story.get("isAgeRestrictedContent", False)),
    }


# ── Data Quality checks ───────────────────────────────────────

def _dq_flags(row: dict) -> dict:
    """
    Return a dict of DQ boolean flags for each article row.
    These are stored in the DWH for analyst visibility.
    """
    flags = {}
    flags["dq_missing_title"]       = not row.get("title")
    flags["dq_missing_content"]     = not row.get("content")
    flags["dq_missing_published_at"]= row.get("published_at") is None
    flags["dq_missing_author_id"]   = not row.get("author_id")
    flags["dq_missing_category"]    = not row.get("category") or row["category"] == "other"
    flags["dq_future_published_at"] = (
        row.get("published_at") is not None
        and row["published_at"] > datetime.now(tz=timezone.utc)
    )
    flags["dq_is_deleted"]          = row.get("deleted_at") is not None
    flags["dq_ok"] = not any([
        flags["dq_missing_title"],
        flags["dq_missing_content"],
        flags["dq_missing_published_at"],
        flags["dq_missing_author_id"],
    ])
    return flags


# ── Public API ────────────────────────────────────────────────

def scrape_headlines(
    max_pages: int = MAX_PAGES,
    since: datetime | None = None,
) -> Iterator[dict]:
    """
    Generator that yields one article dict per story, newest-first.

    Args:
        max_pages: hard cap on number of API pages to fetch.
        since:     stop pagination when publishedAt < since (for incremental runs).
                   If None, fetches everything up to max_pages.

    Yields:
        dict with article fields + dq_* flags merged in.
    """
    session  = _make_session()
    cursor   = "1"
    page_num = 0
    seen_ids: set[str] = set()
    stop     = False

    while page_num < max_pages and not stop:
        log.info("Fetching page %d (cursor=%s)", page_num + 1, cursor)
        try:
            data  = _fetch_page(session, cursor)
        except requests.RequestException as e:
            log.error("Request failed on page %d: %s", page_num + 1, e)
            break

        edges = (
            data.get("data", {})
                .get(OPERATION, {})
                .get("edges", [])
        )
        if not edges:
            log.info("No more edges — stopping pagination.")
            break

        for edge in edges:
            row = _extract_story(edge)
            if row is None:
                continue

            # dedup within run
            if row["id"] in seen_ids:
                continue
            seen_ids.add(row["id"])

            # incremental cut-off
            pub_at = row.get("published_at")
            if since and pub_at and pub_at < since:
                log.info(
                    "Reached articles older than watermark (%s). Stopping.", since
                )
                stop = True
                break

            # merge DQ flags into the row
            row.update(_dq_flags(row))
            yield row

        # next page: use last article id as cursor
        last_story_id = edges[-1].get("story", {}).get("id") if edges else None
        if not last_story_id or stop:
            break

        cursor = last_story_id
        page_num += 1
        time.sleep(RATE_LIMIT_S)

    log.info("Scrape complete. Pages: %d, Articles: %d", page_num + 1, len(seen_ids))
