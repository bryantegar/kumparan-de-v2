"""
include/utils.py
Shared transform helpers — imported by ALL DAGs.
Single source of truth; no more copy-paste.
"""

from __future__ import annotations


def age_group(age) -> str:
    if age is None:    return "Unknown"
    if age < 18:       return "<18"
    if age < 25:       return "18-24"
    if age < 35:       return "25-34"
    if age < 45:       return "35-44"
    if age < 55:       return "45-54"
    return "55+"


def follower_tier(n) -> str:
    if n is None:          return "nano"
    if n < 10_000:         return "nano"
    if n < 100_000:        return "micro"
    if n < 500_000:        return "mid"
    if n < 1_000_000:      return "macro"
    return "mega"


def edu_level(edu) -> int:
    return {"SMA": 1, "D3": 2, "S1": 3, "S2": 4, "S3": 5}.get(edu or "", 0)


def device_class(device) -> str:
    if not device:          return "unknown"
    d = device.lower()
    if "mobile" in d:       return "mobile"
    if "tablet" in d:       return "tablet"
    return "desktop"


# Channel ID → human-readable category slug mapping from kumparan GraphQL
CHANNEL_SLUG_MAP: dict[str, str] = {
    "1":  "news",
    "2":  "entertainment",
    "3":  "woman",
    "4":  "mom",
    "5":  "bola-sports",
    "6":  "tekno-sains",
    "7":  "otomotif",
    "8":  "food-travel",
    "9":  "bolanita",
    "10": "bisnis",
}


def channel_to_category(channel_id: str | None, channel_slug: str | None) -> str:
    """Return a human-readable category; fall back gracefully."""
    if channel_slug:
        return channel_slug
    return CHANNEL_SLUG_MAP.get(str(channel_id or ""), "other")
