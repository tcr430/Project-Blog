from __future__ import annotations

from datetime import datetime
from typing import Iterable
from urllib.parse import quote


def normalize_category_segment(category: str) -> str:
    cleaned = str(category).strip().lower()
    if not cleaned:
        return "posts"
    return quote(cleaned, safe="")


def build_post_relative_url(categories: Iterable[str], published_at: datetime, slug: str) -> str:
    category_list = [str(item).strip() for item in categories if str(item).strip()]
    category_path = "/".join(normalize_category_segment(item) for item in category_list) or "posts"
    return f"/{category_path}/{published_at.strftime('%Y/%m/%d')}/{slug}/"
