from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from publish_pins import (
    HISTORY_FILE_PATH,
    QUEUE_FILE_PATH,
    load_history,
    load_queue,
    save_history,
    save_queue,
    upsert_history_entry,
)

ARTICLE_SCORES_FILE_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_article_scores.json"
REPINS_PER_ARTICLE = 2
REPIN_DELAY_DAYS = [7, 21]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan future Pinterest repins for strong-performing articles."
    )
    parser.add_argument(
        "--article-scores-path",
        type=str,
        default=str(ARTICLE_SCORES_FILE_PATH),
        help="Path to pinterest_article_scores.json.",
    )
    parser.add_argument(
        "--history-path",
        type=str,
        default=str(HISTORY_FILE_PATH),
        help="Path to pinterest_history.json.",
    )
    parser.add_argument(
        "--queue-path",
        type=str,
        default=str(QUEUE_FILE_PATH),
        help="Path to pinterest_queue.json.",
    )
    return parser.parse_args()


def load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return {}

    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return data


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    raw = value.strip()
    if not raw:
        return None

    normalized = raw.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def repin_performance_score(entry: dict[str, Any]) -> float:
    impressions = float(entry.get("impressions") or 0)
    saves = float(entry.get("saves") or 0)
    outbound_clicks = float(entry.get("outbound_clicks") or 0)
    pin_clicks = float(entry.get("pin_clicks") or 0)
    closeups = float(entry.get("closeups") or 0)
    return round((outbound_clicks * 5.0) + (saves * 3.0) + (pin_clicks * 2.0) + closeups + (impressions / 2000.0), 4)


def load_article_scores(article_scores_path: Path) -> list[dict[str, Any]]:
    payload = load_json_object(article_scores_path)
    articles = payload.get("articles", [])
    if not isinstance(articles, list):
        raise ValueError(f"Article scores file must contain an articles list: {article_scores_path}")
    return [item for item in articles if isinstance(item, dict)]


def select_strong_articles(article_scores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    strong_articles = [
        item
        for item in article_scores
        if str(item.get("classification") or "").strip().lower() == "strong_candidate"
    ]
    strong_articles.sort(
        key=lambda item: (
            int(item.get("score") or 0),
            float(item.get("average_outbound_clicks") or 0),
            float(item.get("average_saves") or 0),
            float(item.get("average_impressions") or 0),
        ),
        reverse=True,
    )
    return strong_articles


def select_repin_candidates(history_data: list[dict[str, Any]], article_slug: str) -> list[dict[str, Any]]:
    candidates = [
        entry
        for entry in history_data
        if str(entry.get("article_slug") or "") == article_slug
        and str(entry.get("status") or "").strip().lower() == "published"
        and str(entry.get("target_url") or "").strip()
        and str(entry.get("image_path") or "").strip()
    ]
    candidates.sort(
        key=lambda entry: (
            repin_performance_score(entry),
            parse_timestamp(str(entry.get("published_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    return candidates


def repin_identity(entry: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(entry.get("article_slug") or ""),
        str(entry.get("variant_type") or ""),
        str(entry.get("repin_source_created_at") or ""),
    )


def existing_repin_identities(queue_data: list[dict[str, Any]], history_data: list[dict[str, Any]]) -> set[tuple[str, str, str]]:
    identities: set[tuple[str, str, str]] = set()
    for collection in (queue_data, history_data):
        for entry in collection:
            if str(entry.get("distribution_kind") or "").strip().lower() != "repin":
                continue
            identities.add(repin_identity(entry))
    return identities


def latest_distribution_time(entries: list[dict[str, Any]], article_slug: str, now: datetime) -> datetime:
    latest = now
    for entry in entries:
        if str(entry.get("article_slug") or "") != article_slug:
            continue
        for field in ["scheduled_for", "published_at", "created_at"]:
            parsed = parse_timestamp(str(entry.get(field) or ""))
            if parsed and parsed > latest:
                latest = parsed
    return latest


def build_repin_entry(
    *,
    source_entry: dict[str, Any],
    article_score: dict[str, Any],
    scheduled_for: datetime,
    now: datetime,
    duplicate_index: int,
) -> dict[str, Any]:
    variant_type = str(source_entry.get("variant_type") or "repin")
    return {
        "article_slug": source_entry["article_slug"],
        "variant_type": variant_type,
        "board": dict(source_entry.get("board") or {}),
        "title": source_entry["title"],
        "description": source_entry["description"],
        "image_path": source_entry["image_path"],
        "target_url": source_entry["target_url"],
        "status": "scheduled",
        "created_at": now.isoformat(),
        "scheduled_for": scheduled_for.isoformat(),
        "published_at": None,
        "error_message": None,
        "provider_mode": "queue",
        "provider_pin_id": None,
        "priority_score": source_entry.get("priority_score") or repin_performance_score(source_entry),
        "schedule_rank": duplicate_index,
        "variant_key": f"repin-{duplicate_index + 1}",
        "site_root_url": source_entry.get("site_root_url"),
        "last_analytics_sync_at": None,
        "impressions": None,
        "outbound_clicks": None,
        "saves": None,
        "pin_clicks": None,
        "closeups": None,
        "distribution_kind": "repin",
        "repin_source_created_at": source_entry.get("created_at"),
        "repin_source_variant_type": source_entry.get("variant_type"),
        "repin_reason": article_score.get("classification"),
    }


def plan_pinterest_repins(
    article_scores_path: Path = ARTICLE_SCORES_FILE_PATH,
    history_path: Path = HISTORY_FILE_PATH,
    queue_path: Path = QUEUE_FILE_PATH,
) -> dict[str, Any]:
    print("[pinterest] planning repins for strong articles")
    article_scores = load_article_scores(article_scores_path)
    strong_articles = select_strong_articles(article_scores)
    queue_data = load_queue(queue_path)
    history_data = load_history(history_path)
    known_identities = existing_repin_identities(queue_data, history_data)
    now = datetime.now(timezone.utc)

    planned_count = 0
    strong_article_count = 0

    for article_score in strong_articles:
        article_slug = str(article_score.get("article_slug") or "").strip()
        if not article_slug:
            continue

        candidates = select_repin_candidates(history_data, article_slug)
        if not candidates:
            continue

        strong_article_count += 1
        base_time = latest_distribution_time(queue_data + history_data, article_slug, now)
        print(f"[pinterest] scheduling high-performing articles: {article_slug}")

        scheduled_for_article = 0
        for candidate in candidates:
            if scheduled_for_article >= REPINS_PER_ARTICLE:
                break

            identity = (
                article_slug,
                str(candidate.get("variant_type") or ""),
                str(candidate.get("created_at") or ""),
            )
            if identity in known_identities:
                continue

            delay_days = REPIN_DELAY_DAYS[min(scheduled_for_article, len(REPIN_DELAY_DAYS) - 1)]
            scheduled_for = max(now, base_time) + timedelta(days=delay_days)
            repin_entry = build_repin_entry(
                source_entry=candidate,
                article_score=article_score,
                scheduled_for=scheduled_for,
                now=now,
                duplicate_index=scheduled_for_article,
            )
            queue_data.append(repin_entry)
            upsert_history_entry(history_data, repin_entry)
            known_identities.add(identity)
            planned_count += 1
            scheduled_for_article += 1
            print(
                f"[pinterest] queued repin: {article_slug} / {repin_entry['variant_type']} "
                f"for {scheduled_for.isoformat()}"
            )

    save_queue(queue_path, queue_data)
    save_history(history_path, history_data)
    return {
        "article_scores_path": article_scores_path,
        "history_path": history_path,
        "queue_path": queue_path,
        "strong_article_count": strong_article_count,
        "planned_count": planned_count,
    }


def main() -> int:
    args = parse_args()
    try:
        result = plan_pinterest_repins(
            article_scores_path=Path(args.article_scores_path),
            history_path=Path(args.history_path),
            queue_path=Path(args.queue_path),
        )
        print(result["queue_path"])
        print(result["history_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
