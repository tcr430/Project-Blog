from __future__ import annotations

import argparse
import json
import re
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pinterest_client import PinterestClient

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
QUEUE_FILE_PATH = DATA_DIR / "pinterest_queue.json"
HISTORY_FILE_PATH = DATA_DIR / "pinterest_history.json"
SCHEDULE_OFFSETS_HOURS = [0, 18, 48, 96]
MAX_PINS_PER_RUN = 2
MAX_PINS_PER_DAY = 2
MIN_HOURS_BETWEEN_SAME_ARTICLE = 24 * 7
MIN_HOURS_BETWEEN_SAME_BOARD_IN_RUN = 12
DEFER_HOURS_AFTER_FAILURE = 24
DEFAULT_PUBLIC_IMAGE_TIMEOUT_SECONDS = 180
DEFAULT_PUBLIC_IMAGE_POLL_INTERVAL_SECONDS = 5
SEASONAL_WINDOWS = {
    "spring": ((2, 15), (6, 15)),
    "summer": ((5, 15), (9, 15)),
    "fall": ((8, 15), (11, 30)),
    "winter": ((11, 15), (2, 15)),
}
HOLIDAY_WINDOWS = {
    "christmas": ((10, 15), (1, 7)),
    "easter": ((2, 15), (4, 20)),
    "halloween": ((9, 1), (10, 31)),
    "thanksgiving": ((10, 15), (11, 30)),
    "valentines": ((1, 10), (2, 14)),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Queue or publish Pinterest variants from a Pinterest metadata file."
    )
    parser.add_argument("pinterest_metadata_path", type=str, help="Path to Pinterest metadata JSON.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return data


def load_json_list(path: Path, label: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return []

    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"{label} file must contain a JSON array: {path}")
    return data


def save_json_list(path: Path, records: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_queue(queue_path: Path) -> list[dict[str, Any]]:
    return load_json_list(queue_path, "Queue")


def save_queue(queue_path: Path, queue_data: list[dict[str, Any]]) -> Path:
    return save_json_list(queue_path, queue_data)


def load_history(history_path: Path) -> list[dict[str, Any]]:
    return load_json_list(history_path, "History")


def save_history(history_path: Path, history_data: list[dict[str, Any]]) -> Path:
    return save_json_list(history_path, history_data)


def build_public_asset_url(site_root_url: str, image_path: str) -> str:
    clean_root = site_root_url.rstrip("/")
    clean_path = image_path if image_path.startswith("/") else f"/{image_path}"
    return f"{clean_root}{clean_path}"


def is_public_image_reachable(image_url: str) -> bool:
    request = urllib.request.Request(
        image_url,
        headers={"User-Agent": "The-Livin-Edit-Pinterest/1.0"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            content_type = response.headers.get("Content-Type", "").lower()
            return response.status == 200 and content_type.startswith("image/")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return False


def wait_for_public_image(
    image_url: str,
    *,
    timeout_seconds: int = DEFAULT_PUBLIC_IMAGE_TIMEOUT_SECONDS,
    poll_interval_seconds: int = DEFAULT_PUBLIC_IMAGE_POLL_INTERVAL_SECONDS,
) -> bool:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=max(1, timeout_seconds))
    while datetime.now(timezone.utc) <= deadline:
        if is_public_image_reachable(image_url):
            return True
        import time

        time.sleep(max(1, poll_interval_seconds))
    return False


def resolve_publishable_image_path(image_path: str) -> str:
    clean_path = image_path.strip()
    if not clean_path.lower().endswith(".svg"):
        return clean_path

    project_root = Path(__file__).resolve().parents[2]
    png_relative = f"{clean_path[:-4]}.png"
    png_path = project_root / png_relative.lstrip("/")
    if png_path.exists():
        return png_relative

    slug = png_path.parent.name
    pinterest_metadata_path = project_root / "_data" / "pinterest" / f"{slug}.json"
    if pinterest_metadata_path.exists():
        try:
            from generate_pin_assets import generate_pin_assets

            print(f"[pinterest] regenerating PNG pin assets for {slug}")
            generate_pin_assets(pinterest_metadata_path)
        except Exception as exc:
            print(f"[pinterest] could not regenerate PNG assets for {slug}: {exc}")
        if png_path.exists():
            return png_relative
    return clean_path


def normalize_text(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", normalized)


def normalize_identifier(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


def month_day_key(month: int, day: int) -> int:
    return month * 100 + day


def is_date_within_window(now: datetime, start: tuple[int, int], end: tuple[int, int]) -> bool:
    current = month_day_key(now.month, now.day)
    start_key = month_day_key(*start)
    end_key = month_day_key(*end)
    if start_key <= end_key:
        return start_key <= current <= end_key
    return current >= start_key or current <= end_key


def infer_entry_season(value: str) -> str:
    normalized = normalize_text(value)
    for season in ("spring", "summer", "fall", "autumn", "winter"):
        if season in normalized:
            return "fall" if season == "autumn" else season
    return ""


def infer_entry_holiday(value: str) -> str:
    normalized = normalize_text(value)
    for holiday in ("christmas", "easter", "halloween", "thanksgiving", "valentines", "valentine"):
        if holiday in normalized:
            return "valentines" if holiday == "valentine" else holiday
    return ""


def resolve_entry_seasonality(entry: dict[str, Any]) -> tuple[str, str]:
    season = normalize_identifier(entry.get("season", ""))
    holiday = normalize_identifier(entry.get("holiday", ""))
    if season or holiday:
        return season, holiday

    haystack = " ".join(
        [
            str(entry.get("title", "")),
            str(entry.get("description", "")),
            str(entry.get("target_url", "")),
            str(entry.get("article_slug", "")),
        ]
    )
    inferred_season = infer_entry_season(haystack)
    inferred_holiday = infer_entry_holiday(haystack)
    return normalize_identifier(inferred_season), normalize_identifier(inferred_holiday)


def is_entry_in_season(entry: dict[str, Any], now: datetime) -> bool:
    season, holiday = resolve_entry_seasonality(entry)
    if holiday:
        window = HOLIDAY_WINDOWS.get(holiday)
        if not window:
            return False
        return is_date_within_window(now, window[0], window[1])

    if season:
        window = SEASONAL_WINDOWS.get(season)
        if not window:
            return False
        return is_date_within_window(now, window[0], window[1])

    return True


def history_published_at(entry: dict[str, Any]) -> datetime | None:
    return parse_timestamp(str(entry.get("published_at", "")), "published_at")


def count_published_today(history_data: list[dict[str, Any]], now: datetime) -> int:
    count = 0
    for entry in history_data:
        if str(entry.get("status", "")).strip().lower() != "published":
            continue
        published_at = history_published_at(entry)
        if not published_at:
            continue
        if published_at.date() == now.date():
            count += 1
    return count


def has_recent_article_publish(
    *,
    history_data: list[dict[str, Any]],
    article_slug: str,
    now: datetime,
) -> bool:
    cutoff = now - timedelta(hours=MIN_HOURS_BETWEEN_SAME_ARTICLE)
    for entry in history_data:
        if str(entry.get("status", "")).strip().lower() != "published":
            continue
        if str(entry.get("article_slug", "")).strip() != article_slug:
            continue
        published_at = history_published_at(entry)
        if published_at and published_at >= cutoff:
            return True
    return False


def defer_entry(entry: dict[str, Any], *, now: datetime, hours: int, reason: str) -> dict[str, Any]:
    deferred = dict(entry)
    deferred["status"] = "scheduled"
    deferred["scheduled_for"] = (now + timedelta(hours=hours)).isoformat()
    deferred["error_message"] = reason
    return deferred


def schedule_variant_time(created_at: datetime, index: int) -> datetime:
    if index < len(SCHEDULE_OFFSETS_HOURS):
        offset_hours = SCHEDULE_OFFSETS_HOURS[index]
    else:
        extra_steps = index - len(SCHEDULE_OFFSETS_HOURS) + 1
        offset_hours = SCHEDULE_OFFSETS_HOURS[-1] + extra_steps * 48
    return created_at + timedelta(hours=offset_hours)


def parse_timestamp(value: str | None, field_name: str) -> datetime | None:
    if not value:
        return None

    raw = value.strip()
    if not raw:
        return None

    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} timestamp: {value}") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def extract_provider_pin_id(response_payload: dict[str, Any]) -> str | None:
    candidate_keys = ["id", "pin_id", "item_id"]
    for key in candidate_keys:
        value = response_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def publish_single_entry(
    *,
    client: PinterestClient,
    entry: dict[str, Any],
    wait_for_image: bool = False,
    image_timeout_seconds: int = DEFAULT_PUBLIC_IMAGE_TIMEOUT_SECONDS,
    image_poll_interval_seconds: int = DEFAULT_PUBLIC_IMAGE_POLL_INTERVAL_SECONDS,
) -> dict[str, Any]:
    publishable_image_path = resolve_publishable_image_path(str(entry["image_path"]))
    image_url = build_public_asset_url(
        site_root_url=str(entry["site_root_url"]),
        image_path=publishable_image_path,
    )
    if wait_for_image:
        print(f"[pinterest] waiting for public image to become reachable: {image_url}")
        if not wait_for_public_image(
            image_url,
            timeout_seconds=image_timeout_seconds,
            poll_interval_seconds=image_poll_interval_seconds,
        ):
            raise RuntimeError(
                f"Public pin image did not become reachable in time: {image_url}"
            )

    title = str(entry["title"])
    variant_label = str(entry.get("variant_type", entry.get("variant_key", "pin")))
    board = entry.get("board") or {}
    board_key = str(board.get("key", "default")).strip() or "default"
    print(f"[pinterest] publishing due pin: {variant_label} - {title}")
    response_payload = client.publish_variant(
        board_key=board_key,
        title=title,
        description=str(entry["description"]),
        article_url=str(entry["target_url"]),
        image_url=image_url,
    )
    published_entry = dict(entry)
    published_entry["status"] = "published"
    published_entry["published_at"] = datetime.now(timezone.utc).isoformat()
    published_entry["error_message"] = None
    published_entry["provider_pin_id"] = extract_provider_pin_id(response_payload)
    published_entry["image_path"] = publishable_image_path
    return published_entry


def build_queue_entries(payload: dict[str, Any], provider_mode: str) -> list[dict[str, Any]]:
    created_at = datetime.now(timezone.utc)
    entries: list[dict[str, Any]] = []

    for index, variant in enumerate(payload["variants"]):
        board = variant.get("board") or {}
        board_key = str(board.get("key", "default")).strip() or "default"
        board_name = str(board.get("name", board_key)).strip() or board_key
        scheduled_for = schedule_variant_time(created_at=created_at, index=index)
        variant_type = str(variant["variant_type"])
        status = "queued" if index == 0 else "scheduled"

        board_label = f"{board_name} ({board_key})"
        if status == "queued":
            print(f"[pinterest] pin queued: {variant_type} on {board_label}")
        else:
            print(f"[pinterest] pin scheduled: {variant_type} on {board_label} for {scheduled_for.isoformat()}")

        entries.append(
            {
                "article_slug": payload["article_slug"],
                "cluster_id": payload.get("cluster_id", ""),
                "subtopic_id": payload.get("subtopic_id", ""),
                "angle_id": payload.get("angle_id", ""),
                "intent_id": payload.get("intent_id", ""),
                "season": payload.get("season", ""),
                "holiday": payload.get("holiday", ""),
                "visual_direction": payload.get("visual_direction", {}),
                "variant_type": variant_type,
                "style_name": variant.get("style_name", ""),
                "board": {
                    "key": board_key,
                    "name": board_name,
                },
                "title": variant["title"],
                "description": variant["description"],
                "image_path": variant["image_path"],
                "target_url": payload["article_url"],
                "status": status,
                "created_at": created_at.isoformat(),
                "scheduled_for": scheduled_for.isoformat(),
                "published_at": None,
                "error_message": None,
                "provider_mode": provider_mode,
                "provider_pin_id": None,
                "priority_score": variant.get("priority_score"),
                "schedule_rank": variant.get("schedule_rank", index),
                "variant_key": variant["variant_key"],
                "site_root_url": payload["site_root_url"],
                "last_analytics_sync_at": None,
                "impressions": None,
                "outbound_clicks": None,
                "saves": None,
                "pin_clicks": None,
                "closeups": None,
            }
        )

    return entries


def build_history_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "article_slug": entry["article_slug"],
        "cluster_id": entry.get("cluster_id", ""),
        "subtopic_id": entry.get("subtopic_id", ""),
        "angle_id": entry.get("angle_id", ""),
        "intent_id": entry.get("intent_id", ""),
        "season": entry.get("season", ""),
        "holiday": entry.get("holiday", ""),
        "visual_direction": entry.get("visual_direction", {}),
        "variant_type": entry["variant_type"],
        "style_name": entry.get("style_name", ""),
        "board": entry["board"],
        "title": entry["title"],
        "description": entry["description"],
        "image_path": entry["image_path"],
        "target_url": entry["target_url"],
        "created_at": entry["created_at"],
        "scheduled_for": entry["scheduled_for"],
        "published_at": entry.get("published_at"),
        "status": entry["status"],
        "error_message": entry.get("error_message"),
        "provider_mode": entry.get("provider_mode", "queue"),
        "provider_pin_id": entry.get("provider_pin_id"),
        "priority_score": entry.get("priority_score"),
        "schedule_rank": entry.get("schedule_rank"),
        "last_analytics_sync_at": entry.get("last_analytics_sync_at"),
        "impressions": entry.get("impressions"),
        "outbound_clicks": entry.get("outbound_clicks"),
        "saves": entry.get("saves"),
        "pin_clicks": entry.get("pin_clicks"),
        "closeups": entry.get("closeups"),
    }


def history_identity(entry: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(entry["article_slug"]),
        str(entry["variant_type"]),
        str(entry["created_at"]),
    )


def upsert_history_entry(history_data: list[dict[str, Any]], entry: dict[str, Any]) -> None:
    replacement = build_history_entry(entry)
    identity = history_identity(replacement)

    for index, existing in enumerate(history_data):
        if history_identity(existing) != identity:
            continue

        for analytics_field in [
            "provider_pin_id",
            "last_analytics_sync_at",
            "impressions",
            "outbound_clicks",
            "saves",
            "pin_clicks",
            "closeups",
        ]:
            if replacement.get(analytics_field) is None:
                replacement[analytics_field] = existing.get(analytics_field)
        history_data[index] = replacement
        return

    history_data.append(replacement)


def queue_pinterest_payload(
    payload: dict[str, Any],
    queue_path: Path,
    history_path: Path,
    provider_mode: str,
) -> tuple[Path, Path, int]:
    queue_data = load_queue(queue_path)
    history_data = load_history(history_path)
    new_entries = build_queue_entries(payload=payload, provider_mode=provider_mode)
    queue_data.extend(new_entries)

    for entry in new_entries:
        upsert_history_entry(history_data, entry)

    save_queue(queue_path=queue_path, queue_data=queue_data)
    save_history(history_path=history_path, history_data=history_data)
    return queue_path, history_path, len(new_entries)


def process_queue(client: PinterestClient, queue_path: Path, history_path: Path) -> dict[str, Any]:
    queue_data = load_queue(queue_path)
    history_data = load_history(history_path)
    if not queue_data:
        return {
            "mode": "publish",
            "published_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "queue_path": queue_path,
            "history_path": history_path,
        }

    print("[pinterest] authenticating with Pinterest")

    published_count = 0
    failed_count = 0
    skipped_count = 0
    now = datetime.now(timezone.utc)
    remaining_entries: list[dict[str, Any]] = []
    published_today = count_published_today(history_data, now)
    selected_board_keys: set[str] = set()
    selected_article_slugs: set[str] = set()
    publish_budget_remaining = max(0, MAX_PINS_PER_DAY - published_today)
    publish_budget_remaining = min(publish_budget_remaining, MAX_PINS_PER_RUN)

    ordered_queue = sorted(
        queue_data,
        key=lambda item: (
            parse_timestamp(str(item.get("scheduled_for", "")), "scheduled_for") or now,
            parse_timestamp(str(item.get("created_at", "")), "created_at") or now,
            str(item.get("article_slug", "")),
            str(item.get("variant_key", "")),
        ),
    )

    for entry in ordered_queue:
        status = str(entry.get("status", "queued")).strip().lower() or "queued"
        if status == "published":
            upsert_history_entry(history_data, entry)
            continue

        scheduled_for = parse_timestamp(str(entry.get("scheduled_for", "")), "scheduled_for")
        variant_label = str(entry.get("variant_type", entry.get("variant_key", "pin")))

        if scheduled_for and scheduled_for > now:
            skipped_count += 1
            print(
                f"[pinterest] pin skipped because not due: {variant_label} "
                f"(scheduled for {scheduled_for.isoformat()})"
            )
            remaining_entries.append(entry)
            continue

        if not is_entry_in_season(entry, now):
            skipped_count += 1
            season, holiday = resolve_entry_seasonality(entry)
            seasonal_label = holiday or season or "seasonal"
            deferred = defer_entry(
                entry,
                now=now,
                hours=24 * 14,
                reason=f"Deferred because '{seasonal_label}' is outside the current publishing window.",
            )
            remaining_entries.append(deferred)
            print(
                f"[pinterest] pin deferred because out of season: {variant_label} "
                f"({seasonal_label})"
            )
            continue

        article_slug = str(entry.get("article_slug", "")).strip()
        board = entry.get("board") or {}
        board_key = str(board.get("key", "default")).strip() or "default"

        if publish_budget_remaining <= 0:
            skipped_count += 1
            deferred = defer_entry(
                entry,
                now=now,
                hours=12,
                reason="Deferred because the safe daily publish cap has been reached.",
            )
            remaining_entries.append(deferred)
            print(f"[pinterest] pin deferred by daily cap: {variant_label}")
            continue

        if article_slug in selected_article_slugs or has_recent_article_publish(
            history_data=history_data,
            article_slug=article_slug,
            now=now,
        ):
            skipped_count += 1
            deferred = defer_entry(
                entry,
                now=now,
                hours=MIN_HOURS_BETWEEN_SAME_ARTICLE,
                reason="Deferred to avoid publishing multiple pins for the same article too close together.",
            )
            remaining_entries.append(deferred)
            print(f"[pinterest] pin deferred by article spacing: {variant_label} ({article_slug})")
            continue

        if board_key in selected_board_keys:
            skipped_count += 1
            deferred = defer_entry(
                entry,
                now=now,
                hours=MIN_HOURS_BETWEEN_SAME_BOARD_IN_RUN,
                reason="Deferred to rotate boards instead of publishing repeatedly to the same board in one run.",
            )
            remaining_entries.append(deferred)
            print(f"[pinterest] pin deferred by board rotation: {variant_label} ({board_key})")
            continue

        try:
            published_entry = publish_single_entry(
                client=client,
                entry=entry,
            )
            published_count += 1
            publish_budget_remaining -= 1
            if article_slug:
                selected_article_slugs.add(article_slug)
            if board_key:
                selected_board_keys.add(board_key)
            upsert_history_entry(history_data, published_entry)
            print(f"[pinterest] pin published: {variant_label}")
            if not published_entry["provider_pin_id"]:
                print(f"[pinterest] analytics unavailable: provider pin ID missing for {variant_label}")
        except Exception as exc:
            failed_count += 1
            failed_entry = defer_entry(
                entry,
                now=now,
                hours=DEFER_HOURS_AFTER_FAILURE,
                reason=str(exc),
            )
            failed_entry["status"] = "failed"
            remaining_entries.append(failed_entry)
            upsert_history_entry(history_data, failed_entry)
            print(f"[pinterest] pin failed: {variant_label} ({exc})")

    save_queue(queue_path=queue_path, queue_data=remaining_entries)
    save_history(history_path=history_path, history_data=history_data)
    return {
        "mode": "publish",
        "published_count": published_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "queue_path": queue_path,
        "history_path": history_path,
    }


def select_primary_article_entry(
    queue_data: list[dict[str, Any]],
    *,
    article_slug: str,
) -> tuple[int, dict[str, Any]] | None:
    candidates: list[tuple[int, dict[str, Any]]] = []
    for index, entry in enumerate(queue_data):
        if str(entry.get("article_slug", "")).strip() != article_slug:
            continue
        if str(entry.get("status", "")).strip().lower() == "published":
            continue
        candidates.append((index, entry))

    if not candidates:
        return None

    def sort_key(item: tuple[int, dict[str, Any]]) -> tuple[datetime, int, datetime]:
        _, entry = item
        created_at = parse_timestamp(str(entry.get("created_at", "")), "created_at") or datetime.min.replace(
            tzinfo=timezone.utc
        )
        schedule_rank = int(entry.get("schedule_rank", 9999))
        scheduled_for = parse_timestamp(str(entry.get("scheduled_for", "")), "scheduled_for") or datetime.max.replace(
            tzinfo=timezone.utc
        )
        return (-created_at.timestamp(), schedule_rank, scheduled_for)

    return sorted(candidates, key=sort_key)[0]


def select_latest_primary_entry(
    queue_data: list[dict[str, Any]],
) -> tuple[int, dict[str, Any]] | None:
    candidates: list[tuple[int, dict[str, Any]]] = []
    for index, entry in enumerate(queue_data):
        status = str(entry.get("status", "")).strip().lower()
        if status == "published":
            continue
        schedule_rank = int(entry.get("schedule_rank", 9999))
        variant_key = str(entry.get("variant_key", "")).strip().lower()
        if schedule_rank != 0 and variant_key != "pin-1":
            continue
        candidates.append((index, entry))

    if not candidates:
        return None

    def sort_key(item: tuple[int, dict[str, Any]]) -> tuple[datetime, datetime]:
        _, entry = item
        created_at = parse_timestamp(str(entry.get("created_at", "")), "created_at") or datetime.min.replace(
            tzinfo=timezone.utc
        )
        scheduled_for = parse_timestamp(str(entry.get("scheduled_for", "")), "scheduled_for") or datetime.min.replace(
            tzinfo=timezone.utc
        )
        return (created_at, scheduled_for)

    return sorted(candidates, key=sort_key, reverse=True)[0]


def primary_entry_exists(
    *,
    article_slug: str,
    queue_data: list[dict[str, Any]],
    history_data: list[dict[str, Any]],
) -> bool:
    for collection in (queue_data, history_data):
        for entry in collection:
            if str(entry.get("article_slug", "")).strip() != article_slug:
                continue
            schedule_rank = int(entry.get("schedule_rank", 9999))
            variant_key = str(entry.get("variant_key", "")).strip().lower()
            if schedule_rank == 0 or variant_key == "pin-1":
                return True
    return False


def build_primary_queue_entry(payload: dict[str, Any]) -> dict[str, Any]:
    variants = payload.get("variants") or []
    if not isinstance(variants, list) or not variants:
        raise ValueError("Pinterest metadata does not contain variants.")

    primary_variant = variants[0]
    if not isinstance(primary_variant, dict):
        raise ValueError("Primary Pinterest variant must be an object.")

    board = primary_variant.get("board") or {}
    board_key = str(board.get("key", "default")).strip() or "default"
    board_name = str(board.get("name", board_key)).strip() or board_key
    now = datetime.now(timezone.utc).isoformat()

    return {
        "article_slug": str(payload.get("article_slug", "")).strip(),
        "cluster_id": str(payload.get("cluster_id", "")).strip(),
        "subtopic_id": str(payload.get("subtopic_id", "")).strip(),
        "angle_id": str(payload.get("angle_id", "")).strip(),
        "intent_id": str(payload.get("intent_id", "")).strip(),
        "season": str(payload.get("season", "")).strip(),
        "holiday": str(payload.get("holiday", "")).strip(),
        "visual_direction": payload.get("visual_direction", {}),
        "variant_type": str(primary_variant.get("variant_type", "")).strip() or "trend_overview",
        "style_name": str(primary_variant.get("style_name", "")).strip(),
        "board": {
            "key": board_key,
            "name": board_name,
        },
        "title": str(primary_variant.get("title", "")).strip(),
        "description": str(primary_variant.get("description", "")).strip(),
        "image_path": str(primary_variant.get("image_path", "")).strip(),
        "target_url": str(payload.get("article_url", "")).strip(),
        "status": "queued",
        "created_at": now,
        "scheduled_for": now,
        "published_at": None,
        "error_message": None,
        "provider_mode": "queue",
        "provider_pin_id": None,
        "priority_score": primary_variant.get("priority_score"),
        "schedule_rank": int(primary_variant.get("schedule_rank", 0) or 0),
        "variant_key": str(primary_variant.get("variant_key", "pin-1")).strip() or "pin-1",
        "site_root_url": str(payload.get("site_root_url", "")).strip(),
        "last_analytics_sync_at": None,
        "impressions": None,
        "outbound_clicks": None,
        "saves": None,
        "pin_clicks": None,
        "closeups": None,
    }


def backfill_primary_entry_if_missing(
    *,
    project_root: Path,
    article_slug: str,
    queue_path: Path = QUEUE_FILE_PATH,
    history_path: Path = HISTORY_FILE_PATH,
) -> bool:
    queue_data = load_queue(queue_path)
    history_data = load_history(history_path)
    if primary_entry_exists(article_slug=article_slug, queue_data=queue_data, history_data=history_data):
        return False

    pinterest_metadata_path = project_root / "_data" / "pinterest" / f"{article_slug}.json"
    if not pinterest_metadata_path.exists():
        return False

    payload = load_json(pinterest_metadata_path)
    if str(payload.get("article_slug", "")).strip() != article_slug:
        return False

    entry = build_primary_queue_entry(payload)
    if not entry["title"] or not entry["image_path"] or not entry["target_url"]:
        raise ValueError(f"Primary Pinterest metadata is incomplete for '{article_slug}'.")

    queue_data.append(entry)
    upsert_history_entry(history_data, entry)
    save_queue(queue_path, queue_data)
    save_history(history_path, history_data)
    print(f"[pinterest] backfilled missing primary queue entry for {article_slug}")
    return True


def publish_primary_article_pin(
    *,
    client: PinterestClient,
    article_slug: str,
    queue_path: Path = QUEUE_FILE_PATH,
    history_path: Path = HISTORY_FILE_PATH,
    wait_for_image: bool = True,
    image_timeout_seconds: int = DEFAULT_PUBLIC_IMAGE_TIMEOUT_SECONDS,
    image_poll_interval_seconds: int = DEFAULT_PUBLIC_IMAGE_POLL_INTERVAL_SECONDS,
) -> dict[str, Any]:
    queue_data = load_queue(queue_path)
    history_data = load_history(history_path)
    selected = select_primary_article_entry(queue_data, article_slug=article_slug)
    if selected is None:
        raise RuntimeError(f"No queued Pinterest entry found for article slug '{article_slug}'.")

    entry_index, entry = selected
    published_entry = publish_single_entry(
        client=client,
        entry=entry,
        wait_for_image=wait_for_image,
        image_timeout_seconds=image_timeout_seconds,
        image_poll_interval_seconds=image_poll_interval_seconds,
    )
    queue_data.pop(entry_index)
    upsert_history_entry(history_data, published_entry)
    save_queue(queue_path=queue_path, queue_data=queue_data)
    save_history(history_path=history_path, history_data=history_data)
    return {
        "article_slug": article_slug,
        "variant_type": published_entry.get("variant_type", ""),
        "provider_pin_id": published_entry.get("provider_pin_id"),
        "queue_path": queue_path,
        "history_path": history_path,
    }


def publish_latest_primary_pin(
    *,
    client: PinterestClient,
    queue_path: Path = QUEUE_FILE_PATH,
    history_path: Path = HISTORY_FILE_PATH,
    wait_for_image: bool = True,
    image_timeout_seconds: int = DEFAULT_PUBLIC_IMAGE_TIMEOUT_SECONDS,
    image_poll_interval_seconds: int = DEFAULT_PUBLIC_IMAGE_POLL_INTERVAL_SECONDS,
) -> dict[str, Any] | None:
    queue_data = load_queue(queue_path)
    history_data = load_history(history_path)
    selected = select_latest_primary_entry(queue_data)
    if selected is None:
        return None

    entry_index, entry = selected
    published_entry = publish_single_entry(
        client=client,
        entry=entry,
        wait_for_image=wait_for_image,
        image_timeout_seconds=image_timeout_seconds,
        image_poll_interval_seconds=image_poll_interval_seconds,
    )
    queue_data.pop(entry_index)
    upsert_history_entry(history_data, published_entry)
    save_queue(queue_path=queue_path, queue_data=queue_data)
    save_history(history_path=history_path, history_data=history_data)
    return {
        "article_slug": str(published_entry.get("article_slug", "")).strip(),
        "variant_type": published_entry.get("variant_type", ""),
        "provider_pin_id": published_entry.get("provider_pin_id"),
        "queue_path": queue_path,
        "history_path": history_path,
    }


def publish_or_queue_pins(pinterest_metadata_path: Path) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[2]
    payload = load_json(pinterest_metadata_path)
    client = PinterestClient.from_env(project_root)

    if not client.should_attempt_publish() or not client.is_configured_for_publish():
        if client.should_attempt_publish():
            print("[pinterest] publish mode requested but credentials are missing. Falling back to queue.")
        queue_path, history_path, queued_count = queue_pinterest_payload(
            payload=payload,
            queue_path=QUEUE_FILE_PATH,
            history_path=HISTORY_FILE_PATH,
            provider_mode="queue",
        )
        return {
            "mode": "queue",
            "published_count": 0,
            "queue_path": queue_path,
            "history_path": history_path,
            "queued_count": queued_count,
        }

    queue_path, history_path, queued_count = queue_pinterest_payload(
        payload=payload,
        queue_path=QUEUE_FILE_PATH,
        history_path=HISTORY_FILE_PATH,
        provider_mode="live_publish",
    )
    publish_result = process_queue(client=client, queue_path=queue_path, history_path=history_path)
    publish_result["queued_count"] = queued_count
    return publish_result


def main() -> int:
    args = parse_args()
    try:
        result = publish_or_queue_pins(Path(args.pinterest_metadata_path))
        if result.get("queue_path"):
            print(result["queue_path"])
        if result.get("history_path"):
            print(result["history_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
