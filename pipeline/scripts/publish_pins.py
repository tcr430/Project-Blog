from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pinterest_client import PinterestClient

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
QUEUE_FILE_PATH = DATA_DIR / "pinterest_queue.json"
HISTORY_FILE_PATH = DATA_DIR / "pinterest_history.json"
SCHEDULE_OFFSETS_HOURS = [0, 18, 48, 96]


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
                "variant_type": variant_type,
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
        "variant_type": entry["variant_type"],
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

    for entry in queue_data:
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

        board = entry.get("board") or {}
        board_key = str(board.get("key", "default")).strip() or "default"
        image_url = build_public_asset_url(
            site_root_url=str(entry["site_root_url"]),
            image_path=str(entry["image_path"]),
        )
        title = str(entry["title"])
        print(f"[pinterest] publishing due pin: {variant_label} - {title}")

        try:
            response_payload = client.publish_variant(
                board_key=board_key,
                title=title,
                description=str(entry["description"]),
                article_url=str(entry["target_url"]),
                image_url=image_url,
            )
            published_count += 1
            entry["status"] = "published"
            entry["published_at"] = datetime.now(timezone.utc).isoformat()
            entry["error_message"] = None
            entry["provider_pin_id"] = extract_provider_pin_id(response_payload)
            upsert_history_entry(history_data, entry)
            print(f"[pinterest] pin published: {variant_label}")
            if not entry["provider_pin_id"]:
                print(f"[pinterest] analytics unavailable: provider pin ID missing for {variant_label}")
        except Exception as exc:
            failed_count += 1
            entry["status"] = "failed"
            entry["error_message"] = str(exc)
            remaining_entries.append(entry)
            upsert_history_entry(history_data, entry)
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
