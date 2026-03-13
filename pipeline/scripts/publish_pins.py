from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pinterest_client import PinterestClient

QUEUE_FILE_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_queue.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Queue or publish Pinterest variants from a Pinterest metadata file."
    )
    parser.add_argument("pinterest_metadata_path", type=str, help="Path to Pinterest metadata JSON.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return data


def load_queue(queue_path: Path) -> list[dict[str, Any]]:
    if not queue_path.exists():
        return []

    raw = queue_path.read_text(encoding="utf-8").strip()
    if not raw:
        return []

    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"Queue file must contain a JSON array: {queue_path}")
    return data


def save_queue(queue_path: Path, queue_data: list[dict[str, Any]]) -> Path:
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    queue_path.write_text(json.dumps(queue_data, ensure_ascii=False, indent=2), encoding="utf-8")
    return queue_path


def build_queue_entries(payload: dict[str, Any], publish_mode: str) -> list[dict[str, Any]]:
    created_at = datetime.now(timezone.utc).isoformat()
    entries: list[dict[str, Any]] = []

    for variant in payload["variants"]:
        board = variant.get("board") or {}
        board_key = str(board.get("key", "default")).strip() or "default"
        board_name = str(board.get("name", board_key)).strip() or board_key

        entries.append(
            {
                "article_slug": payload["article_slug"],
                "variant_type": variant["variant_type"],
                "board": {
                    "key": board_key,
                    "name": board_name,
                },
                "title": variant["title"],
                "description": variant["description"],
                "image_path": variant["image_path"],
                "target_url": payload["article_url"],
                "status": "queued",
                "created_at": created_at,
                "mode": publish_mode,
                "variant_key": variant["variant_key"],
                "site_root_url": payload["site_root_url"],
            }
        )

    return entries


def queue_pinterest_payload(payload: dict[str, Any], queue_path: Path) -> tuple[Path, int]:
    queue_data = load_queue(queue_path)
    new_entries = build_queue_entries(payload=payload, publish_mode="queue")
    queue_data.extend(new_entries)
    save_queue(queue_path=queue_path, queue_data=queue_data)
    return queue_path, len(new_entries)


def build_public_asset_url(site_root_url: str, image_path: str) -> str:
    clean_root = site_root_url.rstrip("/")
    clean_path = image_path if image_path.startswith("/") else f"/{image_path}"
    return f"{clean_root}{clean_path}"


def process_queue(client: PinterestClient, queue_path: Path) -> dict[str, Any]:
    queue_data = load_queue(queue_path)
    if not queue_data:
        return {
            "mode": "publish",
            "published_count": 0,
            "failed_count": 0,
            "queue_path": queue_path,
        }

    print("[pinterest] authenticating with Pinterest")

    published_count = 0
    failed_count = 0
    remaining_entries: list[dict[str, Any]] = []

    for entry in queue_data:
        board = entry.get("board") or {}
        board_key = str(board.get("key", "default")).strip() or "default"
        image_url = build_public_asset_url(
            site_root_url=str(entry["site_root_url"]),
            image_path=str(entry["image_path"]),
        )
        title = str(entry["title"])
        variant_label = str(entry.get("variant_type", entry.get("variant_key", "pin")))
        print(f"[pinterest] publishing pin: {variant_label} - {title}")

        try:
            client.publish_variant(
                board_key=board_key,
                title=title,
                description=str(entry["description"]),
                article_url=str(entry["target_url"]),
                image_url=image_url,
            )
            published_count += 1
            print(f"[pinterest] pin published successfully: {variant_label}")
        except Exception as exc:
            failed_count += 1
            remaining_entries.append(entry)
            print(f"[pinterest] pin failed, remaining in queue: {variant_label} ({exc})")

    save_queue(queue_path=queue_path, queue_data=remaining_entries)
    return {
        "mode": "publish",
        "published_count": published_count,
        "failed_count": failed_count,
        "queue_path": queue_path,
    }


def publish_or_queue_pins(pinterest_metadata_path: Path) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[2]
    payload = load_json(pinterest_metadata_path)
    client = PinterestClient.from_env(project_root)

    if not client.should_attempt_publish() or not client.is_configured_for_publish():
        if client.should_attempt_publish():
            print("[pinterest] publish mode requested but credentials are missing. Falling back to queue.")
        queue_path, queued_count = queue_pinterest_payload(payload=payload, queue_path=QUEUE_FILE_PATH)
        return {
            "mode": "queue",
            "published_count": 0,
            "queue_path": queue_path,
            "queued_count": queued_count,
        }

    queue_path, queued_count = queue_pinterest_payload(payload=payload, queue_path=QUEUE_FILE_PATH)
    publish_result = process_queue(client=client, queue_path=queue_path)
    publish_result["queued_count"] = queued_count
    return publish_result


def main() -> int:
    args = parse_args()
    try:
        result = publish_or_queue_pins(Path(args.pinterest_metadata_path))
        if result.get("queue_path"):
            print(result["queue_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
