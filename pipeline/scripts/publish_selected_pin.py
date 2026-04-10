from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from pinterest_client import PinterestClient
from publish_pins import (
    HISTORY_FILE_PATH,
    QUEUE_FILE_PATH,
    backfill_primary_entry_if_missing,
    load_history,
    load_queue,
    publish_single_entry,
    save_history,
    save_queue,
    upsert_history_entry,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish one explicitly selected Pinterest queue entry."
    )
    parser.add_argument(
        "--article-slug",
        required=True,
        help="Article slug whose pin should be published.",
    )
    parser.add_argument(
        "--variant-key",
        default="pin-1",
        help="Variant key to publish, such as pin-1, pin-2, pin-3, or pin-4.",
    )
    parser.add_argument(
        "--image-timeout-seconds",
        type=int,
        default=180,
        help="How long to wait for the public pin image URL to become reachable.",
    )
    parser.add_argument(
        "--image-poll-interval-seconds",
        type=int,
        default=5,
        help="How often to retry the public pin image URL check.",
    )
    return parser.parse_args()


def select_queue_entry(
    queue_data: list[dict[str, Any]],
    *,
    article_slug: str,
    variant_key: str,
) -> tuple[int, dict[str, Any]] | None:
    normalized_slug = article_slug.strip()
    normalized_variant = variant_key.strip().lower()
    for index, entry in enumerate(queue_data):
        if str(entry.get("article_slug", "")).strip() != normalized_slug:
            continue
        if str(entry.get("variant_key", "")).strip().lower() != normalized_variant:
            continue
        if str(entry.get("status", "")).strip().lower() == "published":
            continue
        return index, entry
    return None


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    article_slug = args.article_slug.strip()
    variant_key = args.variant_key.strip() or "pin-1"

    client = PinterestClient.from_env(project_root)
    if not client.should_attempt_publish():
        print("Error: PINTEREST_MODE must be set to publish.")
        return 1
    if not client.is_configured_for_publish():
        print("Error: Pinterest publish credentials are incomplete.")
        return 1
    if not article_slug:
        print("Error: --article-slug cannot be empty.")
        return 1

    try:
        if variant_key.lower() == "pin-1":
            backfill_primary_entry_if_missing(project_root=project_root, article_slug=article_slug)

        queue_data = load_queue(QUEUE_FILE_PATH)
        history_data = load_history(HISTORY_FILE_PATH)
        selected = select_queue_entry(
            queue_data,
            article_slug=article_slug,
            variant_key=variant_key,
        )
        if selected is None:
            print(
                f"[pinterest] no pending queue entry found for "
                f"{article_slug} / {variant_key}. Skipping."
            )
            return 0

        entry_index, entry = selected
        published_entry = publish_single_entry(
            client=client,
            entry=entry,
            wait_for_image=True,
            image_timeout_seconds=args.image_timeout_seconds,
            image_poll_interval_seconds=args.image_poll_interval_seconds,
        )
        queue_data.pop(entry_index)
        upsert_history_entry(history_data, published_entry)
        save_queue(QUEUE_FILE_PATH, queue_data)
        save_history(HISTORY_FILE_PATH, history_data)

        print(
            f"[pinterest] selected pin published: {article_slug} / {variant_key} "
            f"({published_entry.get('variant_type', '')})"
        )
        print(HISTORY_FILE_PATH)
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
