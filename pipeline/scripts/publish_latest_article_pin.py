from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pinterest_client import PinterestClient
from publish_pins import publish_primary_article_pin


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish the primary Pinterest pin for the most recently published article."
    )
    parser.add_argument(
        "--metadata-dir",
        type=str,
        default=str(Path(__file__).resolve().parents[2] / "_data" / "article_metadata"),
        help="Directory containing article metadata JSON files.",
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


def load_json(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Metadata JSON must contain an object: {path}")
    return data


def find_latest_article_slug(metadata_dir: Path) -> str:
    if not metadata_dir.exists():
        raise FileNotFoundError(f"Metadata directory not found: {metadata_dir}")

    latest_path: Path | None = None
    latest_published_at = ""
    for path in sorted(metadata_dir.glob("*.json")):
        data = load_json(path)
        published_at = str(data.get("published_at", "")).strip()
        slug = str(data.get("slug", "")).strip()
        if not published_at or not slug:
            continue
        if published_at > latest_published_at:
            latest_published_at = published_at
            latest_path = path

    if latest_path is None:
        raise RuntimeError("No published article metadata files were found.")

    latest_data = load_json(latest_path)
    slug = str(latest_data.get("slug", "")).strip()
    if not slug:
        raise RuntimeError(f"Latest metadata file is missing a slug: {latest_path}")
    print(f"[pinterest] latest article selected for primary-pin publish: {slug}")
    return slug


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    client = PinterestClient.from_env(project_root)

    if not client.should_attempt_publish():
        print("Error: PINTEREST_MODE must be set to publish for immediate pin publishing.")
        return 1
    if not client.is_configured_for_publish():
        print("Error: Pinterest publish credentials are incomplete.")
        return 1

    try:
        article_slug = find_latest_article_slug(Path(args.metadata_dir))
        result = publish_primary_article_pin(
            client=client,
            article_slug=article_slug,
            wait_for_image=True,
            image_timeout_seconds=args.image_timeout_seconds,
            image_poll_interval_seconds=args.image_poll_interval_seconds,
        )
        print(
            f"[pinterest] primary article pin published: {result['article_slug']} "
            f"({result['variant_type']})"
        )
        print(result["history_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
