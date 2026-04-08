from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pinterest_client import PinterestClient
from publish_pins import publish_primary_article_pin


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish pending primary Pinterest pins for the most recently published articles."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=2,
        help="How many recent published articles to inspect (default: 2).",
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


def find_recent_article_slugs(metadata_dir: Path, count: int) -> list[str]:
    if not metadata_dir.exists():
        raise FileNotFoundError(f"Metadata directory not found: {metadata_dir}")

    candidates: list[tuple[str, str]] = []
    for path in sorted(metadata_dir.glob("*.json")):
        data = load_json(path)
        published_at = str(data.get("published_at", "")).strip()
        slug = str(data.get("slug", "")).strip()
        if not published_at or not slug:
            continue
        candidates.append((published_at, slug))

    ordered = sorted(candidates, reverse=True)
    unique_slugs: list[str] = []
    seen: set[str] = set()
    for _, slug in ordered:
        if slug in seen:
            continue
        unique_slugs.append(slug)
        seen.add(slug)
        if len(unique_slugs) >= max(1, count):
            break
    return unique_slugs


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    client = PinterestClient.from_env(project_root)

    if not client.should_attempt_publish():
        print("Error: PINTEREST_MODE must be set to publish.")
        return 1
    if not client.is_configured_for_publish():
        print("Error: Pinterest publish credentials are incomplete.")
        return 1

    try:
        article_slugs = find_recent_article_slugs(Path(args.metadata_dir), count=args.count)
        if not article_slugs:
            print("[pinterest] no recent published articles found. Skipping.")
            return 0

        published_count = 0
        skipped_count = 0

        for slug in article_slugs:
            print(f"[pinterest] attempting primary-pin publish for recent article: {slug}")
            try:
                result = publish_primary_article_pin(
                    client=client,
                    article_slug=slug,
                    wait_for_image=True,
                    image_timeout_seconds=args.image_timeout_seconds,
                    image_poll_interval_seconds=args.image_poll_interval_seconds,
                )
                published_count += 1
                print(
                    f"[pinterest] primary article pin published: {result['article_slug']} "
                    f"({result['variant_type']})"
                )
            except Exception as exc:
                skipped_count += 1
                print(f"[pinterest] skipped {slug}: {exc}")

        print(
            f"[pinterest] recent primary-pin run complete: "
            f"published={published_count}, skipped={skipped_count}"
        )
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
