from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pinterest_client import PinterestClient
from publish_pins import backfill_primary_entry_if_missing, publish_primary_article_pin


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish the current workflow article's primary Pinterest pin after site deploy."
    )
    parser.add_argument(
        "--run-state-path",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "data" / "current_pinterest_run.json"),
        help="Path to the current Pinterest run state JSON written by weekly_pipeline.py.",
    )
    parser.add_argument(
        "--article-slug",
        type=str,
        default="",
        help="Optional explicit article slug override.",
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


def load_current_article_slug(run_state_path: Path) -> str | None:
    if not run_state_path.exists():
        return None
    data = load_json(run_state_path)
    slug = str(data.get("article_slug", "")).strip()
    return slug or None


def load_run_state(run_state_path: Path) -> dict[str, Any]:
    if not run_state_path.exists():
        return {}
    return load_json(run_state_path)


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
        run_state_path = Path(args.run_state_path)
        run_state = load_run_state(run_state_path)
        article_slug = args.article_slug.strip() or str(run_state.get("article_slug", "")).strip()
        if not article_slug:
            print("[pinterest] no current article slug found for primary-pin publish. Skipping.")
            return 0
        if args.article_slug.strip() == "":
            if not bool(run_state.get("pinterest_step_succeeded")):
                print("[pinterest] pinterest step did not complete successfully for the current article. Skipping primary-pin publish.")
                return 0
            if not bool(run_state.get("primary_pin_rendered")):
                print("[pinterest] current article does not have a rendered primary pin asset. Skipping primary-pin publish.")
                return 0

        print(f"[pinterest] current article selected for primary-pin publish: {article_slug}")
        backfill_primary_entry_if_missing(project_root=project_root, article_slug=article_slug)
        try:
            result = publish_primary_article_pin(
                client=client,
                article_slug=article_slug,
                wait_for_image=True,
                image_timeout_seconds=args.image_timeout_seconds,
                image_poll_interval_seconds=args.image_poll_interval_seconds,
            )
        except RuntimeError as exc:
            if "No queued Pinterest entry found" in str(exc):
                print(f"[pinterest] no pending primary pin found for current article '{article_slug}'. Skipping.")
                return 0
            raise
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
