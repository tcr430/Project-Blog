from __future__ import annotations

import argparse
from pathlib import Path

from pinterest_client import PinterestClient
from publish_pins import publish_latest_primary_pin


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish the newest pending primary Pinterest pin after site deploy."
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
        result = publish_latest_primary_pin(
            client=client,
            wait_for_image=True,
            image_timeout_seconds=args.image_timeout_seconds,
            image_poll_interval_seconds=args.image_poll_interval_seconds,
        )
        if result is None:
            print("[pinterest] no pending primary Pinterest pin found. Skipping.")
            return 0
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
