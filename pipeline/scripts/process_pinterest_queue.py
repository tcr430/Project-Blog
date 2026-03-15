from __future__ import annotations

import argparse
from pathlib import Path

from pinterest_client import PinterestClient
from publish_pins import HISTORY_FILE_PATH, QUEUE_FILE_PATH, process_queue
from pinterest_performance_summary import build_performance_summary
from plan_pinterest_repins import plan_pinterest_repins


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish due items from the Pinterest queue without generating a new article."
    )
    parser.add_argument(
        "--queue-path",
        type=str,
        default=str(QUEUE_FILE_PATH),
        help="Path to pinterest_queue.json.",
    )
    parser.add_argument(
        "--history-path",
        type=str,
        default=str(HISTORY_FILE_PATH),
        help="Path to pinterest_history.json.",
    )
    return parser.parse_args()


def process_pinterest_queue_only(queue_path: Path, history_path: Path) -> dict[str, object]:
    project_root = Path(__file__).resolve().parents[2]
    client = PinterestClient.from_env(project_root)

    if not client.should_attempt_publish():
        raise RuntimeError(
            "Pinterest queue processing requires PINTEREST_MODE=publish."
        )
    if not client.is_configured_for_publish():
        raise RuntimeError(
            "Pinterest publish credentials are incomplete. "
            "Set a valid access token or refresh-token OAuth config and a board ID."
        )

    result = process_queue(client=client, queue_path=queue_path, history_path=history_path)

    if history_path.exists():
        summary_result = build_performance_summary(
            history_path=history_path,
            summary_path=project_root / "pipeline" / "data" / "pinterest_performance_summary.json",
            article_scores_path=project_root / "pipeline" / "data" / "pinterest_article_scores.json",
        )
        result["performance_summary"] = summary_result

        repin_result = plan_pinterest_repins(
            article_scores_path=Path(summary_result["article_scores_path"]),
            history_path=history_path,
            queue_path=queue_path,
        )
        result["repin_plan"] = repin_result

    return result


def main() -> int:
    args = parse_args()

    try:
        result = process_pinterest_queue_only(
            queue_path=Path(args.queue_path),
            history_path=Path(args.history_path),
        )
        print(
            f"[pinterest] queue processed: published={result.get('published_count', 0)}, "
            f"failed={result.get('failed_count', 0)}, skipped={result.get('skipped_count', 0)}"
        )
        if result.get("queue_path"):
            print(result["queue_path"])
        if result.get("history_path"):
            print(result["history_path"])
        if isinstance(result.get("performance_summary"), dict):
            print(result["performance_summary"]["summary_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
