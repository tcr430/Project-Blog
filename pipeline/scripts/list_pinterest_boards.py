from __future__ import annotations

import argparse
import json
from pathlib import Path

from pinterest_client import PinterestClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List Pinterest boards available to the configured account."
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="How many boards to request per API call (default: 100).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the raw board list as JSON instead of a simple table.",
    )
    return parser.parse_args()


def fetch_boards(client: PinterestClient, page_size: int) -> list[dict[str, str]]:
    response = client.api_request(
        method="GET",
        path="/boards",
        query={"page_size": max(1, min(page_size, 250))},
    )

    items = response.get("items", [])
    if not isinstance(items, list):
        raise RuntimeError("Pinterest returned an unexpected boards payload.")

    boards: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        board_id = str(item.get("id") or "").strip()
        name = str(item.get("name") or "").strip()
        privacy = str(item.get("privacy") or "").strip()
        if not board_id or not name:
            continue
        boards.append(
            {
                "id": board_id,
                "name": name,
                "privacy": privacy or "unknown",
            }
        )

    return boards


def print_board_table(boards: list[dict[str, str]]) -> None:
    if not boards:
        print("No Pinterest boards were returned.")
        return

    print("Pinterest boards:\n")
    for board in boards:
        print(f"- {board['name']}")
        print(f"  id: {board['id']}")
        print(f"  privacy: {board['privacy']}")


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    client = PinterestClient.from_env(project_root)

    try:
        boards = fetch_boards(client=client, page_size=args.page_size)
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    if args.json:
        print(json.dumps(boards, ensure_ascii=False, indent=2))
    else:
        print_board_table(boards)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
