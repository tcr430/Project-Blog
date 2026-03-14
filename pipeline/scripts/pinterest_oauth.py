from __future__ import annotations

import argparse
import json
from pathlib import Path

from pinterest_client import PinterestClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pinterest OAuth helper for authorization URL generation and token exchange."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    authorize_parser = subparsers.add_parser(
        "authorize-url",
        help="Build the Pinterest authorization URL for the configured app.",
    )
    authorize_parser.add_argument(
        "--state",
        type=str,
        default="",
        help="Optional OAuth state value to include in the authorization URL.",
    )

    exchange_parser = subparsers.add_parser(
        "exchange-code",
        help="Exchange a Pinterest OAuth authorization code for tokens.",
    )
    exchange_parser.add_argument(
        "--code",
        type=str,
        required=True,
        help="OAuth authorization code returned by Pinterest.",
    )

    refresh_parser = subparsers.add_parser(
        "refresh-token",
        help="Refresh a Pinterest access token using the configured refresh token.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]
    client = PinterestClient.from_env(project_root)

    try:
        if args.command == "authorize-url":
            print(client.build_authorization_url(state=(args.state or None)))
            return 0

        if args.command == "exchange-code":
            token_payload = client.exchange_authorization_code(args.code)
            print(json.dumps(token_payload, ensure_ascii=False, indent=2))
            return 0

        if args.command == "refresh-token":
            token_payload = client.refresh_access_token()
            print(json.dumps(token_payload, ensure_ascii=False, indent=2))
            return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1

    print("Error: unsupported Pinterest OAuth command.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
