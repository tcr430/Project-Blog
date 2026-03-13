from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from pinterest_client import PinterestClient
from publish_pins import load_history, parse_timestamp, save_history

HISTORY_FILE_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_history.json"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_SYNC_INTERVAL_HOURS = 24
DEFAULT_MAX_ITEMS = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Pinterest analytics for published pins and update history."
    )
    parser.add_argument(
        "--history-path",
        type=str,
        default=str(HISTORY_FILE_PATH),
        help="Path to pinterest_history.json.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"How many days of analytics to request (default: {DEFAULT_LOOKBACK_DAYS}).",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=DEFAULT_MAX_ITEMS,
        help=f"Maximum published pins to sync per run (default: {DEFAULT_MAX_ITEMS}).",
    )
    return parser.parse_args()


def should_sync_pinterest_analytics(project_root: Path) -> bool:
    load_dotenv(project_root / ".env")
    value = (os.getenv("PINTEREST_SYNC_ANALYTICS") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def normalize_metric_key(metric_name: str) -> str:
    return metric_name.strip().lower().replace("-", "_").replace(" ", "_")


def extract_metric_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, dict):
        for key in ["value", "total", "sum", "metric_value"]:
            candidate = value.get(key)
            if isinstance(candidate, (int, float)):
                return int(candidate)
    return None


def normalize_analytics_payload(payload: dict[str, Any]) -> dict[str, int | None]:
    metrics = {
        "impressions": None,
        "outbound_clicks": None,
        "saves": None,
        "pin_clicks": None,
        "closeups": None,
    }

    metric_aliases = {
        "impression": "impressions",
        "impressions": "impressions",
        "outbound_click": "outbound_clicks",
        "outbound_clicks": "outbound_clicks",
        "save": "saves",
        "saves": "saves",
        "pin_click": "pin_clicks",
        "pin_clicks": "pin_clicks",
        "closeup": "closeups",
        "closeups": "closeups",
    }

    candidates: list[tuple[str, Any]] = []

    for key, value in payload.items():
        candidates.append((key, value))

    summary_metrics = payload.get("summary_metrics")
    if isinstance(summary_metrics, dict):
        for key, value in summary_metrics.items():
            candidates.append((key, value))

    all_metrics = payload.get("all")
    if isinstance(all_metrics, dict):
        for key, value in all_metrics.items():
            candidates.append((key, value))

    for raw_key, value in candidates:
        normalized_key = normalize_metric_key(raw_key)
        metric_name = metric_aliases.get(normalized_key)
        if not metric_name:
            continue
        metric_value = extract_metric_value(value)
        if metric_value is None:
            continue
        metrics[metric_name] = metric_value

    return metrics


def is_entry_eligible(entry: dict[str, Any], now: datetime) -> bool:
    if str(entry.get("status", "")).strip().lower() != "published":
        return False

    provider_pin_id = str(entry.get("provider_pin_id") or "").strip()
    if not provider_pin_id:
        return False

    last_sync = parse_timestamp(str(entry.get("last_analytics_sync_at", "")), "last_analytics_sync_at")
    if last_sync is None:
        return True

    return last_sync <= now - timedelta(hours=DEFAULT_SYNC_INTERVAL_HOURS)


def sync_pinterest_analytics(
    history_path: Path,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    max_items: int = DEFAULT_MAX_ITEMS,
) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[2]
    client = PinterestClient.from_env(project_root)

    if not client.is_configured_for_analytics():
        print("[pinterest] analytics unavailable: Pinterest analytics credentials are not configured")
        return {
            "mode": "analytics_unavailable",
            "history_path": history_path,
            "updated_count": 0,
            "failed_count": 0,
            "checked_count": 0,
        }

    history_data = load_history(history_path)
    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=max(1, lookback_days))).date().isoformat()
    end_date = now.date().isoformat()

    print("[pinterest] analytics sync started")

    updated_count = 0
    failed_count = 0
    checked_count = 0

    for entry in history_data:
        if checked_count >= max_items:
            break

        status = str(entry.get("status", "")).strip().lower()
        if status != "published":
            continue

        variant_type = str(entry.get("variant_type") or "pin")
        provider_pin_id = str(entry.get("provider_pin_id") or "").strip()
        if not provider_pin_id:
            print(f"[pinterest] analytics unavailable: provider pin ID missing for {variant_type}")
            continue

        if not is_entry_eligible(entry, now):
            continue

        checked_count += 1
        print(f"[pinterest] fetching analytics for pin: {variant_type} ({provider_pin_id})")

        try:
            analytics_payload = client.fetch_pin_analytics(
                pin_id=provider_pin_id,
                start_date=start_date,
                end_date=end_date,
            )
            normalized = normalize_analytics_payload(analytics_payload)
            entry["impressions"] = normalized["impressions"]
            entry["outbound_clicks"] = normalized["outbound_clicks"]
            entry["saves"] = normalized["saves"]
            entry["pin_clicks"] = normalized["pin_clicks"]
            entry["closeups"] = normalized["closeups"]
            entry["last_analytics_sync_at"] = datetime.now(timezone.utc).isoformat()
            updated_count += 1
            print(f"[pinterest] analytics updated: {variant_type}")
        except Exception as exc:
            failed_count += 1
            print(f"[pinterest] analytics sync failed for a pin: {variant_type} ({exc})")

    save_history(history_path=history_path, history_data=history_data)
    return {
        "mode": "analytics_sync",
        "history_path": history_path,
        "updated_count": updated_count,
        "failed_count": failed_count,
        "checked_count": checked_count,
    }


def main() -> int:
    args = parse_args()
    try:
        result = sync_pinterest_analytics(
            history_path=Path(args.history_path),
            lookback_days=args.lookback_days,
            max_items=args.max_items,
        )
        if result.get("history_path"):
            print(result["history_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
