from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

HISTORY_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_history.json"
SUMMARY_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_performance_summary.json"
REPORT_PATH = Path(__file__).resolve().parents[1] / "reports" / "weekly_report.md"
LOOKBACK_DAYS = 7
TOP_LIMIT = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a weekly pipeline markdown report from Pinterest history and summary data."
    )
    parser.add_argument("--history-path", type=str, default=str(HISTORY_PATH))
    parser.add_argument("--summary-path", type=str, default=str(SUMMARY_PATH))
    parser.add_argument("--output-path", type=str, default=str(REPORT_PATH))
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    return parser.parse_args()


def load_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array at {path}")
    return [item for item in data if isinstance(item, dict)]


def load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return {}
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return data


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def int_value(value: Any) -> int:
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value.strip()))
        except ValueError:
            return 0
    return 0


def in_window(moment: datetime | None, start: datetime, end: datetime) -> bool:
    return moment is not None and start <= moment <= end


def pin_performance_score(entry: dict[str, Any]) -> tuple[int, int, int]:
    return (
        int_value(entry.get("outbound_clicks")),
        int_value(entry.get("saves")),
        int_value(entry.get("impressions")),
    )


def top_pins(entries: list[dict[str, Any]], limit: int = TOP_LIMIT) -> list[dict[str, Any]]:
    published = [entry for entry in entries if str(entry.get("status") or "").strip().lower() == "published"]
    published.sort(key=pin_performance_score, reverse=True)
    return published[:limit]


def weak_pins(entries: list[dict[str, Any]], limit: int = TOP_LIMIT) -> list[dict[str, Any]]:
    published = [entry for entry in entries if str(entry.get("status") or "").strip().lower() == "published"]
    published = [
        entry for entry in published
        if any(int_value(entry.get(field)) > 0 for field in ["impressions", "saves", "outbound_clicks"])
    ]
    published.sort(key=pin_performance_score)
    return published[:limit]


def format_pin_line(entry: dict[str, Any]) -> str:
    title = str(entry.get("title") or "Untitled pin")
    variant_type = str(entry.get("variant_type") or "unknown")
    board_name = str((entry.get("board") or {}).get("name") or "Unknown board")
    outbound_clicks = int_value(entry.get("outbound_clicks"))
    saves = int_value(entry.get("saves"))
    impressions = int_value(entry.get("impressions"))
    return (
        f"- {title} ({variant_type}, {board_name}) "
        f"- {outbound_clicks} clicks, {saves} saves, {impressions} impressions"
    )


def format_board_line(entry: dict[str, Any]) -> str:
    board_name = str(entry.get("board_name") or entry.get("board_key") or "Unknown board")
    outbound_clicks = entry.get("average_outbound_clicks")
    saves = entry.get("average_saves")
    impressions = entry.get("average_impressions")
    return (
        f"- {board_name} - avg clicks: {outbound_clicks if outbound_clicks is not None else 'n/a'}, "
        f"avg saves: {saves if saves is not None else 'n/a'}, "
        f"avg impressions: {impressions if impressions is not None else 'n/a'}"
    )


def build_weekly_report(
    *,
    history_path: Path,
    summary_path: Path,
    output_path: Path,
    lookback_days: int = LOOKBACK_DAYS,
) -> Path:
    history = load_json_list(history_path)
    summary = load_json_object(summary_path)

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=max(1, lookback_days))

    recent_created = [
        entry for entry in history
        if in_window(parse_timestamp(str(entry.get("created_at") or "")), window_start, now)
    ]
    recent_published = [
        entry for entry in history
        if in_window(parse_timestamp(str(entry.get("published_at") or "")), window_start, now)
    ]
    recent_scheduled = [
        entry for entry in history
        if str(entry.get("status") or "").strip().lower() in {"queued", "scheduled", "failed"}
        and parse_timestamp(str(entry.get("scheduled_for") or "")) is not None
        and parse_timestamp(str(entry.get("scheduled_for") or "")) >= window_start
    ]
    repins_scheduled = [
        entry for entry in recent_created
        if str(entry.get("distribution_kind") or "").strip().lower() == "repin"
    ]
    article_slugs = sorted({str(entry.get("article_slug") or "") for entry in recent_created if str(entry.get("article_slug") or "").strip()})

    top_pin_entries = top_pins(recent_published)
    weak_pin_entries = weak_pins(recent_published)
    top_boards = list(summary.get("rankings", {}).get("top_boards", []))[:TOP_LIMIT]

    lines = [
        "# Weekly Pipeline Report",
        "",
        f"Generated: {now.date().isoformat()}",
        f"Window: last {max(1, lookback_days)} days",
        "",
        f"Articles Published: {len(article_slugs)}  ",
        f"Pins Generated: {len(recent_created)}  ",
        f"Pins Scheduled: {len(recent_scheduled)}  ",
        f"Pins Published: {len(recent_published)}  ",
        f"Repins Scheduled: {len(repins_scheduled)}",
        "",
        "## Articles Published",
    ]

    if article_slugs:
        lines.extend([f"- {slug}" for slug in article_slugs])
    else:
        lines.append("- No articles were represented in Pinterest activity this week.")

    lines.extend(["", "## Top Performing Pins"])
    if top_pin_entries:
        lines.extend([format_pin_line(entry) for entry in top_pin_entries])
    else:
        lines.append("- No published pin analytics were available for this window.")

    lines.extend(["", "## Top Performing Boards"])
    if top_boards:
        lines.extend([format_board_line(entry) for entry in top_boards])
    else:
        lines.append("- No board performance data is available yet.")

    lines.extend(["", "## Weak Pins"])
    if weak_pin_entries:
        lines.extend([format_pin_line(entry) for entry in weak_pin_entries])
    else:
        lines.append("- No weak pins identified yet.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def main() -> int:
    args = parse_args()
    try:
        report_path = build_weekly_report(
            history_path=Path(args.history_path),
            summary_path=Path(args.summary_path),
            output_path=Path(args.output_path),
            lookback_days=args.lookback_days,
        )
        print(report_path)
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
