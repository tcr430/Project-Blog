from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
HISTORY_FILE_PATH = DATA_DIR / "pinterest_history.json"
SUMMARY_FILE_PATH = DATA_DIR / "pinterest_performance_summary.json"
ARTICLE_SCORES_FILE_PATH = DATA_DIR / "pinterest_article_scores.json"

METRIC_FIELDS = ["impressions", "saves", "outbound_clicks", "pin_clicks", "closeups"]


def load_history(path: Path = HISTORY_FILE_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return []

    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"Pinterest history must contain a JSON array: {path}")
    return data


def save_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def is_published_entry(entry: dict[str, Any]) -> bool:
    return str(entry.get("status", "")).strip().lower() == "published"


def to_int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value.strip()))
        except ValueError:
            return None
    return None


def safe_average(values: list[int | None]) -> float | None:
    usable = [value for value in values if value is not None]
    if not usable:
        return None
    return round(sum(usable) / len(usable), 2)


def compute_engagement_rate(entry: dict[str, Any]) -> float | None:
    impressions = to_int_or_none(entry.get("impressions"))
    if impressions is None or impressions <= 0:
        return None

    engaged = 0
    found_metric = False
    for field in ["saves", "outbound_clicks", "pin_clicks", "closeups"]:
        value = to_int_or_none(entry.get(field))
        if value is None:
            continue
        engaged += value
        found_metric = True

    if not found_metric:
        return None
    return round(engaged / impressions, 4)


def analytics_ready_count(entries: list[dict[str, Any]]) -> int:
    total = 0
    for entry in entries:
        if any(to_int_or_none(entry.get(field)) is not None for field in METRIC_FIELDS):
            total += 1
    return total


def build_metrics_snapshot(entries: list[dict[str, Any]]) -> dict[str, Any]:
    impressions = [to_int_or_none(entry.get("impressions")) for entry in entries]
    saves = [to_int_or_none(entry.get("saves")) for entry in entries]
    outbound_clicks = [to_int_or_none(entry.get("outbound_clicks")) for entry in entries]
    pin_clicks = [to_int_or_none(entry.get("pin_clicks")) for entry in entries]
    closeups = [to_int_or_none(entry.get("closeups")) for entry in entries]
    engagement_rates = [compute_engagement_rate(entry) for entry in entries]

    return {
        "published_pins": len(entries),
        "analytics_ready_pins": analytics_ready_count(entries),
        "average_impressions": safe_average(impressions),
        "average_saves": safe_average(saves),
        "average_outbound_clicks": safe_average(outbound_clicks),
        "average_pin_clicks": safe_average(pin_clicks),
        "average_closeups": safe_average(closeups),
        "average_engagement_rate": safe_average(engagement_rates),
    }
