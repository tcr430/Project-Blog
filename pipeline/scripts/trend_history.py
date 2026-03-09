from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypedDict


class TrendHistoryEntry(TypedDict):
    trend_cluster: str
    trend_keyword: str
    season: str
    holiday: str
    last_used: str
    article_slug: str


DEFAULT_NON_SEASONAL_COOLDOWN_DAYS = 120


def _default_history_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "trend_history.json"


def _normalize_text(value: object) -> str:
    return str(value or "").strip().lower()


def _normalize_entry(entry: dict[str, object]) -> TrendHistoryEntry:
    return {
        "trend_cluster": _normalize_text(entry.get("trend_cluster", "")),
        "trend_keyword": _normalize_text(entry.get("trend_keyword", "")),
        "season": _normalize_text(entry.get("season", "")),
        "holiday": _normalize_text(entry.get("holiday", "")),
        "last_used": str(entry.get("last_used", "")).strip(),
        "article_slug": str(entry.get("article_slug", "")).strip(),
    }


def _save_trend_history(entries: list[TrendHistoryEntry], history_path: Path) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"entries": entries}
    history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_trend_history(history_path: Path | None = None) -> list[TrendHistoryEntry]:
    """Load trend history entries from JSON storage."""
    path = history_path or _default_history_path()
    if not path.exists():
        return []

    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)

    if not isinstance(data, dict):
        raise ValueError("Trend history file must contain a JSON object.")

    entries_raw = data.get("entries", [])
    if not isinstance(entries_raw, list):
        raise ValueError("Trend history 'entries' must be a list.")

    return [_normalize_entry(item) for item in entries_raw if isinstance(item, dict)]


def _parse_last_used(value: str) -> datetime | None:
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed
    except ValueError:
        return None


def _is_seasonal(season: str, holiday: str) -> bool:
    return bool(season.strip() or holiday.strip())


def was_trend_used_recently(
    trend_cluster: str,
    trend_keyword: str,
    cooldown_days: int,
    history_path: Path | None = None,
    now: datetime | None = None,
) -> bool:
    """Check if the same trend cluster+keyword was used within cooldown_days."""
    if cooldown_days < 0:
        raise ValueError("cooldown_days cannot be negative.")

    current_time = now or datetime.now(UTC)
    cutoff = current_time - timedelta(days=cooldown_days)
    cluster_key = trend_cluster.strip().lower()
    keyword_key = trend_keyword.strip().lower()

    for entry in load_trend_history(history_path):
        if entry["trend_cluster"] != cluster_key:
            continue
        if entry["trend_keyword"] != keyword_key:
            continue

        used_at = _parse_last_used(entry["last_used"])
        if used_at and used_at >= cutoff:
            return True

    return False


def is_trend_allowed(
    trend_cluster: str,
    trend_keyword: str,
    season: str = "",
    holiday: str = "",
    cooldown_days: int = DEFAULT_NON_SEASONAL_COOLDOWN_DAYS,
    history_path: Path | None = None,
    now: datetime | None = None,
) -> bool:
    """Apply non-seasonal cooldown and seasonal once-per-year reuse rules."""
    if cooldown_days < 0:
        raise ValueError("cooldown_days cannot be negative.")

    entries = load_trend_history(history_path)
    current_time = now or datetime.now(UTC)
    cluster_key = trend_cluster.strip().lower()
    keyword_key = trend_keyword.strip().lower()
    season_key = season.strip().lower()
    holiday_key = holiday.strip().lower()

    if _is_seasonal(season_key, holiday_key):
        for entry in entries:
            if entry["trend_cluster"] != cluster_key:
                continue
            if entry["trend_keyword"] != keyword_key:
                continue
            if entry["season"] != season_key:
                continue
            if entry["holiday"] != holiday_key:
                continue

            used_at = _parse_last_used(entry["last_used"])
            if used_at and used_at.year == current_time.year:
                return False
        return True

    cutoff = current_time - timedelta(days=cooldown_days)
    for entry in entries:
        if entry["season"] or entry["holiday"]:
            continue
        if entry["trend_cluster"] != cluster_key:
            continue

        used_at = _parse_last_used(entry["last_used"])
        if used_at and used_at >= cutoff:
            return False

    return True


def add_trend_entry(
    trend_cluster: str,
    trend_keyword: str,
    article_slug: str,
    history_path: Path | None = None,
    used_at: datetime | None = None,
    season: str = "",
    holiday: str = "",
) -> TrendHistoryEntry:
    """Add a new trend usage record and save it to JSON storage."""
    cluster_value = trend_cluster.strip().lower()
    keyword_value = trend_keyword.strip().lower()
    slug_value = article_slug.strip()
    season_value = season.strip().lower()
    holiday_value = holiday.strip().lower()

    if not cluster_value:
        raise ValueError("trend_cluster cannot be empty.")
    if not keyword_value:
        raise ValueError("trend_keyword cannot be empty.")
    if not slug_value:
        raise ValueError("article_slug cannot be empty.")

    timestamp = (used_at or datetime.now(UTC)).isoformat()

    new_entry: TrendHistoryEntry = {
        "trend_cluster": cluster_value,
        "trend_keyword": keyword_value,
        "season": season_value,
        "holiday": holiday_value,
        "last_used": timestamp,
        "article_slug": slug_value,
    }

    path = history_path or _default_history_path()
    entries = load_trend_history(path)
    entries.append(new_entry)
    _save_trend_history(entries, path)

    return new_entry

