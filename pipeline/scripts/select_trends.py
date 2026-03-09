from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

from trend_history import DEFAULT_NON_SEASONAL_COOLDOWN_DAYS, is_trend_allowed


class TrendCandidate(TypedDict):
    trend_cluster: str
    trend_keyword: str
    season: str
    holiday: str
    source: str


class ScoredTrend(TrendCandidate):
    score: int
    scoring_notes: list[str]


DECOR_KEYWORDS = {
    "decor",
    "interior",
    "styling",
    "room",
    "kitchen",
    "living",
    "bedroom",
    "bathroom",
    "entryway",
    "home",
    "color",
    "texture",
    "furniture",
}

USEFULNESS_HINTS = {
    "small",
    "budget",
    "storage",
    "layout",
    "ideas",
    "guide",
    "mistakes",
    "styling",
    "tips",
    "modern",
    "neutral",
    "cozy",
}


DEFAULT_HISTORY_FILE = Path(__file__).resolve().parents[1] / "data" / "trend_history.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select top decor trends from candidate JSON using history-aware scoring."
    )
    parser.add_argument("candidates_file", type=str, help="Path to candidate trends JSON array.")
    parser.add_argument(
        "--history-file",
        type=str,
        default=str(DEFAULT_HISTORY_FILE),
        help="Path to trend history JSON file.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=3,
        help="Number of selected trends to return (default: 3).",
    )
    parser.add_argument(
        "--cooldown-days",
        type=int,
        default=DEFAULT_NON_SEASONAL_COOLDOWN_DAYS,
        help=f"Cooldown for non-seasonal trends (default: {DEFAULT_NON_SEASONAL_COOLDOWN_DAYS}).",
    )
    return parser.parse_args()


def normalize_text(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", normalized)


def normalize_candidate(raw: dict[str, Any]) -> TrendCandidate:
    keyword = normalize_text(raw.get("trend_keyword", ""))
    cluster = normalize_text(raw.get("trend_cluster", "")) or keyword
    season = normalize_text(raw.get("season", ""))
    holiday = normalize_text(raw.get("holiday", ""))
    source = normalize_text(raw.get("source", "")) or "unknown"

    return {
        "trend_cluster": cluster,
        "trend_keyword": keyword,
        "season": season,
        "holiday": holiday,
        "source": source,
    }


def normalize_candidates(raw_candidates: list[dict[str, Any]]) -> list[TrendCandidate]:
    return [normalize_candidate(item) for item in raw_candidates]


def is_valid_candidate(candidate: TrendCandidate) -> bool:
    keyword = candidate["trend_keyword"]
    if not keyword:
        return False

    words = [word for word in keyword.split() if word]
    if len(words) < 2:
        return False

    if len(keyword) < 8:
        return False

    return True


def reject_invalid_and_duplicates(candidates: list[TrendCandidate]) -> list[TrendCandidate]:
    unique: list[TrendCandidate] = []
    seen: set[tuple[str, str, str, str]] = set()

    for candidate in candidates:
        if not is_valid_candidate(candidate):
            continue

        key = (
            candidate["trend_cluster"],
            candidate["trend_keyword"],
            candidate["season"],
            candidate["holiday"],
        )
        if key in seen:
            continue

        seen.add(key)
        unique.append(candidate)

    return unique


def filter_recently_used(
    candidates: list[TrendCandidate],
    history_path: Path,
    now: datetime,
    cooldown_days: int,
) -> list[TrendCandidate]:
    allowed: list[TrendCandidate] = []

    for candidate in candidates:
        if is_trend_allowed(
            trend_cluster=candidate["trend_cluster"],
            trend_keyword=candidate["trend_keyword"],
            season=candidate["season"],
            holiday=candidate["holiday"],
            cooldown_days=cooldown_days,
            history_path=history_path,
            now=now,
        ):
            allowed.append(candidate)

    return allowed


def score_candidate(candidate: TrendCandidate) -> tuple[int, list[str]]:
    score = 0
    notes: list[str] = []

    score += 40
    notes.append("novelty: eligible by history rules (+40)")

    keyword_words = [word for word in candidate["trend_keyword"].split() if word]
    if 3 <= len(keyword_words) <= 6:
        score += 20
        notes.append("specificity: clear multi-word topic (+20)")
    elif len(keyword_words) >= 2:
        score += 10
        notes.append("specificity: acceptable topic detail (+10)")

    joined = f"{candidate['trend_cluster']} {candidate['trend_keyword']}"
    decor_hits = sum(1 for token in DECOR_KEYWORDS if token in joined)
    decor_points = min(20, decor_hits * 5)
    score += decor_points
    notes.append(f"decor relevance: keyword match strength (+{decor_points})")

    usefulness_hits = sum(1 for token in USEFULNESS_HINTS if token in joined)
    usefulness_points = min(20, usefulness_hits * 5)
    score += usefulness_points
    notes.append(f"article usefulness: practical angle signals (+{usefulness_points})")

    if candidate["season"] or candidate["holiday"]:
        score += 5
        notes.append("seasonality: timely angle bonus (+5)")

    return score, notes


def score_candidates(candidates: list[TrendCandidate]) -> list[ScoredTrend]:
    scored: list[ScoredTrend] = []

    for candidate in candidates:
        score, notes = score_candidate(candidate)
        scored.append(
            {
                **candidate,
                "score": score,
                "scoring_notes": notes,
            }
        )

    return sorted(scored, key=lambda item: (-item["score"], item["trend_keyword"]))


def select_top_trends(
    raw_candidates: list[dict[str, Any]] | list[TrendCandidate],
    history_path: Path | None = None,
    top_n: int = 3,
    cooldown_days: int = DEFAULT_NON_SEASONAL_COOLDOWN_DAYS,
    now: datetime | None = None,
) -> list[ScoredTrend]:
    if top_n <= 0:
        raise ValueError("top_n must be greater than zero.")

    normalized = normalize_candidates([dict(item) for item in raw_candidates])
    unique_valid = reject_invalid_and_duplicates(normalized)

    current_time = now or datetime.now(UTC)
    selected_history_path = history_path or DEFAULT_HISTORY_FILE
    allowed = filter_recently_used(
        candidates=unique_valid,
        history_path=selected_history_path,
        now=current_time,
        cooldown_days=cooldown_days,
    )

    scored = score_candidates(allowed)
    return scored[:top_n]


def load_candidates_from_file(candidates_file: Path) -> list[dict[str, Any]]:
    if not candidates_file.exists():
        raise FileNotFoundError(f"Candidates file not found: {candidates_file}")

    raw = json.loads(candidates_file.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Candidates file must contain a JSON array.")

    return [item for item in raw if isinstance(item, dict)]


def main() -> int:
    args = parse_args()

    try:
        candidates_path = Path(args.candidates_file)
        history_path = Path(args.history_file)
        raw_candidates = load_candidates_from_file(candidates_path)
        selected = select_top_trends(
            raw_candidates=raw_candidates,
            history_path=history_path,
            top_n=args.top,
            cooldown_days=args.cooldown_days,
            now=datetime.now(UTC),
        )

        if not selected:
            raise RuntimeError("No candidate trends passed filtering and history rules.")

        print(json.dumps(selected, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

