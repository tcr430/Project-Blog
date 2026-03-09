from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, TypedDict


class TrendCandidate(TypedDict):
    trend_cluster: str
    trend_keyword: str
    season: str
    holiday: str
    source: str


DEFAULT_CANDIDATES_PATH = Path(__file__).resolve().parents[1] / "data" / "candidate_trends.json"


BUILTIN_DECOR_TRENDS: list[TrendCandidate] = [
    {
        "trend_cluster": "kitchen decor",
        "trend_keyword": "terracotta kitchen decor",
        "season": "",
        "holiday": "",
        "source": "builtin",
    },
    {
        "trend_cluster": "living room styling",
        "trend_keyword": "organic modern living room",
        "season": "",
        "holiday": "",
        "source": "builtin",
    },
    {
        "trend_cluster": "bedroom decor",
        "trend_keyword": "layered neutral bedroom",
        "season": "",
        "holiday": "",
        "source": "builtin",
    },
    {
        "trend_cluster": "bathroom decor",
        "trend_keyword": "spa-style bathroom shelving",
        "season": "",
        "holiday": "",
        "source": "builtin",
    },
    {
        "trend_cluster": "small spaces",
        "trend_keyword": "small apartment dining nook",
        "season": "",
        "holiday": "",
        "source": "builtin",
    },
    {
        "trend_cluster": "seasonal decor",
        "trend_keyword": "spring mantel styling ideas",
        "season": "spring",
        "holiday": "",
        "source": "builtin",
    },
    {
        "trend_cluster": "seasonal decor",
        "trend_keyword": "easter brunch table decor",
        "season": "spring",
        "holiday": "easter",
        "source": "builtin",
    },
    {
        "trend_cluster": "entryway decor",
        "trend_keyword": "minimalist entryway storage",
        "season": "",
        "holiday": "",
        "source": "builtin",
    },
    {
        "trend_cluster": "textiles",
        "trend_keyword": "linen and boucle texture mix",
        "season": "",
        "holiday": "",
        "source": "builtin",
    },
    {
        "trend_cluster": "color trends",
        "trend_keyword": "sage green home accents",
        "season": "",
        "holiday": "",
        "source": "builtin",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch candidate decor trends from fallback sources for the MVP."
    )
    parser.add_argument(
        "--candidates-file",
        type=str,
        default=None,
        help="Optional JSON file with candidate trend items.",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="auto",
        choices=["auto", "file", "mock"],
        help="Candidate source mode (default: auto).",
    )
    return parser.parse_args()


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_candidate(raw: dict[str, Any], source: str) -> TrendCandidate:
    keyword = _normalize_text(raw.get("trend_keyword", ""))
    cluster = _normalize_text(raw.get("trend_cluster", "")) or keyword

    if not keyword:
        raise ValueError("Candidate trend is missing trend_keyword.")

    return {
        "trend_cluster": cluster,
        "trend_keyword": keyword,
        "season": _normalize_text(raw.get("season", "")),
        "holiday": _normalize_text(raw.get("holiday", "")),
        "source": _normalize_text(raw.get("source", "")) or source,
    }


def load_candidates_from_file(path: Path) -> list[TrendCandidate]:
    if not path.exists():
        raise FileNotFoundError(f"Candidates file not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Candidates file must contain a JSON array.")

    return [normalize_candidate(item, source="file") for item in data if isinstance(item, dict)]


def load_mock_candidates() -> list[TrendCandidate]:
    return [dict(item) for item in BUILTIN_DECOR_TRENDS]


def fetch_candidate_trends(
    candidates_file: Path | None = None,
    source: str = "auto",
) -> list[TrendCandidate]:
    """Return candidate decor trends with fallback-friendly source selection.

    Source behavior:
    - file: requires a JSON file
    - mock: returns built-in decor candidates
    - auto: tries provided file, then default file, then built-in list
    """

    selected_source = source.strip().lower()

    if selected_source == "file":
        if candidates_file is None:
            raise ValueError("--source file requires --candidates-file.")
        return load_candidates_from_file(candidates_file)

    if selected_source == "mock":
        return load_mock_candidates()

    if candidates_file is not None and candidates_file.exists():
        return load_candidates_from_file(candidates_file)

    if DEFAULT_CANDIDATES_PATH.exists():
        return load_candidates_from_file(DEFAULT_CANDIDATES_PATH)

    return load_mock_candidates()


def main() -> int:
    args = parse_args()

    try:
        file_path = Path(args.candidates_file) if args.candidates_file else None
        trends = fetch_candidate_trends(candidates_file=file_path, source=args.source)
        print(json.dumps(trends, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

