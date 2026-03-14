from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, TypedDict

from topic_clusters import (
    TopicCandidate,
    build_topic_candidate,
    expand_clusters_to_candidates,
    load_default_topic_clusters,
)


class TrendCandidate(TypedDict):
    trend_cluster: str
    trend_keyword: str
    primary_keyword: str
    secondary_keywords: list[str]
    cluster_keywords: list[str]
    search_intent: str
    season: str
    holiday: str
    source: str


DEFAULT_CANDIDATES_PATH = Path(__file__).resolve().parents[1] / "data" / "candidate_trends.json"


BUILTIN_DECOR_TRENDS: list[TrendCandidate] = [
    build_topic_candidate(
        cluster_name="kitchen decor",
        primary_keyword="kitchen decor ideas",
        all_keywords=[
            "kitchen decor ideas",
            "how to style kitchen decor",
            "warm kitchen decor ideas",
            "kitchen decor mistakes to avoid",
        ],
        source="builtin",
    ),
    build_topic_candidate(
        cluster_name="living room styling",
        primary_keyword="organic modern living room ideas",
        all_keywords=[
            "organic modern living room ideas",
            "how to style an organic modern living room",
            "organic modern living room decor",
            "mistakes to avoid in an organic modern living room",
        ],
        source="builtin",
    ),
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
    primary_keyword = _normalize_text(raw.get("primary_keyword", "")) or keyword
    secondary_keywords_raw = raw.get("secondary_keywords", [])
    cluster_keywords_raw = raw.get("cluster_keywords", [])

    if not keyword:
        raise ValueError("Candidate trend is missing trend_keyword.")

    if isinstance(secondary_keywords_raw, list):
        secondary_keywords = [_normalize_text(item) for item in secondary_keywords_raw if _normalize_text(item)]
    else:
        secondary_keywords = []

    if isinstance(cluster_keywords_raw, list):
        cluster_keywords = [_normalize_text(item) for item in cluster_keywords_raw if _normalize_text(item)]
    else:
        cluster_keywords = []

    if not cluster_keywords:
        cluster_keywords = [primary_keyword, *secondary_keywords]

    return {
        "trend_cluster": cluster,
        "trend_keyword": keyword,
        "primary_keyword": primary_keyword,
        "secondary_keywords": secondary_keywords,
        "cluster_keywords": cluster_keywords,
        "search_intent": _normalize_text(raw.get("search_intent", "")) or "styling_advice",
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


def load_cluster_candidates() -> list[TrendCandidate]:
    clusters = load_default_topic_clusters()
    return [dict(item) for item in expand_clusters_to_candidates(clusters)]


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

    cluster_candidates = load_cluster_candidates()
    if cluster_candidates:
        return cluster_candidates

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

