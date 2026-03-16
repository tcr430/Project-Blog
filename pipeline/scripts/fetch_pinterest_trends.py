from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pinterest_client import PinterestClient
from topic_clusters import build_topic_candidate, normalize_text

DEFAULT_REGION = "US"
DEFAULT_TREND_TYPE = "monthly"
DEFAULT_INTEREST = "home_decor"
DEFAULT_LIMIT = 50
DEFAULT_CACHE_HOURS = 24
DEFAULT_TRENDS_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_trends.json"
DEFAULT_ENDPOINT_PATH_TEMPLATE = "/trends/keywords/{region}/top/{trend_type}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Pinterest trending keywords and normalize them into pipeline candidates."
    )
    parser.add_argument("--region", type=str, default=DEFAULT_REGION)
    parser.add_argument("--trend-type", type=str, default=DEFAULT_TREND_TYPE)
    parser.add_argument("--interest", type=str, default=DEFAULT_INTEREST)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--cache-hours", type=int, default=DEFAULT_CACHE_HOURS)
    parser.add_argument("--output", type=str, default=str(DEFAULT_TRENDS_PATH))
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore fresh cache and fetch directly from Pinterest.",
    )
    return parser.parse_args()


def is_cache_fresh(path: Path, max_age_hours: int) -> bool:
    if not path.exists():
        return False

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return False

    generated_at = str(payload.get("generated_at", "")).strip()
    if not generated_at:
        return False

    try:
        generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        if generated.tzinfo is None:
            generated = generated.replace(tzinfo=UTC)
    except ValueError:
        return False

    return generated >= datetime.now(UTC) - timedelta(hours=max_age_hours)


def load_cached_trends(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("Pinterest trends cache must contain a JSON object.")
    return payload


def infer_season(keyword: str) -> str:
    normalized = normalize_text(keyword)
    for season in ("spring", "summer", "fall", "autumn", "winter"):
        if season in normalized:
            return "fall" if season == "autumn" else season
    return ""


def infer_holiday(keyword: str) -> str:
    normalized = normalize_text(keyword)
    for holiday in ("christmas", "easter", "halloween", "thanksgiving", "valentines", "valentine"):
        if holiday in normalized:
            return "valentines" if holiday == "valentine" else holiday
    return ""


def extract_keywords(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("trends") or payload.get("items") or payload.get("data") or []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def normalize_trend_row(row: dict[str, Any]) -> dict[str, Any] | None:
    keyword = normalize_text(
        row.get("keyword")
        or row.get("trend_keyword")
        or row.get("query")
        or row.get("name")
        or ""
    )
    if not keyword:
        return None

    score_value = row.get("trend_score")
    if score_value is None:
        score_value = row.get("score")
    if score_value is None:
        score_value = row.get("search_interest")

    try:
        trend_score = int(float(score_value)) if score_value is not None else 0
    except (TypeError, ValueError):
        trend_score = 0

    season = infer_season(keyword)
    holiday = infer_holiday(keyword)
    candidate = build_topic_candidate(
        cluster_name=keyword,
        primary_keyword=keyword,
        all_keywords=[
            keyword,
            f"{keyword} ideas",
            f"how to style {keyword}",
            f"{keyword} decor",
            f"{keyword} mistakes to avoid",
        ],
        season=season,
        holiday=holiday,
        source="pinterest_trends_api",
    )
    candidate["pinterest_trend_score"] = trend_score
    return candidate


def fetch_raw_pinterest_trends(
    *,
    client: PinterestClient,
    region: str,
    trend_type: str,
    interest: str,
    limit: int,
) -> dict[str, Any]:
    path = DEFAULT_ENDPOINT_PATH_TEMPLATE.format(
        region=region.strip().upper(),
        trend_type=normalize_text(trend_type),
    )
    query = {
        "interests": interest.strip(),
        "limit": max(1, min(limit, DEFAULT_LIMIT)),
    }
    return client.api_request(method="GET", path=path, query=query)


def fetch_pinterest_trends(
    *,
    project_root: Path,
    output_path: Path = DEFAULT_TRENDS_PATH,
    region: str = DEFAULT_REGION,
    trend_type: str = DEFAULT_TREND_TYPE,
    interest: str = DEFAULT_INTEREST,
    limit: int = DEFAULT_LIMIT,
    cache_hours: int = DEFAULT_CACHE_HOURS,
    refresh: bool = False,
) -> dict[str, Any]:
    if not refresh and is_cache_fresh(output_path, cache_hours):
        return load_cached_trends(output_path)

    client = PinterestClient.from_env(project_root)
    try:
        raw_payload = fetch_raw_pinterest_trends(
            client=client,
            region=region,
            trend_type=trend_type,
            interest=interest,
            limit=limit,
        )
    except Exception:
        if output_path.exists():
            return load_cached_trends(output_path)
        raise
    raw_rows = extract_keywords(raw_payload)
    candidates: list[dict[str, Any]] = []
    seen_keywords: set[str] = set()
    for row in raw_rows:
        candidate = normalize_trend_row(row)
        if candidate is None:
            continue
        keyword = candidate["trend_keyword"]
        if keyword in seen_keywords:
            continue
        seen_keywords.add(keyword)
        candidates.append(candidate)

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "region": region.strip().upper(),
        "topic_filter": interest.strip(),
        "timeframe": normalize_text(trend_type),
        "source": "pinterest_trends_api",
        "endpoint_path": DEFAULT_ENDPOINT_PATH_TEMPLATE.format(
            region=region.strip().upper(),
            trend_type=normalize_text(trend_type),
        ),
        "raw_count": len(raw_rows),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "raw_response": raw_payload,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    args = parse_args()
    output_path = Path(args.output)
    project_root = Path(__file__).resolve().parents[2]

    try:
        payload = fetch_pinterest_trends(
            project_root=project_root,
            output_path=output_path,
            region=args.region,
            trend_type=args.trend_type,
            interest=args.interest,
            limit=args.limit,
            cache_hours=args.cache_hours,
            refresh=args.refresh,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
