from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DEFAULT_METADATA_DIR = Path(__file__).resolve().parents[2] / "_data" / "article_metadata"
DEFAULT_PINTEREST_SUMMARY_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_performance_summary.json"
DEFAULT_CLUSTER_REPORT_PATH = Path(__file__).resolve().parents[1] / "data" / "keyword_cluster_report.json"
DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
RECENT_WINDOW_DAYS = 45


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate lightweight keyword cluster and article-cluster index reports."
    )
    parser.add_argument("--metadata-dir", type=str, default=str(DEFAULT_METADATA_DIR))
    parser.add_argument("--pinterest-summary-path", type=str, default=str(DEFAULT_PINTEREST_SUMMARY_PATH))
    parser.add_argument("--cluster-report-path", type=str, default=str(DEFAULT_CLUSTER_REPORT_PATH))
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default

    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def parse_publish_date(metadata_path: Path, metadata: dict[str, Any]) -> datetime | None:
    post_path = str(metadata.get("post_path") or "").strip()
    filename = Path(post_path).stem if post_path else metadata_path.stem
    date_prefix = filename[:10]
    try:
        parsed = datetime.strptime(date_prefix, "%Y-%m-%d")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def build_article_entry(metadata_path: Path, metadata: dict[str, Any]) -> dict[str, Any]:
    publish_date = parse_publish_date(metadata_path, metadata)
    publish_date_iso = publish_date.date().isoformat() if publish_date else ""
    cluster_name = str(metadata.get("topical_cluster") or "").strip() or "uncategorized"
    primary_keyword = str(metadata.get("primary_keyword") or metadata.get("slug") or "").strip()

    return {
        "article_slug": str(metadata.get("slug") or metadata_path.stem).strip(),
        "article_title": str(metadata.get("title") or "").strip(),
        "primary_keyword": primary_keyword,
        "secondary_keywords": normalize_string_list(metadata.get("secondary_keywords")),
        "cluster_name": cluster_name,
        "publish_date": publish_date_iso,
        "tags": normalize_string_list(metadata.get("keywords") or metadata.get("tags")),
        "permalink": str(metadata.get("article_relative_url") or "").strip(),
        "search_intent": str(metadata.get("search_intent") or "").strip(),
        "excerpt": str(metadata.get("excerpt") or metadata.get("meta_description") or "").strip(),
    }


def load_article_entries(metadata_dir: Path) -> list[dict[str, Any]]:
    if not metadata_dir.exists():
        return []

    entries: list[dict[str, Any]] = []
    for metadata_path in sorted(metadata_dir.glob("*.json")):
        try:
            metadata = load_json(metadata_path, default={})
        except json.JSONDecodeError:
            continue
        if not isinstance(metadata, dict):
            continue
        entries.append(build_article_entry(metadata_path, metadata))

    return sorted(entries, key=lambda item: (item["publish_date"], item["article_slug"]), reverse=True)


def extract_ranked_article_slugs(summary_data: dict[str, Any], ranking_name: str) -> set[str]:
    rankings = summary_data.get("rankings", {})
    ranking_items = rankings.get(ranking_name, [])
    result: set[str] = set()
    if not isinstance(ranking_items, list):
        return result

    for item in ranking_items:
        if not isinstance(item, dict):
            continue
        slug = str(item.get("article_slug") or item.get("slug") or "").strip()
        if slug:
            result.add(slug)
    return result


def classify_cluster_status(
    *,
    article_count: int,
    recent_article_count: int,
    promoted_article_count: int,
    weak_article_count: int,
) -> str:
    if article_count >= 3 and promoted_article_count > weak_article_count:
        return "strong"
    if article_count <= 1:
        return "underdeveloped"
    if recent_article_count >= 1 or article_count >= 2:
        return "growing"
    return "underdeveloped"


def build_cluster_report(
    article_entries: list[dict[str, Any]],
    pinterest_summary: dict[str, Any],
) -> dict[str, Any]:
    top_articles = extract_ranked_article_slugs(pinterest_summary, "top_articles")
    weak_articles = extract_ranked_article_slugs(pinterest_summary, "weak_articles")
    cluster_map: dict[str, list[dict[str, Any]]] = {}

    for entry in article_entries:
        cluster_map.setdefault(entry["cluster_name"], []).append(entry)

    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(days=RECENT_WINDOW_DAYS)
    cluster_rows: list[dict[str, Any]] = []

    for cluster_name, entries in sorted(cluster_map.items()):
        publish_dates = [
            datetime.strptime(entry["publish_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            for entry in entries
            if entry["publish_date"]
        ]
        latest_date = max(publish_dates).date().isoformat() if publish_dates else ""
        recent_article_count = sum(1 for item in publish_dates if item >= recent_cutoff)
        promoted_article_count = sum(1 for entry in entries if entry["article_slug"] in top_articles)
        weak_article_count = sum(1 for entry in entries if entry["article_slug"] in weak_articles)
        unique_primary_keywords = list(dict.fromkeys(entry["primary_keyword"] for entry in entries if entry["primary_keyword"]))
        representative_slugs = [entry["article_slug"] for entry in entries[:3]]
        internal_link_targets_available = max(0, len(entries) - 1)

        cluster_rows.append(
            {
                "cluster_name": cluster_name,
                "article_count": len(entries),
                "primary_keywords_used": unique_primary_keywords,
                "latest_article_date": latest_date,
                "representative_article_slugs": representative_slugs,
                "recent_article_count": recent_article_count,
                "internal_link_targets_available": internal_link_targets_available,
                "promoted_article_count": promoted_article_count,
                "weak_article_count": weak_article_count,
                "status": classify_cluster_status(
                    article_count=len(entries),
                    recent_article_count=recent_article_count,
                    promoted_article_count=promoted_article_count,
                    weak_article_count=weak_article_count,
                ),
            }
        )

    cluster_rows.sort(key=lambda item: (-item["article_count"], item["cluster_name"]))
    return {
        "generated_at": now.isoformat(),
        "cluster_count": len(cluster_rows),
        "article_count": len(article_entries),
        "clusters": cluster_rows,
    }


def write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_cluster_intelligence_outputs(
    *,
    metadata_dir: Path,
    pinterest_summary_path: Path,
    cluster_report_path: Path,
    cluster_index_path: Path,
) -> dict[str, Any]:
    article_entries = load_article_entries(metadata_dir)
    pinterest_summary = load_json(pinterest_summary_path, default={})
    if not isinstance(pinterest_summary, dict):
        pinterest_summary = {}

    cluster_report = build_cluster_report(article_entries, pinterest_summary)
    write_json(cluster_report_path, cluster_report)
    write_json(
        cluster_index_path,
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "article_count": len(article_entries),
            "articles": article_entries,
        },
    )
    return {
        "cluster_report_path": cluster_report_path,
        "cluster_index_path": cluster_index_path,
        "article_count": len(article_entries),
        "cluster_count": cluster_report["cluster_count"],
    }


def main() -> int:
    args = parse_args()
    try:
        result = build_cluster_intelligence_outputs(
            metadata_dir=Path(args.metadata_dir),
            pinterest_summary_path=Path(args.pinterest_summary_path),
            cluster_report_path=Path(args.cluster_report_path),
            cluster_index_path=Path(args.cluster_index_path),
        )
        print(result["cluster_report_path"])
        print(result["cluster_index_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
