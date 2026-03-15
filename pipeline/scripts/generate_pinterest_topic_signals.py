from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
DEFAULT_PINTEREST_SUMMARY_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_performance_summary.json"
DEFAULT_OUTPUT_PATH = Path(__file__).resolve().parents[1] / "reports" / "pinterest_topic_signals.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Pinterest-informed topic signals from article clusters and pin performance."
    )
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--pinterest-summary-path", type=str, default=str(DEFAULT_PINTEREST_SUMMARY_PATH))
    parser.add_argument("--output-path", type=str, default=str(DEFAULT_OUTPUT_PATH))
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def classify_signal_label(
    *,
    pinterest_article_count: int,
    strong_article_count: int,
    weak_article_count: int,
    average_score: float,
) -> str:
    if pinterest_article_count <= 0:
        return "no_data"
    if strong_article_count >= 1 and average_score >= 2.5:
        return "hot"
    if weak_article_count > strong_article_count and average_score < 1.5:
        return "cold"
    return "warm"


def signal_boost_for_label(label: str) -> int:
    if label == "hot":
        return 18
    if label == "warm":
        return 10
    if label == "cold":
        return -6
    return 0


def build_pinterest_topic_signals(
    cluster_index_data: dict[str, Any],
    pinterest_summary_data: dict[str, Any],
) -> dict[str, Any]:
    index_articles = cluster_index_data.get("articles", []) if isinstance(cluster_index_data, dict) else []
    article_summaries = pinterest_summary_data.get("performance_by_article", []) if isinstance(pinterest_summary_data, dict) else []

    article_lookup: dict[str, dict[str, Any]] = {}
    for article in index_articles:
        if not isinstance(article, dict):
            continue
        article_slug = str(article.get("article_slug") or "").strip()
        if article_slug:
            article_lookup[article_slug] = article

    cluster_map: dict[str, dict[str, Any]] = {}
    for article in index_articles:
        if not isinstance(article, dict):
            continue
        cluster_name = str(article.get("cluster_name") or "").strip()
        article_slug = str(article.get("article_slug") or "").strip()
        if not cluster_name or not article_slug:
            continue

        cluster_row = cluster_map.setdefault(
            cluster_name,
            {
                "cluster_name": cluster_name,
                "article_count": 0,
                "article_slugs": [],
                "primary_keywords": [],
                "secondary_keywords": [],
                "pinterest_articles": [],
            },
        )
        cluster_row["article_count"] += 1
        cluster_row["article_slugs"].append(article_slug)
        primary_keyword = str(article.get("primary_keyword") or "").strip()
        if primary_keyword and primary_keyword not in cluster_row["primary_keywords"]:
            cluster_row["primary_keywords"].append(primary_keyword)
        for keyword in article.get("secondary_keywords", []):
            clean_keyword = str(keyword or "").strip()
            if clean_keyword and clean_keyword not in cluster_row["secondary_keywords"]:
                cluster_row["secondary_keywords"].append(clean_keyword)

    for summary in article_summaries:
        if not isinstance(summary, dict):
            continue
        article_slug = str(summary.get("article_slug") or "").strip()
        if not article_slug:
            continue
        article = article_lookup.get(article_slug)
        if article is None:
            continue

        cluster_name = str(article.get("cluster_name") or "").strip()
        if not cluster_name:
            continue

        cluster_row = cluster_map.setdefault(
            cluster_name,
            {
                "cluster_name": cluster_name,
                "article_count": 0,
                "article_slugs": [],
                "primary_keywords": [],
                "secondary_keywords": [],
                "pinterest_articles": [],
            },
        )
        cluster_row["pinterest_articles"].append(
            {
                "article_slug": article_slug,
                "classification": str(summary.get("classification") or "").strip(),
                "score": int(summary.get("score") or 0),
                "average_impressions": summary.get("average_impressions"),
                "average_saves": summary.get("average_saves"),
                "average_outbound_clicks": summary.get("average_outbound_clicks"),
                "average_engagement_rate": summary.get("average_engagement_rate"),
            }
        )

    cluster_signals: list[dict[str, Any]] = []
    for cluster_name, row in sorted(cluster_map.items()):
        pinterest_articles = row["pinterest_articles"]
        pinterest_article_count = len(pinterest_articles)
        strong_article_count = sum(1 for item in pinterest_articles if item["classification"] == "strong_candidate")
        weak_article_count = sum(1 for item in pinterest_articles if item["classification"] == "underperforming")
        score_values = [float(item["score"]) for item in pinterest_articles]
        average_score = round(sum(score_values) / len(score_values), 2) if score_values else 0.0
        average_outbound_clicks = round(
            sum(float(item.get("average_outbound_clicks") or 0) for item in pinterest_articles) / pinterest_article_count,
            2,
        ) if pinterest_article_count else 0.0
        average_saves = round(
            sum(float(item.get("average_saves") or 0) for item in pinterest_articles) / pinterest_article_count,
            2,
        ) if pinterest_article_count else 0.0
        signal_label = classify_signal_label(
            pinterest_article_count=pinterest_article_count,
            strong_article_count=strong_article_count,
            weak_article_count=weak_article_count,
            average_score=average_score,
        )

        cluster_signals.append(
            {
                "cluster_name": cluster_name,
                "article_count": row["article_count"],
                "article_slugs": row["article_slugs"],
                "primary_keywords": row["primary_keywords"],
                "secondary_keywords": row["secondary_keywords"],
                "pinterest_article_count": pinterest_article_count,
                "strong_article_count": strong_article_count,
                "weak_article_count": weak_article_count,
                "average_score": average_score,
                "average_outbound_clicks": average_outbound_clicks,
                "average_saves": average_saves,
                "signal_label": signal_label,
                "signal_boost": signal_boost_for_label(signal_label),
            }
        )

    summary = {
        "hot": sum(1 for item in cluster_signals if item["signal_label"] == "hot"),
        "warm": sum(1 for item in cluster_signals if item["signal_label"] == "warm"),
        "cold": sum(1 for item in cluster_signals if item["signal_label"] == "cold"),
        "no_data": sum(1 for item in cluster_signals if item["signal_label"] == "no_data"),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cluster_count": len(cluster_signals),
        "summary": summary,
        "clusters": cluster_signals,
    }


def write_signals(output_path: Path, payload: dict[str, Any]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def build_pinterest_topic_signal_outputs(
    *,
    cluster_index_path: Path,
    pinterest_summary_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    cluster_index_data = load_json(cluster_index_path, {"articles": []})
    pinterest_summary_data = load_json(pinterest_summary_path, {"performance_by_article": []})
    signals = build_pinterest_topic_signals(cluster_index_data, pinterest_summary_data)
    write_signals(output_path, signals)
    return {
        "output_path": output_path,
        "signals": signals,
    }


def main() -> int:
    args = parse_args()
    result = build_pinterest_topic_signal_outputs(
        cluster_index_path=Path(args.cluster_index_path),
        pinterest_summary_path=Path(args.pinterest_summary_path),
        output_path=Path(args.output_path),
    )
    print(result["output_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
