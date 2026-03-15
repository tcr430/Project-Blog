from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from topic_clusters import TopicCandidate, build_manual_topic_candidate, build_topic_candidate, load_default_topic_clusters, normalize_text

DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
DEFAULT_CLUSTER_REPORT_PATH = Path(__file__).resolve().parents[1] / "data" / "keyword_cluster_report.json"
DEFAULT_PINTEREST_SIGNALS_PATH = Path(__file__).resolve().parents[1] / "reports" / "pinterest_topic_signals.json"
DEFAULT_OUTPUT_PATH = Path(__file__).resolve().parents[1] / "reports" / "content_plan.json"

STRONG_MIN_ARTICLES = 6
GROWING_MIN_ARTICLES = 3
UNDERDEVELOPED_MIN_ARTICLES = 1
RECOMMENDATIONS_PER_CLUSTER = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a lightweight cluster-aware content plan.")
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--cluster-report-path", type=str, default=str(DEFAULT_CLUSTER_REPORT_PATH))
    parser.add_argument("--pinterest-signals-path", type=str, default=str(DEFAULT_PINTEREST_SIGNALS_PATH))
    parser.add_argument("--output-path", type=str, default=str(DEFAULT_OUTPUT_PATH))
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def classify_cluster_health(article_count: int) -> str:
    if article_count >= STRONG_MIN_ARTICLES:
        return "strong"
    if article_count >= GROWING_MIN_ARTICLES:
        return "growing"
    if article_count >= UNDERDEVELOPED_MIN_ARTICLES:
        return "underdeveloped"
    return "missing"


def parse_iso_date(value: str) -> datetime | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    try:
        return datetime.strptime(cleaned, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def normalize_keyword_list(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def derive_related_topics(cluster_name: str, existing_keywords: list[str]) -> list[str]:
    base_name = cluster_name
    if base_name.endswith(" styling"):
        base_name = base_name[: -len(" styling")].strip()

    suggestions = [
        f"{base_name} ideas",
        f"how to style {base_name}",
        f"{base_name} decor",
        f"best {base_name} layout",
        f"{base_name} mistakes to avoid",
    ]
    normalized_existing = set(normalize_keyword_list(existing_keywords))
    return [suggestion for suggestion in normalize_keyword_list(suggestions) if suggestion not in normalized_existing]


def build_cluster_rows(
    index_data: dict[str, Any],
    report_data: dict[str, Any],
    pinterest_signal_data: dict[str, Any],
) -> list[dict[str, Any]]:
    articles = index_data.get("articles", []) if isinstance(index_data, dict) else []
    report_rows = report_data.get("clusters", []) if isinstance(report_data, dict) else []
    signal_rows = pinterest_signal_data.get("clusters", []) if isinstance(pinterest_signal_data, dict) else []
    article_map: dict[str, list[dict[str, Any]]] = {}
    report_map: dict[str, dict[str, Any]] = {}
    signal_map: dict[str, dict[str, Any]] = {}

    for article in articles:
        if not isinstance(article, dict):
            continue
        cluster_name = normalize_text(article.get("cluster_name") or "uncategorized") or "uncategorized"
        article_map.setdefault(cluster_name, []).append(article)

    for row in report_rows:
        if not isinstance(row, dict):
            continue
        cluster_name = normalize_text(row.get("cluster_name") or "uncategorized") or "uncategorized"
        report_map[cluster_name] = row

    for row in signal_rows:
        if not isinstance(row, dict):
            continue
        cluster_name = normalize_text(row.get("cluster_name") or "uncategorized") or "uncategorized"
        signal_map[cluster_name] = row

    clusters = load_default_topic_clusters()
    cluster_rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for cluster in clusters:
        cluster_name = cluster["cluster_name"]
        cluster_articles = article_map.get(cluster_name, [])
        report_row = report_map.get(cluster_name, {})
        signal_row = signal_map.get(cluster_name, {})
        article_count = len(cluster_articles)
        health_label = classify_cluster_health(article_count)

        used_keywords = normalize_keyword_list(
            [
                str(article.get("primary_keyword") or "")
                for article in cluster_articles
            ]
            + [
                keyword
                for article in cluster_articles
                for keyword in article.get("secondary_keywords", [])
                if isinstance(keyword, str)
            ]
        )
        cluster_keywords = normalize_keyword_list(cluster["keywords"])
        covered_keywords = [keyword for keyword in cluster_keywords if keyword in set(used_keywords)]
        uncovered_keywords = [keyword for keyword in cluster_keywords if keyword not in set(used_keywords)]
        latest_article_date = str(report_row.get("latest_article_date") or "").strip()
        parsed_latest = parse_iso_date(latest_article_date)
        days_since_latest = (now - parsed_latest).days if parsed_latest else None

        recommended_topics = uncovered_keywords[:RECOMMENDATIONS_PER_CLUSTER]
        if len(recommended_topics) < RECOMMENDATIONS_PER_CLUSTER:
            extras = derive_related_topics(cluster_name, cluster_keywords + covered_keywords + recommended_topics)
            for extra in extras:
                if extra not in recommended_topics:
                    recommended_topics.append(extra)
                if len(recommended_topics) >= RECOMMENDATIONS_PER_CLUSTER:
                    break

        pinterest_signal = {
            "label": str(signal_row.get("signal_label") or "no_data"),
            "boost": int(signal_row.get("signal_boost") or 0),
            "pinterest_article_count": int(signal_row.get("pinterest_article_count") or 0),
            "strong_article_count": int(signal_row.get("strong_article_count") or 0),
            "weak_article_count": int(signal_row.get("weak_article_count") or 0),
            "average_score": float(signal_row.get("average_score") or 0.0),
            "average_outbound_clicks": float(signal_row.get("average_outbound_clicks") or 0.0),
            "average_saves": float(signal_row.get("average_saves") or 0.0),
        }
        priority_score = (
            (25 if health_label == "missing" else 18 if health_label == "underdeveloped" else 8 if health_label == "growing" else 0)
            + pinterest_signal["boost"]
            + max(0, len(uncovered_keywords) * 2)
        )

        cluster_rows.append(
            {
                "cluster_name": cluster_name,
                "cluster_slug": cluster_name.replace(" ", "-"),
                "health_label": health_label,
                "article_count": article_count,
                "latest_article_date": latest_article_date,
                "days_since_latest": days_since_latest,
                "keyword_coverage": {
                    "total_keywords": len(cluster_keywords),
                    "covered_count": len(covered_keywords),
                    "coverage_ratio": round(len(covered_keywords) / len(cluster_keywords), 2) if cluster_keywords else 0.0,
                    "covered_keywords": covered_keywords,
                    "uncovered_keywords": uncovered_keywords,
                },
                "primary_keywords_used": report_row.get("primary_keywords_used", []),
                "representative_article_slugs": report_row.get("representative_article_slugs", []),
                "recommended_topics": recommended_topics,
                "pinterest_signal": pinterest_signal,
                "priority_score": priority_score,
                "season": cluster.get("season", ""),
                "holiday": cluster.get("holiday", ""),
                "source": cluster.get("source", "cluster"),
            }
        )

    priority_order = {"missing": 0, "underdeveloped": 1, "growing": 2, "strong": 3}
    cluster_rows.sort(
        key=lambda item: (
            priority_order.get(item["health_label"], 99),
            -(item["priority_score"]),
            item["article_count"],
            -(item["days_since_latest"] if item["days_since_latest"] is not None else -1),
            item["cluster_name"],
        )
    )
    return cluster_rows


def build_content_plan(
    index_data: dict[str, Any],
    report_data: dict[str, Any],
    pinterest_signal_data: dict[str, Any],
) -> dict[str, Any]:
    cluster_rows = build_cluster_rows(
        index_data=index_data,
        report_data=report_data,
        pinterest_signal_data=pinterest_signal_data,
    )
    summary = {
        "missing": sum(1 for row in cluster_rows if row["health_label"] == "missing"),
        "underdeveloped": sum(1 for row in cluster_rows if row["health_label"] == "underdeveloped"),
        "growing": sum(1 for row in cluster_rows if row["health_label"] == "growing"),
        "strong": sum(1 for row in cluster_rows if row["health_label"] == "strong"),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_clusters": len(cluster_rows),
        "summary": summary,
        "top_pinterest_clusters": [
            row["cluster_name"]
            for row in cluster_rows
            if row.get("pinterest_signal", {}).get("label") in {"hot", "warm"}
        ][:5],
        "priority_clusters": [
            row["cluster_name"]
            for row in cluster_rows
            if row["health_label"] in {"missing", "underdeveloped"}
        ],
        "clusters": cluster_rows,
    }


def write_content_plan(output_path: Path, plan: dict[str, Any]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def build_content_plan_outputs(
    *,
    cluster_index_path: Path,
    cluster_report_path: Path,
    pinterest_signals_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    index_data = load_json(cluster_index_path, {"articles": []})
    report_data = load_json(cluster_report_path, {"clusters": []})
    pinterest_signal_data = load_json(pinterest_signals_path, {"clusters": []})
    plan = build_content_plan(
        index_data=index_data,
        report_data=report_data,
        pinterest_signal_data=pinterest_signal_data,
    )
    write_content_plan(output_path, plan)
    return {
        "output_path": output_path,
        "plan": plan,
        "priority_cluster_count": len(plan["priority_clusters"]),
    }


def select_topic_candidates_from_plan(plan: dict[str, Any], limit: int) -> list[TopicCandidate]:
    if limit <= 0:
        return []

    clusters = {cluster["cluster_name"]: cluster for cluster in load_default_topic_clusters()}
    candidates: list[TopicCandidate] = []
    seen_keywords: set[str] = set()

    for row in plan.get("clusters", []):
        if not isinstance(row, dict):
            continue
        if row.get("health_label") not in {"missing", "underdeveloped", "growing"}:
            continue

        cluster_name = normalize_text(row.get("cluster_name") or "")
        recommended_topics = [normalize_text(topic) for topic in row.get("recommended_topics", []) if normalize_text(topic)]
        cluster_config = clusters.get(cluster_name)
        if cluster_config is None:
            cluster_config = {
                "cluster_name": cluster_name,
                "keywords": recommended_topics,
                "season": str(row.get("season") or "").strip(),
                "holiday": str(row.get("holiday") or "").strip(),
                "source": str(row.get("source") or "content_plan").strip(),
            }

        all_keywords = normalize_keyword_list(list(cluster_config.get("keywords", [])) + recommended_topics)
        for topic in recommended_topics:
            if topic in seen_keywords:
                continue
            seen_keywords.add(topic)
            if cluster_name:
                candidates.append(
                    build_topic_candidate(
                        cluster_name=cluster_name,
                        primary_keyword=topic,
                        all_keywords=list(dict.fromkeys([topic, *all_keywords])),
                        season=str(cluster_config.get("season") or ""),
                        holiday=str(cluster_config.get("holiday") or ""),
                        source="content_plan",
                    )
                )
            else:
                candidates.append(build_manual_topic_candidate(topic))
            if len(candidates) >= limit:
                return candidates

    return candidates


def main() -> int:
    args = parse_args()
    result = build_content_plan_outputs(
        cluster_index_path=Path(args.cluster_index_path),
        cluster_report_path=Path(args.cluster_report_path),
        pinterest_signals_path=Path(args.pinterest_signals_path),
        output_path=Path(args.output_path),
    )
    print(result["output_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
