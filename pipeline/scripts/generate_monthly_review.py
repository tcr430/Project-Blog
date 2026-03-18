from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from generate_cluster_report import build_cluster_intelligence_outputs, load_json, load_article_entries
from generate_content_plan import build_content_plan_outputs


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_METADATA_DIR = PROJECT_ROOT / "_data" / "article_metadata"
DEFAULT_CLUSTER_REPORT_PATH = PROJECT_ROOT / "pipeline" / "data" / "keyword_cluster_report.json"
DEFAULT_CLUSTER_INDEX_PATH = PROJECT_ROOT / "pipeline" / "data" / "article_cluster_index.json"
DEFAULT_CONTENT_PLAN_PATH = PROJECT_ROOT / "pipeline" / "reports" / "content_plan.json"
DEFAULT_PINTEREST_SIGNALS_PATH = PROJECT_ROOT / "pipeline" / "reports" / "pinterest_topic_signals.json"
DEFAULT_PINTEREST_INTELLIGENCE_PATH = PROJECT_ROOT / "pipeline" / "reports" / "pinterest_intelligence_report.json"
DEFAULT_PINTEREST_SUMMARY_PATH = PROJECT_ROOT / "pipeline" / "data" / "pinterest_performance_summary.json"
DEFAULT_PIN_PERFORMANCE_PATH = PROJECT_ROOT / "analytics" / "pin_performance.json"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "pipeline" / "reports" / "monthly_review.json"

REVIEW_LOOKBACK_DAYS = 30
STALE_CLUSTER_DAYS = 60
REFRESH_ARTICLE_DAYS = 90
PIN_REFRESH_DAYS = 14
TOP_LIMIT = 8


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a monthly editorial performance review.")
    parser.add_argument("--metadata-dir", type=str, default=str(DEFAULT_METADATA_DIR))
    parser.add_argument("--cluster-report-path", type=str, default=str(DEFAULT_CLUSTER_REPORT_PATH))
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--content-plan-path", type=str, default=str(DEFAULT_CONTENT_PLAN_PATH))
    parser.add_argument("--pinterest-signals-path", type=str, default=str(DEFAULT_PINTEREST_SIGNALS_PATH))
    parser.add_argument("--pinterest-intelligence-path", type=str, default=str(DEFAULT_PINTEREST_INTELLIGENCE_PATH))
    parser.add_argument("--pinterest-summary-path", type=str, default=str(DEFAULT_PINTEREST_SUMMARY_PATH))
    parser.add_argument("--pin-performance-path", type=str, default=str(DEFAULT_PIN_PERFORMANCE_PATH))
    parser.add_argument("--output-path", type=str, default=str(DEFAULT_OUTPUT_PATH))
    return parser.parse_args()


def parse_iso_date(value: Any) -> datetime | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    if "T" in cleaned:
        try:
            parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    try:
        return datetime.strptime(cleaned, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def normalize_identifier(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


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


def float_value(value: Any) -> float:
    if isinstance(value, bool) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError:
            return 0.0
    return 0.0


def safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_cluster_maps(cluster_report: dict[str, Any], content_plan: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    report_map = {
        str(row.get("cluster_id") or "").strip() or normalize_identifier(row.get("cluster_name") or ""): row
        for row in safe_list(cluster_report.get("clusters"))
        if isinstance(row, dict)
    }
    plan_map = {
        str(row.get("cluster_id") or "").strip() or normalize_identifier(row.get("cluster_name") or ""): row
        for row in safe_list(content_plan.get("clusters"))
        if isinstance(row, dict)
    }
    return report_map, plan_map


def aggregate_pin_rows(pin_performance: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    rows = [row for row in safe_list(pin_performance.get("pins")) if isinstance(row, dict)]
    rows_by_article: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        slug = str(row.get("article_slug") or "").strip()
        if slug:
            rows_by_article.setdefault(slug, []).append(row)
    return rows, rows_by_article


def summarize_performance(rows: list[dict[str, Any]], *, key_name: str, value_name: str) -> list[dict[str, Any]]:
    aggregate: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get(key_name) or "").strip()
        label = str(row.get(value_name) or "").strip()
        if not key:
            continue
        bucket = aggregate.setdefault(
            key,
            {
                key_name: key,
                value_name: label or key,
                "pin_count": 0,
                "published_pin_count": 0,
                "analytics_ready_pin_count": 0,
                "average_performance_score": 0.0,
                "average_saves": 0.0,
                "average_outbound_clicks": 0.0,
                "average_impressions": 0.0,
            },
        )
        bucket["pin_count"] += 1
        bucket["published_pin_count"] += 1 if bool(row.get("published")) else 0
        bucket["analytics_ready_pin_count"] += 1 if bool(row.get("analytics_ready")) else 0
        bucket["average_performance_score"] += float_value(row.get("performance_score"))
        bucket["average_saves"] += int_value(row.get("saves"))
        bucket["average_outbound_clicks"] += int_value(row.get("outbound_clicks"))
        bucket["average_impressions"] += int_value(row.get("impressions"))

    summarized: list[dict[str, Any]] = []
    for bucket in aggregate.values():
        divisor = max(1, int(bucket["pin_count"]))
        summarized.append(
            {
                **bucket,
                "average_performance_score": round(bucket["average_performance_score"] / divisor, 2),
                "average_saves": round(bucket["average_saves"] / divisor, 2),
                "average_outbound_clicks": round(bucket["average_outbound_clicks"] / divisor, 2),
                "average_impressions": round(bucket["average_impressions"] / divisor, 2),
            }
        )

    summarized.sort(
        key=lambda item: (
            -float(item["average_performance_score"]),
            -float(item["average_outbound_clicks"]),
            -float(item["average_saves"]),
            -int(item["pin_count"]),
            str(item.get(value_name) or ""),
        )
    )
    return summarized[:TOP_LIMIT]


def detect_mojibake(*values: Any) -> bool:
    markers = ("Ã", "Â", "�")
    return any(any(marker in str(value or "") for marker in markers) for value in values)


def build_refresh_opportunities(
    article_entries: list[dict[str, Any]],
    metadata_dir: Path,
    cluster_report_map: dict[str, dict[str, Any]],
    content_plan_map: dict[str, dict[str, Any]],
    pin_rows_by_article: dict[str, list[dict[str, Any]]],
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    opportunities: list[dict[str, Any]] = []
    manual_review: list[dict[str, Any]] = []

    for entry in article_entries:
        slug = str(entry.get("article_slug") or "").strip()
        metadata_path = metadata_dir / f"{slug}.json"
        metadata = load_json(metadata_path, default={}) if metadata_path.exists() else {}
        published_at = parse_iso_date(entry.get("published_at") or entry.get("publish_date") or "")
        age_days = (now - published_at).days if published_at else None
        cluster_id = str(entry.get("cluster_id") or "").strip() or normalize_identifier(entry.get("cluster_name") or "")
        cluster_row = cluster_report_map.get(cluster_id, {})
        plan_row = content_plan_map.get(cluster_id, {})
        pin_rows = pin_rows_by_article.get(slug, [])

        opportunity_types: list[str] = []
        reasons: list[str] = []

        if age_days is not None and age_days >= REFRESH_ARTICLE_DAYS:
            cluster_status = str(cluster_row.get("status") or plan_row.get("health_label") or "")
            days_since_latest = int(plan_row.get("days_since_latest") or 0)
            if cluster_status in {"underdeveloped", "saturated"} or days_since_latest >= STALE_CLUSTER_DAYS:
                opportunity_types.append("update")
                reasons.append(f"Article is {age_days} days old in a weak or stale cluster context.")

        cluster_article_count = int(cluster_row.get("article_count") or 0)
        if age_days is not None and age_days >= 45 and cluster_article_count <= 2:
            opportunity_types.append("expansion")
            reasons.append("Article sits in a thin cluster and could likely support deeper coverage or broader expansion.")

        internal_link_suggestions = safe_list(metadata.get("internal_link_suggestions"))
        if cluster_article_count >= 3 and len(internal_link_suggestions) < 2:
            opportunity_types.append("stronger_internal_linking")
            reasons.append("Cluster now has enough related content that this article likely needs a stronger internal reading path.")

        intent_id = normalize_identifier(metadata.get("intent_id") or entry.get("search_intent") or "")
        angle_id = normalize_identifier(metadata.get("angle_id") or entry.get("angle_id") or "")
        affiliate_products = safe_list(metadata.get("affiliate_products"))
        if intent_id in {"comparison", "decision_making"} or angle_id in {"best_options", "budget"}:
            if not affiliate_products:
                opportunity_types.append("better_monetization")
                reasons.append("Decision-led content has no affiliate products attached yet.")

        published_pins = [row for row in pin_rows if bool(row.get("published"))]
        failed_pins = [row for row in pin_rows if str(row.get("status") or "").strip().lower() == "failed"]
        max_pin_score = max((float_value(row.get("performance_score")) for row in pin_rows), default=0.0)
        if age_days is not None and age_days >= PIN_REFRESH_DAYS:
            if not pin_rows or not published_pins or failed_pins or max_pin_score <= 0.0:
                opportunity_types.append("improved_pin_assets")
                reasons.append("Pinterest follow-through is weak or missing, so refreshed creatives could help.")

        if detect_mojibake(entry.get("article_title"), entry.get("excerpt"), metadata.get("title"), metadata.get("excerpt")):
            manual_review.append(
                {
                    "article_slug": slug,
                    "article_title": str(entry.get("article_title") or metadata.get("title") or ""),
                    "issue": "possible_encoding_issue",
                    "note": "This article still appears to contain mojibake markers and should be reviewed manually.",
                }
            )

        if not opportunity_types:
            continue

        priority_score = len(opportunity_types) * 10
        if "update" in opportunity_types:
            priority_score += 8
        if "expansion" in opportunity_types:
            priority_score += 6
        if "better_monetization" in opportunity_types:
            priority_score += 4
        if "improved_pin_assets" in opportunity_types:
            priority_score += 4

        opportunities.append(
            {
                "article_slug": slug,
                "article_title": str(entry.get("article_title") or "").strip(),
                "permalink": str(entry.get("permalink") or "").strip(),
                "cluster_id": cluster_id,
                "cluster_name": str(entry.get("canonical_cluster_name") or entry.get("cluster_name") or "").strip(),
                "subtopic_id": str(entry.get("subtopic_id") or "").strip(),
                "angle_id": str(entry.get("angle_id") or "").strip(),
                "publish_date": str(entry.get("publish_date") or "").strip(),
                "age_days": age_days,
                "opportunity_types": opportunity_types,
                "reasons": reasons,
                "priority_score": priority_score,
            }
        )

    opportunities.sort(key=lambda item: (-int(item["priority_score"]), -int(item.get("age_days") or 0), item["article_slug"]))
    return opportunities[: max(TOP_LIMIT, 12)], manual_review


def build_monthly_review(
    *,
    metadata_dir: Path,
    cluster_report_path: Path,
    cluster_index_path: Path,
    content_plan_path: Path,
    pinterest_signals_path: Path,
    pinterest_intelligence_path: Path,
    pinterest_summary_path: Path,
    pin_performance_path: Path,
) -> dict[str, Any]:
    build_cluster_intelligence_outputs(
        metadata_dir=metadata_dir,
        pinterest_summary_path=pinterest_summary_path,
        cluster_report_path=cluster_report_path,
        cluster_index_path=cluster_index_path,
    )
    build_content_plan_outputs(
        cluster_index_path=cluster_index_path,
        cluster_report_path=cluster_report_path,
        pinterest_signals_path=pinterest_signals_path,
        pinterest_intelligence_path=pinterest_intelligence_path,
        output_path=content_plan_path,
    )

    cluster_report = load_json(cluster_report_path, default={"clusters": []})
    cluster_index = load_json(cluster_index_path, default={"articles": []})
    content_plan = load_json(content_plan_path, default={"clusters": []})
    pin_performance = load_json(pin_performance_path, default={"pins": []})
    pinterest_summary = load_json(pinterest_summary_path, default={})

    article_entries = load_article_entries(metadata_dir)
    cluster_report_map, content_plan_map = build_cluster_maps(cluster_report, content_plan)
    pin_rows, pin_rows_by_article = aggregate_pin_rows(pin_performance)

    now = datetime.now(timezone.utc)
    growing_clusters: list[dict[str, Any]] = []
    weak_clusters: list[dict[str, Any]] = []
    undercovered_subtopics: list[dict[str, Any]] = []
    angle_overuse: list[dict[str, Any]] = []

    for row in safe_list(content_plan.get("clusters")):
        if not isinstance(row, dict):
            continue
        cluster_id = str(row.get("cluster_id") or "").strip()
        latest_date = parse_iso_date(row.get("latest_publication_date") or "")
        days_since_latest = (now - latest_date).days if latest_date else None

        if row.get("health_label") in {"growing", "strong"} and (days_since_latest is None or days_since_latest <= STALE_CLUSTER_DAYS):
            growing_clusters.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_name": str(row.get("cluster_name") or "").strip(),
                    "health_label": str(row.get("health_label") or ""),
                    "article_count": int(row.get("article_count") or 0),
                    "latest_publication_date": str(row.get("latest_publication_date") or ""),
                    "subtopics_covered_count": len(safe_list(row.get("subtopics_covered"))),
                    "underused_angles": list(row.get("underused_angles") or []),
                }
            )

        if row.get("health_label") in {"missing", "underdeveloped", "saturated"} or (days_since_latest is not None and days_since_latest >= STALE_CLUSTER_DAYS):
            weak_clusters.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_name": str(row.get("cluster_name") or "").strip(),
                    "health_label": str(row.get("health_label") or ""),
                    "article_count": int(row.get("article_count") or 0),
                    "latest_publication_date": str(row.get("latest_publication_date") or ""),
                    "days_since_latest": days_since_latest,
                    "missing_subtopics_count": len(safe_list(row.get("subtopics_missing"))),
                    "saturation_state": str(row.get("saturation_state") or ""),
                }
            )

        for missing_name, missing_id in zip(safe_list(row.get("subtopics_missing")), safe_list(row.get("subtopic_ids_missing"))):
            undercovered_subtopics.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_name": str(row.get("cluster_name") or "").strip(),
                    "subtopic_id": str(missing_id or "").strip(),
                    "subtopic_name": str(missing_name or "").strip(),
                    "health_label": str(row.get("health_label") or ""),
                    "recommended_next_concepts": [
                        {
                            "primary_keyword": str(item.get("primary_keyword") or ""),
                            "angle_id": str(item.get("angle_id") or ""),
                            "intent_id": str(item.get("intent_id") or ""),
                        }
                        for item in safe_list(row.get("recommended_concepts"))[:2]
                        if isinstance(item, dict)
                    ],
                }
            )

        if safe_list(row.get("overused_angles")):
            angle_overuse.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_name": str(row.get("cluster_name") or "").strip(),
                    "overused_angles": list(row.get("overused_angles") or []),
                    "angle_distribution": dict(row.get("angle_distribution") or {}),
                }
            )

    growing_clusters.sort(key=lambda item: (-item["article_count"], item["cluster_name"]))
    weak_clusters.sort(
        key=lambda item: (
            -int(item.get("days_since_latest") or 0),
            item["health_label"],
            item["cluster_name"],
        )
    )
    undercovered_subtopics.sort(key=lambda item: (item["health_label"], item["cluster_name"], item["subtopic_name"]))
    angle_overuse.sort(key=lambda item: (len(item["overused_angles"]), item["cluster_name"]), reverse=True)

    strong_pinterest_performers = {
        "clusters": summarize_performance(pin_rows, key_name="cluster_id", value_name="cluster_name"),
        "subtopics": summarize_performance(pin_rows, key_name="subtopic_id", value_name="subtopic_name"),
        "angles": summarize_performance(pin_rows, key_name="angle_id", value_name="angle_id"),
        "visual_styles": summarize_performance(pin_rows, key_name="visual_style_key", value_name="visual_style_key"),
    }

    refresh_opportunities, manual_review = build_refresh_opportunities(
        article_entries=article_entries,
        metadata_dir=metadata_dir,
        cluster_report_map=cluster_report_map,
        content_plan_map=content_plan_map,
        pin_rows_by_article=pin_rows_by_article,
        now=now,
    )

    planning_inputs = {
        "priority_growth_clusters": [item["cluster_id"] for item in weak_clusters[:TOP_LIMIT]],
        "deprioritize_clusters": [
            str(row.get("cluster_id") or "").strip()
            for row in safe_list(content_plan.get("clusters"))
            if isinstance(row, dict) and str(row.get("saturation_state") or "") == "saturated"
        ],
        "refresh_priority_articles": [item["article_slug"] for item in refresh_opportunities[:TOP_LIMIT]],
    }

    return {
        "generated_at": now.isoformat(),
        "review_window_days": REVIEW_LOOKBACK_DAYS,
        "stale_cluster_days": STALE_CLUSTER_DAYS,
        "refresh_article_days": REFRESH_ARTICLE_DAYS,
        "summary": {
            "architecture_cluster_count": int(content_plan.get("total_clusters") or len(safe_list(content_plan.get("clusters")))),
            "published_cluster_count": int(cluster_report.get("cluster_count") or 0),
            "article_count": int(cluster_index.get("article_count") or len(article_entries)),
            "pin_count": int(pin_performance.get("pin_count") or len(pin_rows)),
            "growing_cluster_count": len(growing_clusters),
            "weak_or_stale_cluster_count": len(weak_clusters),
            "undercovered_subtopic_count": len(undercovered_subtopics),
            "angle_overuse_count": len(angle_overuse),
            "refresh_opportunity_count": len(refresh_opportunities),
            "manual_review_count": len(manual_review),
        },
        "clusters_growing_well": growing_clusters[:TOP_LIMIT],
        "clusters_weak_or_stale": weak_clusters[:TOP_LIMIT],
        "subtopics_still_undercovered": undercovered_subtopics[: max(TOP_LIMIT, 12)],
        "angles_overused": angle_overuse[:TOP_LIMIT],
        "strong_pinterest_performers": strong_pinterest_performers,
        "articles_refresh_or_expansion_opportunities": refresh_opportunities,
        "manual_review_items": manual_review,
        "planning_inputs": planning_inputs,
        "notes": {
            "pinterest_data_status": (
                "analytics_ready"
                if int(pinterest_summary.get("published_pin_count") or 0) > 0
                else "limited_or_not_ready"
            ),
            "heuristics": [
                "Weak or stale clusters are based on health label, missing coverage, and days since latest publication.",
                "Refresh opportunities use age, cluster weakness, internal linking gaps, monetization gaps, and Pinterest follow-through.",
                "Pinterest performer rankings come from normalized pin performance rows, not speculative projections.",
            ],
        },
    }


def build_monthly_review_outputs(
    *,
    metadata_dir: Path,
    cluster_report_path: Path,
    cluster_index_path: Path,
    content_plan_path: Path,
    pinterest_signals_path: Path,
    pinterest_intelligence_path: Path,
    pinterest_summary_path: Path,
    pin_performance_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    review = build_monthly_review(
        metadata_dir=metadata_dir,
        cluster_report_path=cluster_report_path,
        cluster_index_path=cluster_index_path,
        content_plan_path=content_plan_path,
        pinterest_signals_path=pinterest_signals_path,
        pinterest_intelligence_path=pinterest_intelligence_path,
        pinterest_summary_path=pinterest_summary_path,
        pin_performance_path=pin_performance_path,
    )
    write_json(output_path, review)
    return {
        "output_path": output_path,
        "review": review,
    }


def main() -> int:
    args = parse_args()
    result = build_monthly_review_outputs(
        metadata_dir=Path(args.metadata_dir),
        cluster_report_path=Path(args.cluster_report_path),
        cluster_index_path=Path(args.cluster_index_path),
        content_plan_path=Path(args.content_plan_path),
        pinterest_signals_path=Path(args.pinterest_signals_path),
        pinterest_intelligence_path=Path(args.pinterest_intelligence_path),
        pinterest_summary_path=Path(args.pinterest_summary_path),
        pin_performance_path=Path(args.pin_performance_path),
        output_path=Path(args.output_path),
    )
    print(result["output_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
