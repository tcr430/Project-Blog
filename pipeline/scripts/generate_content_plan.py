from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from content_architecture import build_article_concepts, load_content_angles, load_content_clusters, resolve_intent_id
from editorial_mix import (
    build_recent_editorial_mix_state,
    classify_editorial_mix,
    editorial_mix_adjustment,
    load_editorial_mix_rules,
)
from topic_clusters import TopicCandidate, build_manual_topic_candidate, build_topic_candidate, normalize_text

DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
DEFAULT_CLUSTER_REPORT_PATH = Path(__file__).resolve().parents[1] / "data" / "keyword_cluster_report.json"
DEFAULT_PINTEREST_SIGNALS_PATH = Path(__file__).resolve().parents[1] / "reports" / "pinterest_topic_signals.json"
DEFAULT_PINTEREST_INTELLIGENCE_PATH = Path(__file__).resolve().parents[1] / "reports" / "pinterest_intelligence_report.json"
DEFAULT_OUTPUT_PATH = Path(__file__).resolve().parents[1] / "reports" / "content_plan.json"

RECOMMENDATIONS_PER_CLUSTER = 3
STRONG_MIN_ARTICLES = 6
GROWING_MIN_ARTICLES = 3
NEAR_CAPACITY_RATIO = 0.85
SATURATED_CAPACITY_RATIO = 1.2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a concept-level content plan.")
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--cluster-report-path", type=str, default=str(DEFAULT_CLUSTER_REPORT_PATH))
    parser.add_argument("--pinterest-signals-path", type=str, default=str(DEFAULT_PINTEREST_SIGNALS_PATH))
    parser.add_argument("--pinterest-intelligence-path", type=str, default=str(DEFAULT_PINTEREST_INTELLIGENCE_PATH))
    parser.add_argument("--output-path", type=str, default=str(DEFAULT_OUTPUT_PATH))
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def parse_iso_date(value: str) -> datetime | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    if "T" in cleaned:
        normalized = cleaned.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None
    try:
        return datetime.strptime(cleaned, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


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


def summarize_capacity_state(
    *,
    article_count: int,
    article_capacity_target: int,
    missing_subtopic_count: int,
    underused_angle_count: int,
) -> dict[str, Any]:
    capacity_target = max(1, int(article_capacity_target or 1))
    capacity_ratio = round(article_count / capacity_target, 2)
    growth_openings = max(0, missing_subtopic_count) + max(0, underused_angle_count)

    if article_count == 0:
        saturation_state = "empty"
        selection_action = "boost"
    elif capacity_ratio < 0.6:
        saturation_state = "needs_growth"
        selection_action = "boost"
    elif capacity_ratio < NEAR_CAPACITY_RATIO:
        saturation_state = "healthy_growth"
        selection_action = "normal"
    elif capacity_ratio <= 1.0 and growth_openings > 0:
        saturation_state = "near_capacity_with_gaps"
        selection_action = "warn"
    elif capacity_ratio <= 1.0:
        saturation_state = "near_capacity"
        selection_action = "warn"
    elif growth_openings > 0:
        saturation_state = "above_target_but_open"
        selection_action = "soft_suppress"
    elif capacity_ratio >= SATURATED_CAPACITY_RATIO:
        saturation_state = "saturated"
        selection_action = "strong_suppress"
    else:
        saturation_state = "over_target"
        selection_action = "soft_suppress"

    return {
        "article_capacity_target": capacity_target,
        "capacity_ratio": capacity_ratio,
        "growth_openings": growth_openings,
        "saturation_state": saturation_state,
        "selection_action": selection_action,
    }


def classify_cluster_health(
    article_count: int,
    article_capacity_target: int,
    missing_subtopic_count: int,
    stale_days: int | None,
    saturation_state: str,
) -> str:
    if saturation_state == "saturated":
        return "saturated"
    if article_count >= STRONG_MIN_ARTICLES and missing_subtopic_count <= 1 and (stale_days is None or stale_days <= 45):
        return "strong"
    if article_count >= GROWING_MIN_ARTICLES and missing_subtopic_count <= 2:
        return "growing"
    if article_count >= 1:
        return "underdeveloped"
    return "missing"


def resolve_article_cluster_key(article: dict[str, Any]) -> str:
    cluster_id = str(article.get("cluster_id") or "").strip()
    if cluster_id:
        return cluster_id
    return normalize_text(article.get("cluster_name") or "uncategorized").replace(" ", "_") or "uncategorized"


def resolve_subtopic_id(article: dict[str, Any]) -> str:
    return str(article.get("subtopic_id") or "").strip() or "legacy_unspecified"


def resolve_angle_id(article: dict[str, Any]) -> str:
    return str(article.get("angle_id") or "").strip() or "legacy_unspecified"


def resolve_intent(article: dict[str, Any]) -> str:
    explicit_intent = str(article.get("intent_id") or "").strip()
    if explicit_intent:
        return explicit_intent
    angle_id = resolve_angle_id(article)
    if angle_id == "legacy_unspecified":
        return "legacy_unspecified"
    return resolve_intent_id(angle_id=angle_id)


def build_pinterest_signal_map(signal_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    signal_map: dict[str, dict[str, Any]] = {}
    for row in signal_data.get("clusters", []) if isinstance(signal_data, dict) else []:
        if not isinstance(row, dict):
            continue
        cluster_name = normalize_text(row.get("cluster_name") or "")
        if cluster_name:
            signal_map[cluster_name] = row
    return signal_map


def build_pinterest_intelligence_maps(report_data: dict[str, Any]) -> dict[str, dict[Any, dict[str, Any]]]:
    if not isinstance(report_data, dict):
        return {"clusters": {}, "subtopics": {}, "angles": {}, "visual_styles": {}}

    cluster_map = {
        str(row.get("cluster_id") or "").strip(): row
        for row in report_data.get("best_performing_clusters", [])
        if isinstance(row, dict) and str(row.get("cluster_id") or "").strip()
    }
    subtopic_map = {
        (str(row.get("cluster_id") or "").strip(), str(row.get("subtopic_id") or "").strip()): row
        for row in report_data.get("best_performing_subtopics", [])
        if isinstance(row, dict) and str(row.get("cluster_id") or "").strip() and str(row.get("subtopic_id") or "").strip()
    }
    angle_map = {
        str(row.get("angle_id") or "").strip(): row
        for row in report_data.get("best_performing_angles", [])
        if isinstance(row, dict) and str(row.get("angle_id") or "").strip()
    }
    visual_style_map = {
        str(row.get("visual_style_key") or "").strip(): row
        for row in report_data.get("best_performing_visual_styles", [])
        if isinstance(row, dict) and str(row.get("visual_style_key") or "").strip()
    }
    return {
        "clusters": cluster_map,
        "subtopics": subtopic_map,
        "angles": angle_map,
        "visual_styles": visual_style_map,
    }


def pinterest_intelligence_boost(row: dict[str, Any] | None) -> int:
    if not isinstance(row, dict):
        return 0
    return int(row.get("signal_boost") or 0)


def build_article_maps(
    index_data: dict[str, Any],
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, int]],
    dict[str, dict[str, int]],
]:
    articles_by_cluster: dict[str, list[dict[str, Any]]] = {}
    subtopic_map: dict[str, dict[str, Any]] = {}
    angle_map: dict[str, dict[str, int]] = {}
    intent_map: dict[str, dict[str, int]] = {}

    for article in index_data.get("articles", []) if isinstance(index_data, dict) else []:
        if not isinstance(article, dict):
            continue
        cluster_key = resolve_article_cluster_key(article)
        articles_by_cluster.setdefault(cluster_key, []).append(article)

        subtopic_id = resolve_subtopic_id(article)
        if subtopic_id != "legacy_unspecified":
            row = subtopic_map.setdefault(cluster_key, {})
            row[subtopic_id] = row.get(subtopic_id, 0) + 1

        angle_id = resolve_angle_id(article)
        angle_row = angle_map.setdefault(cluster_key, {})
        angle_row[angle_id] = angle_row.get(angle_id, 0) + 1

        intent_id = resolve_intent(article)
        intent_row = intent_map.setdefault(cluster_key, {})
        intent_row[intent_id] = intent_row.get(intent_id, 0) + 1

    return articles_by_cluster, subtopic_map, angle_map, intent_map


def build_concept_map() -> dict[tuple[str, str, str], dict[str, Any]]:
    concept_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    for concept in build_article_concepts():
        key = (
            str(concept.get("cluster_id") or "").strip(),
            str(concept.get("subtopic_id") or "").strip(),
            str(concept.get("angle_id") or "").strip(),
        )
        if all(key) and key not in concept_map:
            concept_map[key] = concept
    return concept_map


def score_recommendation(
    *,
    cluster_health: str,
    capacity_action: str,
    article_count: int,
    stale_days: int | None,
    subtopic_article_count: int,
    angle_article_count: int,
    angle_known_count: int,
    pinterest_boost: int,
) -> int:
    health_score = {
        "missing": 40,
        "underdeveloped": 28,
        "growing": 14,
        "strong": 4,
    }.get(cluster_health, 0)

    stale_score = 0
    if stale_days is None:
        stale_score = 10
    elif stale_days >= 90:
        stale_score = 12
    elif stale_days >= 45:
        stale_score = 8
    elif stale_days >= 21:
        stale_score = 4

    subtopic_score = 18 if subtopic_article_count == 0 else 8 if subtopic_article_count == 1 else 2
    if angle_article_count == 0:
        angle_score = 12
    elif angle_article_count == 1:
        angle_score = 6
    else:
        angle_score = 0

    capacity_score = {
        "boost": 10,
        "normal": 4,
        "warn": -2,
        "soft_suppress": -8,
        "strong_suppress": -14,
    }.get(capacity_action, 0)

    diversity_penalty = 0
    if article_count >= 3 and angle_known_count <= 1:
        diversity_penalty = 0
    elif article_count >= 5 and angle_known_count >= 4:
        diversity_penalty = -2

    return (
        health_score
        + stale_score
        + subtopic_score
        + angle_score
        + capacity_score
        + pinterest_boost
        + diversity_penalty
    )


def build_cluster_rows(
    index_data: dict[str, Any],
    report_data: dict[str, Any],
    pinterest_signal_data: dict[str, Any],
    pinterest_intelligence_data: dict[str, Any],
) -> list[dict[str, Any]]:
    del report_data
    clusters = load_content_clusters()
    angle_definitions = load_content_angles()
    all_angle_ids = [str(row.get("angle_id") or "").strip() for row in angle_definitions if str(row.get("angle_id") or "").strip()]
    all_intent_ids = sorted(
        {
            resolve_intent_id(str(row.get("angle_id") or ""), str(row.get("intent_id") or ""))
            for row in angle_definitions
            if str(row.get("angle_id") or "").strip()
        }
    )
    concept_map = build_concept_map()
    editorial_mix_rules = load_editorial_mix_rules()
    editorial_mix_state = build_recent_editorial_mix_state(index_data, editorial_mix_rules)
    articles_by_cluster, subtopic_counts_by_cluster, angle_counts_by_cluster, intent_counts_by_cluster = build_article_maps(index_data)
    pinterest_signal_map = build_pinterest_signal_map(pinterest_signal_data)
    pinterest_intelligence_maps = build_pinterest_intelligence_maps(pinterest_intelligence_data)

    now = datetime.now(timezone.utc)
    cluster_rows: list[dict[str, Any]] = []

    for cluster in clusters:
        cluster_id = cluster["cluster_id"]
        cluster_name = cluster["cluster_name"]
        cluster_articles = list(
            articles_by_cluster.get(cluster_id, [])
            or articles_by_cluster.get(normalize_text(cluster_name).replace(" ", "_"), [])
            or []
        )
        subtopic_counts = dict(subtopic_counts_by_cluster.get(cluster_id, {}))
        angle_counts = dict(angle_counts_by_cluster.get(cluster_id, {}))
        intent_counts = dict(intent_counts_by_cluster.get(cluster_id, {}))

        latest_date_value = None
        latest_date = ""
        for article in cluster_articles:
            parsed = parse_iso_date(str(article.get("published_at") or article.get("publish_date") or ""))
            if parsed and (latest_date_value is None or parsed > latest_date_value):
                latest_date_value = parsed
                latest_date = parsed.date().isoformat()
        days_since_latest = (now - latest_date_value).days if latest_date_value else None

        covered_subtopic_ids = [subtopic["subtopic_id"] for subtopic in cluster["subtopics"] if subtopic_counts.get(subtopic["subtopic_id"], 0) > 0]
        missing_subtopic_ids = [subtopic["subtopic_id"] for subtopic in cluster["subtopics"] if subtopic["subtopic_id"] not in covered_subtopic_ids]
        covered_subtopic_names = [
            subtopic["subtopic_name"]
            for subtopic in cluster["subtopics"]
            if subtopic["subtopic_id"] in covered_subtopic_ids
        ]
        missing_subtopic_names = [
            subtopic["subtopic_name"]
            for subtopic in cluster["subtopics"]
            if subtopic["subtopic_id"] in missing_subtopic_ids
        ]

        known_angle_counts = {angle_id: angle_counts.get(angle_id, 0) for angle_id in cluster["allowed_angle_ids"]}
        underused_angles = [angle_id for angle_id in cluster["allowed_angle_ids"] if known_angle_counts.get(angle_id, 0) <= 1]
        known_intent_counts = {intent_id: intent_counts.get(intent_id, 0) for intent_id in all_intent_ids}
        underused_intents = [intent_id for intent_id in all_intent_ids if known_intent_counts.get(intent_id, 0) <= 1]
        article_count = len(cluster_articles)
        overused_angles = [
            angle_id
            for angle_id in cluster["allowed_angle_ids"]
            if known_angle_counts.get(angle_id, 0) >= 2
            and known_angle_counts.get(angle_id, 0) / max(article_count, 1) >= 0.5
        ]
        overused_intents = [
            intent_id
            for intent_id in all_intent_ids
            if known_intent_counts.get(intent_id, 0) >= 2
            and known_intent_counts.get(intent_id, 0) / max(article_count, 1) >= 0.5
        ]

        capacity_state = summarize_capacity_state(
            article_count=article_count,
            article_capacity_target=int(cluster.get("article_capacity_target", 8) or 8),
            missing_subtopic_count=len(missing_subtopic_ids),
            underused_angle_count=len(underused_angles),
        )

        health_label = classify_cluster_health(
            article_count=article_count,
            article_capacity_target=capacity_state["article_capacity_target"],
            missing_subtopic_count=len(missing_subtopic_ids),
            stale_days=days_since_latest,
            saturation_state=capacity_state["saturation_state"],
        )

        signal_row = pinterest_signal_map.get(cluster_name, {})
        cluster_intelligence = pinterest_intelligence_maps["clusters"].get(cluster_id, {})
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
        pinterest_intelligence = {
            "cluster_signal_label": str(cluster_intelligence.get("signal_label") or "no_data"),
            "cluster_signal_boost": pinterest_intelligence_boost(cluster_intelligence),
        }

        recommendation_candidates: list[dict[str, Any]] = []
        for subtopic in cluster["subtopics"]:
            subtopic_id = subtopic["subtopic_id"]
            subtopic_name = subtopic["subtopic_name"]
            subtopic_article_count = int(subtopic_counts.get(subtopic_id, 0))
            for angle_id in subtopic["allowed_angle_ids"]:
                concept = concept_map.get((cluster_id, subtopic_id, angle_id))
                if concept is None:
                    continue
                angle_article_count = int(known_angle_counts.get(angle_id, 0))
                intent_id = str(concept.get("intent_id") or resolve_intent_id(angle_id))
                intent_article_count = int(known_intent_counts.get(intent_id, 0))
                subtopic_intelligence = pinterest_intelligence_maps["subtopics"].get((cluster_id, subtopic_id), {})
                angle_intelligence = pinterest_intelligence_maps["angles"].get(angle_id, {})
                intelligence_boost = pinterest_intelligence_boost(subtopic_intelligence) + pinterest_intelligence_boost(angle_intelligence)
                mix = classify_editorial_mix(
                    concept,
                    cluster_health=health_label,
                    pinterest_signal_label=str(pinterest_signal.get("label") or ""),
                    pinterest_intelligence_label=str(cluster_intelligence.get("signal_label") or ""),
                )
                recommendation_score = score_recommendation(
                    cluster_health=health_label,
                    capacity_action=str(capacity_state["selection_action"]),
                    article_count=article_count,
                    stale_days=days_since_latest,
                    subtopic_article_count=subtopic_article_count,
                    angle_article_count=angle_article_count,
                    angle_known_count=sum(1 for count in known_angle_counts.values() if count > 0),
                    pinterest_boost=pinterest_signal["boost"],
                )
                recommendation_score += intelligence_boost
                strong_opportunity = (
                    bool(concept.get("season") or cluster.get("season") or concept.get("holiday") or cluster.get("holiday"))
                    or str(concept.get("source") or "") == "pinterest_trends_api"
                    or intent_id in {"comparison", "decision_making"}
                )
                mix_adjustment, mix_reason = editorial_mix_adjustment(
                    mix["primary"],
                    rules=editorial_mix_rules,
                    mix_state=editorial_mix_state,
                    stage="planning",
                    strong_opportunity=strong_opportunity,
                )
                recommendation_score += mix_adjustment
                if intent_article_count == 0:
                    recommendation_score += 6
                elif intent_article_count == 1:
                    recommendation_score += 3
                recommendation_candidates.append(
                    {
                        "cluster_id": cluster_id,
                        "cluster_name": cluster_name,
                        "domain_id": cluster["domain_id"],
                        "subtopic_id": subtopic_id,
                        "subtopic_name": subtopic_name,
                        "angle_id": angle_id,
                        "intent_id": intent_id,
                        "modifier": str(concept.get("modifier") or ""),
                        "primary_keyword": str(concept.get("primary_keyword") or concept.get("trend_keyword") or ""),
                        "secondary_keywords": list(concept.get("secondary_keywords") or []),
                        "search_intent": str(concept.get("search_intent") or ""),
                        "season": str(concept.get("season") or cluster.get("season") or ""),
                        "holiday": str(concept.get("holiday") or cluster.get("holiday") or ""),
                        "source": "content_plan",
                        "editorial_mix_primary": mix["primary"],
                        "editorial_mix_tags": mix["tags"],
                        "score": recommendation_score,
                        "pinterest_intelligence_boost": intelligence_boost,
                        "why": {
                            "missing_subtopic": subtopic_article_count == 0,
                            "underused_angle": angle_article_count <= 1,
                            "underused_intent": intent_article_count <= 1,
                            "stale_cluster": days_since_latest is None or days_since_latest >= 45,
                            "cluster_health": health_label,
                            "pinterest_subtopic_signal": str(subtopic_intelligence.get("signal_label") or "no_data"),
                            "pinterest_angle_signal": str(angle_intelligence.get("signal_label") or "no_data"),
                            "editorial_mix_reason": mix_reason,
                        },
                    }
                )

        recommendation_candidates.sort(
            key=lambda item: (
                -int(item["score"]),
                item["subtopic_name"],
                item["angle_id"],
                item["primary_keyword"],
            )
        )

        selected_recommendations: list[dict[str, Any]] = []
        seen_primary_keywords: set[str] = set()
        for candidate in recommendation_candidates:
            primary_keyword = normalize_text(candidate["primary_keyword"])
            if not primary_keyword or primary_keyword in seen_primary_keywords:
                continue
            seen_primary_keywords.add(primary_keyword)
            selected_recommendations.append(candidate)
            if len(selected_recommendations) >= RECOMMENDATIONS_PER_CLUSTER:
                break

        priority_score = (
            {
                "missing": 45,
                "underdeveloped": 30,
                "growing": 15,
                "strong": 5,
                "saturated": -8,
            }.get(health_label, 0)
            + len(missing_subtopic_ids) * 5
            + len(underused_angles) * 2
            + (10 if days_since_latest is None else 8 if days_since_latest >= 90 else 5 if days_since_latest >= 45 else 0)
            + {
                "boost": 10,
                "normal": 4,
                "warn": -2,
                "soft_suppress": -8,
                "strong_suppress": -14,
            }.get(str(capacity_state["selection_action"]), 0)
            + pinterest_signal["boost"]
            + pinterest_intelligence["cluster_signal_boost"]
        )

        angle_distribution = {
            angle_id: known_angle_counts.get(angle_id, 0)
            for angle_id in cluster["allowed_angle_ids"]
        }
        if angle_counts.get("legacy_unspecified", 0):
            angle_distribution["legacy_unspecified"] = angle_counts["legacy_unspecified"]
        intent_distribution = {
            intent_id: known_intent_counts.get(intent_id, 0)
            for intent_id in all_intent_ids
        }
        if intent_counts.get("legacy_unspecified", 0):
            intent_distribution["legacy_unspecified"] = intent_counts["legacy_unspecified"]

        cluster_rows.append(
            {
                "cluster_id": cluster_id,
                "cluster_name": cluster_name,
                "domain_id": cluster["domain_id"],
                "health_label": health_label,
                "article_count": article_count,
                "article_capacity_target": capacity_state["article_capacity_target"],
                "capacity_ratio": capacity_state["capacity_ratio"],
                "growth_openings": capacity_state["growth_openings"],
                "saturation_state": capacity_state["saturation_state"],
                "selection_action": capacity_state["selection_action"],
                "latest_publication_date": latest_date,
                "days_since_latest": days_since_latest,
                "subtopics_total": len(cluster["subtopics"]),
                "subtopics_covered": covered_subtopic_names,
                "subtopic_ids_covered": covered_subtopic_ids,
                "subtopics_missing": missing_subtopic_names,
                "subtopic_ids_missing": missing_subtopic_ids,
                "angle_distribution": angle_distribution,
                "underused_angles": underused_angles,
                "overused_angles": overused_angles,
                "intent_distribution": intent_distribution,
                "underused_intents": underused_intents,
                "overused_intents": overused_intents,
                "recommended_concepts": selected_recommendations,
                "recommended_topics": [item["primary_keyword"] for item in selected_recommendations],
                "priority_score": priority_score,
                "season": cluster.get("season", ""),
                "holiday": cluster.get("holiday", ""),
                "source": cluster.get("source", "content_architecture"),
                "pinterest_signal": pinterest_signal,
                "pinterest_intelligence": pinterest_intelligence,
                "seed_keywords": list(cluster.get("seed_keywords", [])),
                "allowed_angle_ids": [angle_id for angle_id in cluster["allowed_angle_ids"] if angle_id in all_angle_ids],
            }
        )

    priority_order = {"missing": 0, "underdeveloped": 1, "growing": 2, "strong": 3, "saturated": 4}
    cluster_rows.sort(
        key=lambda item: (
            priority_order.get(item["health_label"], 99),
            -int(item["priority_score"]),
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
    pinterest_intelligence_data: dict[str, Any],
) -> dict[str, Any]:
    del report_data
    editorial_mix_rules = load_editorial_mix_rules()
    editorial_mix_state = build_recent_editorial_mix_state(index_data, editorial_mix_rules)
    cluster_rows = build_cluster_rows(
        index_data=index_data,
        report_data={},
        pinterest_signal_data=pinterest_signal_data,
        pinterest_intelligence_data=pinterest_intelligence_data,
    )
    summary = {
        "missing": sum(1 for row in cluster_rows if row["health_label"] == "missing"),
        "underdeveloped": sum(1 for row in cluster_rows if row["health_label"] == "underdeveloped"),
        "growing": sum(1 for row in cluster_rows if row["health_label"] == "growing"),
        "strong": sum(1 for row in cluster_rows if row["health_label"] == "strong"),
        "saturated": sum(1 for row in cluster_rows if row["health_label"] == "saturated"),
    }
    saturation_summary: dict[str, int] = {}
    for row in cluster_rows:
        state = str(row.get("saturation_state") or "unknown")
        saturation_summary[state] = saturation_summary.get(state, 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_clusters": len(cluster_rows),
        "summary": summary,
        "saturation_summary": saturation_summary,
        "editorial_mix_rules": editorial_mix_rules,
        "editorial_mix_state": editorial_mix_state,
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
    pinterest_intelligence_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    index_data = load_json(cluster_index_path, {"articles": []})
    report_data = load_json(cluster_report_path, {"clusters": []})
    pinterest_signal_data = load_json(pinterest_signals_path, {"clusters": []})
    pinterest_intelligence_data = load_json(
        pinterest_intelligence_path,
        {
            "best_performing_clusters": [],
            "best_performing_subtopics": [],
            "best_performing_angles": [],
            "best_performing_visual_styles": [],
        },
    )
    plan = build_content_plan(
        index_data=index_data,
        report_data=report_data,
        pinterest_signal_data=pinterest_signal_data,
        pinterest_intelligence_data=pinterest_intelligence_data,
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

    clusters = {cluster["cluster_id"]: cluster for cluster in load_content_clusters()}
    candidates: list[TopicCandidate] = []
    seen_keywords: set[str] = set()

    for row in plan.get("clusters", []):
        if not isinstance(row, dict):
            continue
        if row.get("health_label") not in {"missing", "underdeveloped", "growing"}:
            continue

        cluster_id = str(row.get("cluster_id") or "").strip()
        cluster_name = normalize_text(row.get("cluster_name") or "")
        cluster_config = clusters.get(cluster_id)
        recommendations = row.get("recommended_concepts", [])

        for recommendation in recommendations:
            if not isinstance(recommendation, dict):
                continue
            primary_keyword = normalize_text(recommendation.get("primary_keyword") or "")
            if not primary_keyword or primary_keyword in seen_keywords:
                continue
            seen_keywords.add(primary_keyword)

            if cluster_config is not None:
                all_keywords = normalize_keyword_list(
                    list(cluster_config.get("seed_keywords", []))
                    + list(recommendation.get("secondary_keywords", []))
                    + [primary_keyword]
                )
                candidates.append(
                    build_topic_candidate(
                        cluster_name=cluster_name,
                        primary_keyword=primary_keyword,
                        all_keywords=list(dict.fromkeys([primary_keyword, *all_keywords])),
                        season=str(recommendation.get("season") or cluster_config.get("season") or ""),
                        holiday=str(recommendation.get("holiday") or cluster_config.get("holiday") or ""),
                        source="content_plan",
                    )
                    | {
                        "domain_id": str(recommendation.get("domain_id") or cluster_config.get("domain_id") or ""),
                        "cluster_id": cluster_id,
                        "subtopic_id": str(recommendation.get("subtopic_id") or ""),
                        "subtopic_name": str(recommendation.get("subtopic_name") or ""),
                        "angle_id": str(recommendation.get("angle_id") or ""),
                        "intent_id": str(recommendation.get("intent_id") or ""),
                        "modifier": str(recommendation.get("modifier") or ""),
                        "editorial_mix_primary": str(recommendation.get("editorial_mix_primary") or ""),
                        "editorial_mix_tags": list(recommendation.get("editorial_mix_tags") or []),
                    }
                )
            else:
                candidates.append(build_manual_topic_candidate(primary_keyword))

            if len(candidates) >= limit:
                return candidates

    return candidates


def main() -> int:
    args = parse_args()
    result = build_content_plan_outputs(
        cluster_index_path=Path(args.cluster_index_path),
        cluster_report_path=Path(args.cluster_report_path),
        pinterest_signals_path=Path(args.pinterest_signals_path),
        pinterest_intelligence_path=Path(args.pinterest_intelligence_path),
        output_path=Path(args.output_path),
    )
    print(result["output_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
