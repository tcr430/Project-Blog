from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, TypedDict

from normalize_keyword_phrase import normalize_phrase
from topic_clusters import normalize_text

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
CONTENT_DOMAINS_PATH = DATA_DIR / "content_domains.json"
CONTENT_ANGLES_PATH = DATA_DIR / "content_angles.json"
CONTENT_CLUSTERS_PATH = DATA_DIR / "content_clusters.json"
CONTENT_SUBTOPICS_PATH = DATA_DIR / "content_subtopics.json"
CONTENT_CONSTRAINTS_PATH = DATA_DIR / "content_constraints.json"


class ContentSubtopic(TypedDict):
    subtopic_id: str
    subtopic_name: str
    cluster_id: str
    description: str
    allowed_angle_ids: list[str]
    keyword_seeds: list[str]
    modifiers: list[str]
    templates: dict[str, str]


class ContentCluster(TypedDict):
    domain_id: str
    cluster_id: str
    cluster_name: str
    description: str
    article_capacity_target: int
    subtopic_ids: list[str]
    allowed_angle_ids: list[str]
    related_clusters: list[str]
    seasonal: bool
    cluster_type: str
    context_phrase: str
    season: str
    holiday: str
    source: str
    modifiers: list[str]
    seed_keywords: list[str]
    subtopics: list[ContentSubtopic]


class ArticleConcept(TypedDict):
    domain_id: str
    cluster_id: str
    trend_cluster: str
    trend_keyword: str
    primary_keyword: str
    secondary_keywords: list[str]
    cluster_keywords: list[str]
    search_intent: str
    intent_id: str
    season: str
    holiday: str
    source: str
    subtopic_id: str
    subtopic_name: str
    angle_id: str
    modifier: str


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def normalize_id(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


def classify_search_intent(angle_id: str) -> str:
    if angle_id == "how_to":
        return "how_to"
    if angle_id == "mistakes":
        return "problem_solution"
    if angle_id == "best_options":
        return "comparison"
    if angle_id == "ideas":
        return "ideas"
    return "styling_advice"


def load_content_domains() -> list[dict[str, Any]]:
    payload = load_json(CONTENT_DOMAINS_PATH, [])
    return payload if isinstance(payload, list) else []


def load_content_angles() -> list[dict[str, Any]]:
    payload = load_json(CONTENT_ANGLES_PATH, [])
    return payload if isinstance(payload, list) else []


@lru_cache(maxsize=1)
def build_angle_lookup() -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in load_content_angles():
        if not isinstance(row, dict):
            continue
        angle_id = normalize_id(row.get("angle_id", ""))
        if angle_id:
            lookup[angle_id] = row
    return lookup


def resolve_intent_id(angle_id: str, explicit_intent_id: str = "") -> str:
    normalized_intent = normalize_id(explicit_intent_id)
    if normalized_intent:
        return normalized_intent

    angle_lookup = build_angle_lookup()
    angle_row = angle_lookup.get(normalize_id(angle_id), {})
    intent_id = normalize_id(angle_row.get("intent_id", ""))
    if intent_id:
        return intent_id

    fallback_map = {
        "ideas": "inspiration",
        "style_specific": "inspiration",
        "how_to": "implementation",
        "mistakes": "problem_solving",
        "best_options": "comparison",
        "budget": "decision_making",
        "small_space": "decision_making",
    }
    return fallback_map.get(normalize_id(angle_id), "inspiration")


def load_content_constraints() -> dict[str, Any]:
    payload = load_json(CONTENT_CONSTRAINTS_PATH, {})
    return payload if isinstance(payload, dict) else {}


def normalize_subtopic_record(raw: dict[str, Any]) -> ContentSubtopic:
    templates_raw = raw.get("templates", {})
    templates = {}
    if isinstance(templates_raw, dict):
        for angle_id, template in templates_raw.items():
            normalized_angle = normalize_id(angle_id)
            normalized_template = normalize_text(template)
            if normalized_angle and normalized_template:
                templates[normalized_angle] = normalized_template

    return {
        "subtopic_id": str(raw.get("subtopic_id", "")).strip(),
        "subtopic_name": str(raw.get("subtopic_name", "")).strip() or str(raw.get("name", "")).strip(),
        "cluster_id": str(raw.get("cluster_id", "")).strip(),
        "description": str(raw.get("description", "")).strip(),
        "allowed_angle_ids": [
            normalize_id(angle_id)
            for angle_id in raw.get("allowed_angle_ids", [])
            if normalize_id(angle_id)
        ],
        "keyword_seeds": [
            normalize_text(seed)
            for seed in raw.get("keyword_seeds", [])
            if normalize_text(seed)
        ],
        "modifiers": [
            normalize_text(modifier)
            for modifier in raw.get("modifiers", [])
            if normalize_text(modifier)
        ],
        "templates": templates,
    }


def normalize_cluster_record(raw: dict[str, Any]) -> ContentCluster:
    return {
        "domain_id": str(raw.get("domain_id", "")).strip() or "decor_foundations",
        "cluster_id": str(raw.get("cluster_id", "")).strip(),
        "cluster_name": normalize_text(raw.get("cluster_name", "")),
        "description": str(raw.get("description", "")).strip(),
        "article_capacity_target": int(raw.get("article_capacity_target", 8) or 8),
        "subtopic_ids": [
            str(subtopic_id).strip()
            for subtopic_id in raw.get("subtopic_ids", [])
            if str(subtopic_id).strip()
        ],
        "allowed_angle_ids": [
            normalize_id(angle_id)
            for angle_id in raw.get("allowed_angle_ids", [])
            if normalize_id(angle_id)
        ],
        "related_clusters": [
            normalize_id(cluster_id)
            for cluster_id in raw.get("related_clusters", [])
            if normalize_id(cluster_id)
        ],
        "seasonal": bool(raw.get("seasonal")),
        "cluster_type": normalize_text(raw.get("cluster_type", "")) or "persisted",
        "context_phrase": normalize_text(raw.get("context_phrase", "")) or normalize_text(raw.get("cluster_name", "")),
        "season": normalize_text(raw.get("season", "")),
        "holiday": normalize_text(raw.get("holiday", "")),
        "source": normalize_text(raw.get("source", "")) or "content_architecture",
        "modifiers": [
            normalize_text(modifier)
            for modifier in raw.get("modifiers", [])
            if normalize_text(modifier)
        ],
        "seed_keywords": [
            normalize_text(keyword)
            for keyword in raw.get("seed_keywords", [])
            if normalize_text(keyword)
        ],
        "subtopics": [],
    }


def load_persisted_subtopics() -> list[ContentSubtopic]:
    payload = load_json(CONTENT_SUBTOPICS_PATH, [])
    if not isinstance(payload, list):
        return []
    subtopics = [normalize_subtopic_record(item) for item in payload if isinstance(item, dict)]
    return [item for item in subtopics if item["cluster_id"] and item["subtopic_id"] and item["subtopic_name"]]


def build_restricted_angle_lookup(constraints: dict[str, Any]) -> dict[tuple[str, str], set[str]]:
    lookup: dict[tuple[str, str], set[str]] = {}
    for item in constraints.get("restricted_subtopic_angles", []):
        if not isinstance(item, dict):
            continue
        cluster_id = str(item.get("cluster_id", "")).strip()
        subtopic_id = str(item.get("subtopic_id", "")).strip()
        allowed_angles = {
            normalize_id(angle_id)
            for angle_id in item.get("allowed_angle_ids", [])
            if normalize_id(angle_id)
        }
        if cluster_id and subtopic_id and allowed_angles:
            lookup[(cluster_id, subtopic_id)] = allowed_angles
    return lookup


def build_modifier_lookup(constraints: dict[str, Any]) -> dict[str, set[str]]:
    lookup: dict[str, set[str]] = {}
    for item in constraints.get("modifier_restrictions", []):
        if not isinstance(item, dict):
            continue
        cluster_id = str(item.get("cluster_id", "")).strip()
        allowed_modifiers = {
            normalize_text(modifier)
            for modifier in item.get("allowed_modifiers", [])
            if normalize_text(modifier)
        }
        if cluster_id and allowed_modifiers:
            lookup[cluster_id] = allowed_modifiers
    return lookup


def apply_constraints(clusters: list[ContentCluster]) -> list[ContentCluster]:
    constraints = load_content_constraints()
    restricted_angles = build_restricted_angle_lookup(constraints)
    modifier_lookup = build_modifier_lookup(constraints)

    constrained: list[ContentCluster] = []
    for cluster in clusters:
        allowed_modifiers = modifier_lookup.get(cluster["cluster_id"])
        if allowed_modifiers is not None:
            cluster["modifiers"] = [
                modifier for modifier in cluster["modifiers"] if modifier in allowed_modifiers
            ]

        filtered_subtopics: list[ContentSubtopic] = []
        for subtopic in cluster["subtopics"]:
            allowed_angles = restricted_angles.get((cluster["cluster_id"], subtopic["subtopic_id"]))
            if allowed_angles is not None:
                subtopic = {
                    **subtopic,
                    "allowed_angle_ids": [
                        angle_id for angle_id in subtopic["allowed_angle_ids"] if angle_id in allowed_angles
                    ],
                    "templates": {
                        angle_id: template
                        for angle_id, template in subtopic["templates"].items()
                        if angle_id in allowed_angles
                    },
                }
            if subtopic["allowed_angle_ids"]:
                filtered_subtopics.append(subtopic)

        if filtered_subtopics:
            cluster = {**cluster, "subtopics": filtered_subtopics}
            constrained.append(cluster)
    return constrained


def load_content_clusters() -> list[ContentCluster]:
    raw_clusters = load_json(CONTENT_CLUSTERS_PATH, [])
    if not isinstance(raw_clusters, list):
        return []

    clusters = [normalize_cluster_record(item) for item in raw_clusters if isinstance(item, dict)]
    clusters = [item for item in clusters if item["cluster_id"] and item["cluster_name"]]

    subtopics_by_cluster: dict[str, list[ContentSubtopic]] = {}
    for subtopic in load_persisted_subtopics():
        subtopics_by_cluster.setdefault(subtopic["cluster_id"], []).append(subtopic)

    hydrated: list[ContentCluster] = []
    for cluster in clusters:
        ordered_subtopics: list[ContentSubtopic] = []
        for subtopic_id in cluster["subtopic_ids"]:
            match = next(
                (
                    item
                    for item in subtopics_by_cluster.get(cluster["cluster_id"], [])
                    if item["subtopic_id"] == subtopic_id
                ),
                None,
            )
            if match is not None:
                ordered_subtopics.append(match)
        if not ordered_subtopics:
            continue
        cluster["subtopics"] = ordered_subtopics
        hydrated.append(cluster)

    return apply_constraints(hydrated)


def render_template(template: str, cluster: ContentCluster) -> str:
    article = "an" if cluster["cluster_name"][:1].lower() in {"a", "e", "i", "o", "u"} else "a"
    return normalize_text(
        template.format(
            cluster=cluster["cluster_name"],
            context=cluster["context_phrase"],
            article=article,
        )
    )


def build_cluster_keyword_pool(cluster: ContentCluster) -> list[str]:
    keywords: list[str] = list(cluster["seed_keywords"])
    for subtopic in cluster["subtopics"]:
        for template in subtopic.get("templates", {}).values():
            keywords.append(
                normalize_phrase(
                    render_template(str(template), cluster),
                    cluster=cluster["cluster_name"],
                    subtopic=subtopic["subtopic_name"],
                )
            )
        keywords.extend(subtopic.get("keyword_seeds", []))
    return list(dict.fromkeys(keyword for keyword in keywords if keyword))


def build_article_concepts() -> list[ArticleConcept]:
    concepts: list[ArticleConcept] = []
    for cluster in load_content_clusters():
        keyword_pool = build_cluster_keyword_pool(cluster)
        for subtopic in cluster["subtopics"]:
            templates = subtopic.get("templates", {})
            for angle_id in subtopic.get("allowed_angle_ids", []):
                template = templates.get(angle_id)
                if not template:
                    continue
                primary_keyword = normalize_phrase(
                    render_template(str(template), cluster),
                    cluster=cluster["cluster_name"],
                    subtopic=subtopic["subtopic_name"],
                    angle=angle_id,
                )
                secondary_keywords = [keyword for keyword in keyword_pool if keyword != primary_keyword][:4]
                concepts.append(
                    {
                        "domain_id": cluster["domain_id"],
                        "cluster_id": cluster["cluster_id"],
                        "trend_cluster": cluster["cluster_name"],
                        "trend_keyword": primary_keyword,
                        "primary_keyword": primary_keyword,
                        "secondary_keywords": secondary_keywords,
                        "cluster_keywords": keyword_pool[:12],
                        "search_intent": classify_search_intent(angle_id),
                        "intent_id": resolve_intent_id(angle_id),
                        "season": cluster["season"],
                        "holiday": cluster["holiday"],
                        "source": cluster["source"],
                        "subtopic_id": str(subtopic["subtopic_id"]),
                        "subtopic_name": str(subtopic["subtopic_name"]),
                        "angle_id": angle_id,
                        "modifier": "",
                    }
                )
    return concepts
