from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from content_architecture import load_content_clusters, resolve_intent_id

ARTICLE_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"

COMPLEMENTARY_ANGLE_MAP = {
    "ideas": ["how_to", "best_options", "mistakes"],
    "how_to": ["ideas", "best_options", "mistakes"],
    "mistakes": ["how_to", "best_options", "ideas"],
    "best_options": ["how_to", "ideas", "style_specific"],
    "style_specific": ["ideas", "how_to", "best_options"],
    "budget": ["best_options", "how_to", "ideas"],
    "small_space": ["how_to", "best_options", "ideas"],
}

COMPLEMENTARY_INTENT_MAP = {
    "inspiration": ["implementation", "comparison", "decision_making"],
    "implementation": ["inspiration", "problem_solving", "comparison"],
    "problem_solving": ["implementation", "comparison", "decision_making"],
    "comparison": ["implementation", "decision_making", "inspiration"],
    "decision_making": ["comparison", "implementation", "inspiration"],
}


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def normalize_text(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("_", " ").replace("-", " ")
    normalized = re.sub(r"[^a-z0-9\\s]", " ", normalized)
    return re.sub(r"\\s+", " ", normalized).strip()


def normalize_id(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


def tokenize(value: Any) -> set[str]:
    return {token for token in normalize_text(value).split() if token}


def load_article_index(path: Path | None = None) -> list[dict[str, Any]]:
    selected_path = path or ARTICLE_CLUSTER_INDEX_PATH
    payload = load_json(selected_path, {"articles": []})
    rows = payload.get("articles", []) if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)]


def build_cluster_lookup() -> dict[str, dict[str, Any]]:
    return {
        str(cluster.get("cluster_id") or "").strip(): cluster
        for cluster in load_content_clusters()
        if str(cluster.get("cluster_id") or "").strip()
    }


def resolve_article_intent(article_like: dict[str, Any]) -> str:
    explicit_intent = normalize_id(article_like.get("intent_id", ""))
    if explicit_intent:
        return explicit_intent
    return resolve_intent_id(
        angle_id=normalize_id(article_like.get("angle_id", "")),
        explicit_intent_id=article_like.get("intent_id", ""),
    )


def resolve_subtopic_order(cluster_lookup: dict[str, dict[str, Any]], cluster_id: str, subtopic_id: str) -> int:
    cluster = cluster_lookup.get(cluster_id, {})
    subtopics = cluster.get("subtopics", []) if isinstance(cluster, dict) else []
    for index, subtopic in enumerate(subtopics):
        if str(subtopic.get("subtopic_id") or "").strip() == subtopic_id:
            return index
    return -1


def build_anchor_text(article: dict[str, Any], relationship: str) -> str:
    primary_keyword = str(article.get("primary_keyword") or "").strip()
    title = str(article.get("article_title") or article.get("title") or "").strip()
    subtopic_name = str(article.get("subtopic_name") or "").strip()
    if relationship == "adjacent_subtopic" and subtopic_name:
        return f"{subtopic_name.lower()} guide"
    if relationship == "complementary_angle" and primary_keyword:
        return primary_keyword
    if relationship == "related_cluster":
        cluster_name = str(article.get("canonical_cluster_name") or article.get("cluster_name") or "").strip()
        if cluster_name:
            return cluster_name
    return primary_keyword or title


def build_blurb(article: dict[str, Any], relationship: str) -> str:
    if relationship == "same_cluster":
        return "A strong next read in the same cluster."
    if relationship == "adjacent_subtopic":
        return "Covers a nearby subtopic that expands the same topic naturally."
    if relationship == "complementary_angle":
        return "Adds a complementary angle so the reader can move from one intent to the next."
    if relationship == "related_cluster":
        return "Extends the topic into a closely related cluster."
    return "A relevant follow-up read."


def build_internal_link_suggestions(
    *,
    topic_context: dict[str, Any],
    article_title: str = "",
    article_slug: str = "",
    cluster_index_path: Path | None = None,
    limit: int = 4,
) -> list[dict[str, Any]]:
    cluster_lookup = build_cluster_lookup()
    article_rows = load_article_index(cluster_index_path)
    cluster_id = str(topic_context.get("cluster_id") or "").strip()
    subtopic_id = str(topic_context.get("subtopic_id") or "").strip()
    angle_id = normalize_id(topic_context.get("angle_id", ""))
    intent_id = resolve_article_intent(topic_context)
    related_clusters = {
        normalize_id(item)
        for item in cluster_lookup.get(cluster_id, {}).get("related_clusters", [])
        if normalize_id(item)
    }
    current_tokens = tokenize(article_title or topic_context.get("primary_keyword", ""))
    current_slug = str(article_slug or "").strip()
    complementary_angles = set(COMPLEMENTARY_ANGLE_MAP.get(angle_id, []))
    complementary_intents = set(COMPLEMENTARY_INTENT_MAP.get(intent_id, []))

    scored: list[tuple[int, dict[str, Any]]] = []
    for article in article_rows:
        permalink = str(article.get("permalink") or "").strip()
        target_slug = str(article.get("article_slug") or "").strip()
        if not permalink:
            continue
        if current_slug and target_slug == current_slug:
            continue

        target_cluster_id = str(article.get("cluster_id") or "").strip()
        target_subtopic_id = str(article.get("subtopic_id") or "").strip()
        target_angle_id = normalize_id(article.get("angle_id", ""))
        target_intent_id = resolve_article_intent(article)
        relationship = ""
        score = 0

        if cluster_id and target_cluster_id == cluster_id:
            relationship = "same_cluster"
            score += 40
            if subtopic_id and target_subtopic_id and target_subtopic_id != subtopic_id:
                current_position = resolve_subtopic_order(cluster_lookup, cluster_id, subtopic_id)
                target_position = resolve_subtopic_order(cluster_lookup, cluster_id, target_subtopic_id)
                if current_position >= 0 and target_position >= 0 and abs(current_position - target_position) <= 1:
                    relationship = "adjacent_subtopic"
                    score += 18
            if target_angle_id in complementary_angles:
                relationship = "complementary_angle"
                score += 16
            if target_intent_id in complementary_intents:
                relationship = "complementary_angle"
                score += 12
            if target_subtopic_id == subtopic_id and target_angle_id == angle_id:
                score -= 18
        elif target_cluster_id in related_clusters:
            relationship = "related_cluster"
            score += 24
            if target_intent_id in complementary_intents:
                score += 8
        else:
            continue

        title_similarity = len(current_tokens & tokenize(article.get("article_title") or "")) if current_tokens else 0
        score += min(6, title_similarity * 2)

        suggestion = {
            "relationship": relationship or "related",
            "title": str(article.get("article_title") or "").strip(),
            "slug": target_slug,
            "permalink": permalink,
            "cluster_id": target_cluster_id,
            "cluster_name": str(article.get("canonical_cluster_name") or article.get("cluster_name") or "").strip(),
            "subtopic_id": target_subtopic_id,
            "subtopic_name": str(article.get("subtopic_name") or "").strip(),
            "angle_id": target_angle_id,
            "intent_id": target_intent_id,
            "anchor_text": build_anchor_text(article, relationship or "related"),
            "blurb": build_blurb(article, relationship or "related"),
        }
        scored.append((score, suggestion))

    scored.sort(key=lambda item: (-item[0], item[1]["title"]))
    selected: list[dict[str, Any]] = []
    used_relationships: set[str] = set()
    used_urls: set[str] = set()

    for _, suggestion in scored:
        if suggestion["permalink"] in used_urls:
            continue
        relationship = suggestion["relationship"]
        if relationship not in used_relationships or len(selected) < 2:
            selected.append(suggestion)
            used_relationships.add(relationship)
            used_urls.add(suggestion["permalink"])
        if len(selected) >= limit:
            return selected

    for _, suggestion in scored:
        if len(selected) >= limit:
            break
        if suggestion["permalink"] in used_urls:
            continue
        selected.append(suggestion)
        used_urls.add(suggestion["permalink"])

    return selected
