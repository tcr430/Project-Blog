from __future__ import annotations

from typing import Any

from content_architecture import load_content_clusters, load_content_constraints
from topic_clusters import normalize_text


class ConceptValidationError(ValueError):
    """Raised when a structured article concept fails rule-based validation."""


def _normalize_id(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


def _build_cluster_map() -> dict[str, dict[str, Any]]:
    return {
        str(cluster.get("cluster_id") or "").strip(): cluster
        for cluster in load_content_clusters()
        if str(cluster.get("cluster_id") or "").strip()
    }


def _build_subtopic_map(cluster: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(subtopic.get("subtopic_id") or "").strip(): subtopic
        for subtopic in cluster.get("subtopics", [])
        if isinstance(subtopic, dict) and str(subtopic.get("subtopic_id") or "").strip()
    }


def _resolve_structured_fields(topic_context: dict[str, Any]) -> tuple[str, str, str]:
    cluster_id = str(topic_context.get("cluster_id") or "").strip()
    subtopic_id = str(topic_context.get("subtopic_id") or "").strip()
    angle_id = _normalize_id(topic_context.get("angle_id") or "")
    return cluster_id, subtopic_id, angle_id


def _validate_required_structured_fields(topic_context: dict[str, Any], errors: list[str]) -> None:
    cluster_id, subtopic_id, angle_id = _resolve_structured_fields(topic_context)
    structured_values = [cluster_id, subtopic_id, angle_id]
    if not any(structured_values):
        return

    missing: list[str] = []
    if not cluster_id:
        missing.append("cluster_id")
    if not subtopic_id:
        missing.append("subtopic_id")
    if not angle_id:
        missing.append("angle_id")
    if missing:
        errors.append("Structured concept is missing required fields: " + ", ".join(missing))


def _validate_cluster_subtopic_angle(topic_context: dict[str, Any], errors: list[str]) -> None:
    cluster_id, subtopic_id, angle_id = _resolve_structured_fields(topic_context)
    if not cluster_id or not subtopic_id or not angle_id:
        return

    cluster_map = _build_cluster_map()
    cluster = cluster_map.get(cluster_id)
    if cluster is None:
        errors.append(f"Unknown cluster_id '{cluster_id}'.")
        return

    subtopic_map = _build_subtopic_map(cluster)
    subtopic = subtopic_map.get(subtopic_id)
    if subtopic is None:
        errors.append(f"Subtopic '{subtopic_id}' does not belong to cluster '{cluster_id}'.")
        return

    allowed_angles = {
        _normalize_id(item)
        for item in subtopic.get("allowed_angle_ids", [])
        if _normalize_id(item)
    }
    if allowed_angles and angle_id not in allowed_angles:
        errors.append(
            f"Angle '{angle_id}' is not allowed for subtopic '{subtopic_id}' in cluster '{cluster_id}'."
        )

    modifier = normalize_text(topic_context.get("modifier") or "")
    if modifier:
        cluster_modifiers = {
            normalize_text(item)
            for item in cluster.get("modifiers", [])
            if normalize_text(item)
        }
        subtopic_modifiers = {
            normalize_text(item)
            for item in subtopic.get("modifiers", [])
            if normalize_text(item)
        }
        allowed_modifiers = subtopic_modifiers or cluster_modifiers
        if allowed_modifiers and modifier not in allowed_modifiers:
            errors.append(
                f"Modifier '{modifier}' is not allowed for subtopic '{subtopic_id}' in cluster '{cluster_id}'."
            )


def _validate_constraints(topic_context: dict[str, Any], errors: list[str]) -> None:
    constraints = load_content_constraints()
    haystack = " ".join(
        [
            str(topic_context.get("trend_cluster") or ""),
            str(topic_context.get("primary_keyword") or ""),
            str(topic_context.get("subtopic_name") or ""),
            str(topic_context.get("modifier") or ""),
            " ".join(str(item) for item in topic_context.get("secondary_keywords", []) if str(item).strip()),
        ]
    )
    normalized_haystack = normalize_text(haystack)

    for item in constraints.get("invalid_combinations", []):
        if not isinstance(item, dict):
            continue
        room = normalize_text(item.get("room") or "")
        feature = normalize_text(item.get("feature") or "")
        if room and feature and room in normalized_haystack and feature in normalized_haystack:
            errors.append(
                f"Concept matches invalid room/feature combination '{feature}' + '{room}'."
            )
            break

    room_feature_rules = constraints.get("room_feature_compatibility", {})
    if isinstance(room_feature_rules, dict):
        for feature, allowed_rooms in room_feature_rules.items():
            normalized_feature = normalize_text(feature)
            if not normalized_feature or normalized_feature not in normalized_haystack:
                continue
            allowed = {
                normalize_text(room)
                for room in allowed_rooms
                if normalize_text(room)
            } if isinstance(allowed_rooms, list) else set()
            room_hits = [room for room in allowed if room in normalized_haystack]
            if room_hits:
                continue
            known_room_hits = [
                room_name
                for room_name in {
                    "living room",
                    "family room",
                    "bedroom",
                    "guest bedroom",
                    "nursery",
                    "kitchen",
                    "dining room",
                    "entryway",
                    "bathroom",
                    "laundry room",
                    "home office",
                    "small apartment",
                    "studio apartment",
                    "patio",
                    "balcony",
                }
                if room_name in normalized_haystack
            ]
            if known_room_hits and not any(room in allowed for room in known_room_hits):
                errors.append(
                    f"Feature '{normalized_feature}' is not compatible with room context '{known_room_hits[0]}'."
                )
                break


def validate_article_concept(topic_context: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    _validate_required_structured_fields(topic_context, errors)
    _validate_cluster_subtopic_angle(topic_context, errors)
    _validate_constraints(topic_context, errors)
    return list(dict.fromkeys(errors))


def ensure_valid_article_concept(topic_context: dict[str, Any]) -> None:
    errors = validate_article_concept(topic_context)
    if errors:
        raise ConceptValidationError("; ".join(errors))


def filter_valid_article_concepts(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid: list[dict[str, Any]] = []
    for candidate in candidates:
        errors = validate_article_concept(candidate)
        if errors:
            trend_label = str(candidate.get("primary_keyword") or candidate.get("trend_keyword") or "").strip()
            print(f"[concept][reject] {trend_label}: {'; '.join(errors)}")
            continue
        valid.append(candidate)
    return valid
