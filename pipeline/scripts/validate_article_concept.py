from __future__ import annotations

from functools import lru_cache
from typing import Any, TypedDict

from content_architecture import load_content_clusters, load_content_constraints
from topic_clusters import normalize_text


class ConceptValidationError(ValueError):
    """Raised when a structured article concept fails rule-based validation."""


class CompatibilityDiagnostic(TypedDict):
    rule_id: str
    classification: str
    message: str
    source: str


class ConceptValidationResult(TypedDict):
    compatibility_class: str
    hard_errors: list[str]
    warnings: list[str]
    diagnostics: list[CompatibilityDiagnostic]
    ontology: dict[str, Any]


SEVERITY_RANK = {
    "valid": 0,
    "valid_with_constraints": 1,
    "soft_warn": 2,
    "invalid": 3,
}

KNOWN_ROOM_NAMES = {
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
    "nook",
}


def _normalize_id(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


@lru_cache(maxsize=1)
def _get_constraints() -> dict[str, Any]:
    constraints = load_content_constraints()
    return constraints if isinstance(constraints, dict) else {}


@lru_cache(maxsize=1)
def _build_cluster_map() -> dict[str, dict[str, Any]]:
    return {
        str(cluster.get("cluster_id") or "").strip(): cluster
        for cluster in load_content_clusters()
        if str(cluster.get("cluster_id") or "").strip()
    }


@lru_cache(maxsize=None)
def _build_subtopic_map(cluster_id: str) -> dict[str, dict[str, Any]]:
    cluster = _build_cluster_map().get(cluster_id, {})
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

    subtopic_map = _build_subtopic_map(cluster_id)
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


def _get_compatibility_model(constraints: dict[str, Any]) -> dict[str, Any]:
    payload = constraints.get("compatibility_model", {})
    return payload if isinstance(payload, dict) else {}


@lru_cache(maxsize=1)
def _get_cached_compatibility_model() -> dict[str, Any]:
    return _get_compatibility_model(_get_constraints())


def _build_haystack(topic_context: dict[str, Any]) -> str:
    return normalize_text(
        " ".join(
            [
                str(topic_context.get("trend_cluster") or ""),
                str(topic_context.get("primary_keyword") or ""),
                str(topic_context.get("subtopic_name") or ""),
                str(topic_context.get("modifier") or ""),
                " ".join(str(item) for item in topic_context.get("secondary_keywords", []) if str(item).strip()),
            ]
        )
    )


def _extract_space_hits(haystack: str, compatibility_model: dict[str, Any]) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for item in compatibility_model.get("space_taxonomy", []):
        if not isinstance(item, dict):
            continue
        match_terms = [normalize_text(term) for term in item.get("match_terms", []) if normalize_text(term)]
        if not match_terms:
            continue
        matched_terms = [term for term in match_terms if term in haystack]
        if not matched_terms:
            continue
        hits.append(
            {
                "space_id": _normalize_id(item.get("space_id", "")),
                "category": normalize_text(item.get("category", "")) or "space_context",
                "tags": sorted(
                    {
                        _normalize_id(tag)
                        for tag in item.get("tags", [])
                        if _normalize_id(tag)
                    }
                ),
                "matched_terms": matched_terms,
            }
        )
    return hits


def _extract_feature_hits(haystack: str, compatibility_model: dict[str, Any]) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for item in compatibility_model.get("feature_taxonomy", []):
        if not isinstance(item, dict):
            continue
        match_terms = [normalize_text(term) for term in item.get("match_terms", []) if normalize_text(term)]
        if not match_terms:
            continue
        matched_terms = [term for term in match_terms if term in haystack]
        if not matched_terms:
            continue
        hits.append(
            {
                "feature_id": _normalize_id(item.get("feature_id", "")),
                "feature_family": _normalize_id(item.get("feature_family", "")),
                "tags": sorted(
                    {
                        _normalize_id(tag)
                        for tag in item.get("tags", [])
                        if _normalize_id(tag)
                    }
                ),
                "matched_terms": matched_terms,
            }
        )
    return hits


def _extract_ontology(topic_context: dict[str, Any], compatibility_model: dict[str, Any]) -> dict[str, Any]:
    haystack = _build_haystack(topic_context)
    space_hits = _extract_space_hits(haystack, compatibility_model)
    feature_hits = _extract_feature_hits(haystack, compatibility_model)
    return {
        "haystack": haystack,
        "space_ids": sorted({hit["space_id"] for hit in space_hits if hit["space_id"]}),
        "space_categories": sorted({hit["category"] for hit in space_hits if hit["category"]}),
        "space_tags": sorted({tag for hit in space_hits for tag in hit["tags"]}),
        "feature_ids": sorted({hit["feature_id"] for hit in feature_hits if hit["feature_id"]}),
        "feature_families": sorted({hit["feature_family"] for hit in feature_hits if hit["feature_family"]}),
        "feature_tags": sorted({tag for hit in feature_hits for tag in hit["tags"]}),
        "space_hits": space_hits,
        "feature_hits": feature_hits,
        "modifier": normalize_text(topic_context.get("modifier") or ""),
    }


def _rule_matches(rule: dict[str, Any], ontology: dict[str, Any]) -> bool:
    checks = [
        ("space_ids_any", set(ontology["space_ids"])),
        ("space_categories_any", set(ontology["space_categories"])),
        ("space_tags_any", set(ontology["space_tags"])),
        ("feature_ids_any", set(ontology["feature_ids"])),
        ("feature_family_any", set(ontology["feature_families"])),
        ("feature_tags_any", set(ontology["feature_tags"])),
        ("modifiers_any", {normalize_text(ontology["modifier"])} if ontology["modifier"] else set()),
    ]
    for rule_key, available in checks:
        required = {
            _normalize_id(item)
            for item in rule.get(rule_key, [])
            if _normalize_id(item)
        }
        if required and not (required & available):
            return False
    return True


def _match_rules(
    rules: list[dict[str, Any]],
    *,
    ontology: dict[str, Any],
    source: str,
) -> list[CompatibilityDiagnostic]:
    diagnostics: list[CompatibilityDiagnostic] = []
    for item in rules:
        if not isinstance(item, dict):
            continue
        if not _rule_matches(item, ontology):
            continue
        classification = _normalize_id(item.get("classification") or "")
        if classification not in SEVERITY_RANK:
            classification = "soft_warn"
        diagnostics.append(
            {
                "rule_id": str(item.get("rule_id") or source).strip() or source,
                "classification": classification,
                "message": str(item.get("reason") or item.get("message") or "Compatibility rule matched.").strip(),
                "source": source,
            }
        )
    return diagnostics


def _evaluate_compatibility_model(topic_context: dict[str, Any]) -> tuple[str, list[str], list[CompatibilityDiagnostic], dict[str, Any]]:
    compatibility_model = _get_cached_compatibility_model()
    ontology = _extract_ontology(topic_context, compatibility_model)

    diagnostics: list[CompatibilityDiagnostic] = []
    diagnostics.extend(
        _match_rules(
            compatibility_model.get("hard_invalid_rules", []),
            ontology=ontology,
            source="compatibility_model.hard_invalid_rules",
        )
    )
    if any(item["classification"] == "invalid" for item in diagnostics):
        errors = [item["message"] for item in diagnostics if item["classification"] == "invalid"]
        return "invalid", errors, diagnostics, ontology

    diagnostics.extend(
        _match_rules(
            compatibility_model.get("contextual_rules", []),
            ontology=ontology,
            source="compatibility_model.contextual_rules",
        )
    )

    classification = "valid"
    warnings: list[str] = []
    for item in diagnostics:
        if item["classification"] == "valid_with_constraints":
            warnings.append(item["message"])
        elif item["classification"] == "soft_warn":
            warnings.append(item["message"])
        if SEVERITY_RANK[item["classification"]] > SEVERITY_RANK[classification]:
            classification = item["classification"]

    return classification, list(dict.fromkeys(warnings)), diagnostics, ontology


def _evaluate_legacy_constraints(
    topic_context: dict[str, Any],
    *,
    ontology: dict[str, Any],
) -> list[CompatibilityDiagnostic]:
    constraints = _get_constraints()
    diagnostics: list[CompatibilityDiagnostic] = []
    haystack = ontology.get("haystack") or _build_haystack(topic_context)
    modern_spaces_present = bool(ontology.get("space_ids"))
    modern_features_present = bool(ontology.get("feature_ids") or ontology.get("feature_families"))
    if modern_spaces_present and modern_features_present:
        return diagnostics

    for item in constraints.get("invalid_combinations", []):
        if not isinstance(item, dict):
            continue
        room = normalize_text(item.get("room") or "")
        feature = normalize_text(item.get("feature") or "")
        if room and feature and room in haystack and feature in haystack:
            diagnostics.append(
                {
                    "rule_id": f"legacy_invalid_pair:{room}:{feature}",
                    "classification": "soft_warn",
                    "message": (
                        f"Legacy room/feature rule matched '{feature}' + '{room}'. "
                        "This combination may be context-sensitive rather than strictly invalid."
                    ),
                    "source": "legacy.invalid_combinations",
                }
            )

    room_feature_rules = constraints.get("room_feature_compatibility", {})
    if isinstance(room_feature_rules, dict):
        known_room_hits = [room_name for room_name in KNOWN_ROOM_NAMES if room_name in haystack]
        for feature, allowed_rooms in room_feature_rules.items():
            normalized_feature = normalize_text(feature)
            if not normalized_feature or normalized_feature not in haystack:
                continue
            allowed = {
                normalize_text(room)
                for room in allowed_rooms
                if normalize_text(room)
            } if isinstance(allowed_rooms, list) else set()
            if not allowed:
                continue
            if any(room in allowed for room in known_room_hits):
                continue
            if known_room_hits:
                diagnostics.append(
                    {
                        "rule_id": f"legacy_room_feature:{normalized_feature}",
                        "classification": "soft_warn",
                        "message": (
                            f"Legacy room-feature compatibility marked '{normalized_feature}' as weak for '{known_room_hits[0]}'. "
                            "This now downgrades confidence instead of hard-blocking."
                        ),
                        "source": "legacy.room_feature_compatibility",
                    }
                )
                break

    return diagnostics


def validate_article_concept(topic_context: dict[str, Any]) -> ConceptValidationResult:
    hard_errors: list[str] = []
    _validate_required_structured_fields(topic_context, hard_errors)
    _validate_cluster_subtopic_angle(topic_context, hard_errors)

    classification, messages, diagnostics, ontology = _evaluate_compatibility_model(topic_context)
    warnings = list(messages)
    if classification == "invalid":
        hard_errors.extend(messages)
        warnings = []
    else:
        legacy_diagnostics = _evaluate_legacy_constraints(topic_context, ontology=ontology)
        diagnostics.extend(legacy_diagnostics)
        for item in legacy_diagnostics:
            if SEVERITY_RANK[item["classification"]] > SEVERITY_RANK[classification]:
                classification = item["classification"]
        warnings = list(dict.fromkeys([*warnings, *(item["message"] for item in legacy_diagnostics)]))

    if hard_errors:
        classification = "invalid"

    return {
        "compatibility_class": classification,
        "hard_errors": list(dict.fromkeys(hard_errors)),
        "warnings": list(dict.fromkeys(warnings)),
        "diagnostics": diagnostics,
        "ontology": ontology,
    }


def ensure_valid_article_concept(topic_context: dict[str, Any]) -> ConceptValidationResult:
    result = validate_article_concept(topic_context)
    if result["compatibility_class"] == "invalid":
        messages = [*result["hard_errors"], *(item["message"] for item in result["diagnostics"] if item["classification"] == "invalid")]
        raise ConceptValidationError("; ".join(dict.fromkeys(messages)))
    return result


def filter_valid_article_concepts(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid: list[dict[str, Any]] = []
    for candidate in candidates:
        result = validate_article_concept(candidate)
        trend_label = str(candidate.get("primary_keyword") or candidate.get("trend_keyword") or "").strip()
        compatibility_class = result["compatibility_class"]

        if compatibility_class == "invalid":
            messages = [*result["hard_errors"], *(item["message"] for item in result["diagnostics"] if item["classification"] == "invalid")]
            print(f"[concept][reject][invalid] {trend_label}: {'; '.join(dict.fromkeys(messages))}")
            continue

        candidate["compatibility_class"] = compatibility_class
        candidate["compatibility_warnings"] = result["warnings"]
        candidate["compatibility_diagnostics"] = result["diagnostics"]

        if compatibility_class in {"valid_with_constraints", "soft_warn"} and result["warnings"]:
            print(
                f"[concept][warn][{compatibility_class}] {trend_label}: "
                f"{'; '.join(result['warnings'])}"
            )

        valid.append(candidate)
    return valid
