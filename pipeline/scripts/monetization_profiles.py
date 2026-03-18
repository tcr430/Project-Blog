from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "monetization_profiles.json"


def _normalize_identifier(value: str) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


@lru_cache(maxsize=1)
def load_monetization_profile_data() -> dict[str, Any]:
    raw = DATA_PATH.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("monetization_profiles.json must contain an object.")
    return data


def resolve_monetization_profile(
    angle_id: str = "",
    intent_id: str = "",
    value: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(value, dict) and value.get("profile_id"):
        return _normalize_profile(value)

    data = load_monetization_profile_data()
    profiles = data.get("profiles", {})
    default_profile_id = str(data.get("default_profile_id") or "editorial_soft")
    normalized_angle = _normalize_identifier(angle_id)
    normalized_intent = _normalize_identifier(intent_id)

    profile_id = str(data.get("angle_overrides", {}).get(normalized_angle) or "").strip()
    if not profile_id:
        profile_id = str(data.get("intent_overrides", {}).get(normalized_intent) or "").strip()
    if not profile_id or profile_id not in profiles:
        profile_id = default_profile_id

    profile = profiles.get(profile_id) or profiles.get(default_profile_id) or {}
    return _normalize_profile(profile)


def _normalize_profile(profile: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "profile_id": str(profile.get("profile_id") or "editorial_soft").strip(),
        "name": str(profile.get("name") or "Editorial Soft").strip(),
        "description": str(profile.get("description") or "").strip(),
        "min_products_to_enable": max(0, int(profile.get("min_products_to_enable") or 0)),
        "max_products": max(0, int(profile.get("max_products") or 0)),
        "max_inline_links": max(0, int(profile.get("max_inline_links") or 0)),
        "preferred_section_indexes": [
            index
            for index in [int(item) for item in profile.get("preferred_section_indexes", []) if str(item).strip()]
            if 1 <= index <= 5
        ],
        "shop_block_enabled": bool(profile.get("shop_block_enabled", False)),
        "shop_block_kicker": str(profile.get("shop_block_kicker") or "Shop the Look").strip(),
        "shop_block_heading": str(profile.get("shop_block_heading") or "Bring the Look Home").strip(),
        "shop_button_label": str(profile.get("shop_button_label") or "View Product").strip(),
        "prompt_guidance": str(profile.get("prompt_guidance") or "").strip(),
        "inline_sentence_templates": [
            str(item).strip()
            for item in profile.get("inline_sentence_templates", [])
            if str(item).strip()
        ],
    }
    if not normalized["preferred_section_indexes"]:
        normalized["preferred_section_indexes"] = [3, 5]
    if not normalized["inline_sentence_templates"]:
        normalized["inline_sentence_templates"] = [
            "A practical option here is [{title}]({affiliate_url})."
        ]
    return normalized


def resolve_affiliate_section_indexes(profile: dict[str, Any], product_count: int) -> list[int]:
    if product_count <= 0:
        return []

    preferred = [index for index in profile.get("preferred_section_indexes", []) if 1 <= index <= 5]
    section_indexes: list[int] = []
    for index in preferred:
        if len(section_indexes) >= product_count:
            break
        if index not in section_indexes:
            section_indexes.append(index)

    for index in range(1, 6):
        if len(section_indexes) >= product_count:
            break
        if index not in section_indexes:
            section_indexes.append(index)

    return section_indexes


def limit_products_for_profile(products: list[dict[str, Any]], profile: dict[str, Any]) -> list[dict[str, Any]]:
    max_products = max(0, int(profile.get("max_products") or 0))
    if max_products <= 0:
        return []
    return products[:max_products]
