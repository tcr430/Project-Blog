from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RULES_PATH = Path(__file__).resolve().parents[1] / "data" / "editorial_mix_rules.json"
DEFAULT_CATEGORY = "evergreen_authority"


def load_editorial_mix_rules() -> dict[str, Any]:
    raw = RULES_PATH.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("editorial_mix_rules.json must contain an object.")
    return data


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


def classify_editorial_mix(
    item: dict[str, Any],
    *,
    cluster_health: str = "",
    pinterest_signal_label: str = "",
    pinterest_intelligence_label: str = "",
) -> dict[str, Any]:
    tags: list[str] = []
    season = str(item.get("season") or "").strip()
    holiday = str(item.get("holiday") or "").strip()
    source = normalize_identifier(item.get("source") or "")
    intent_id = normalize_identifier(item.get("intent_id") or "")
    angle_id = normalize_identifier(item.get("angle_id") or "")
    trend_score = int(item.get("pinterest_trend_score") or 0)

    if season or holiday:
        tags.append("seasonal_opportunity")

    if source == "pinterest_trends_api" or trend_score > 0 or pinterest_signal_label in {"hot", "warm"} or pinterest_intelligence_label in {"hot", "warm"}:
        tags.append("pinterest_responsive")

    if intent_id in {"comparison", "decision_making"} or angle_id in {"best_options", "budget"}:
        tags.append("monetizable_decision")

    if (
        not tags
        or cluster_health in {"missing", "underdeveloped", "growing"}
        or intent_id in {"implementation", "problem_solving", "inspiration"}
    ):
        tags.append("evergreen_authority")

    ordered_unique_tags: list[str] = []
    for tag in tags:
        if tag not in ordered_unique_tags:
            ordered_unique_tags.append(tag)

    if "seasonal_opportunity" in ordered_unique_tags:
        primary = "seasonal_opportunity"
    elif "monetizable_decision" in ordered_unique_tags:
        primary = "monetizable_decision"
    elif "pinterest_responsive" in ordered_unique_tags:
        primary = "pinterest_responsive"
    else:
        primary = DEFAULT_CATEGORY

    return {
        "primary": primary,
        "tags": ordered_unique_tags,
    }


def build_recent_editorial_mix_state(index_data: dict[str, Any], rules: dict[str, Any]) -> dict[str, Any]:
    categories = dict(rules.get("categories") or {})
    window_size = max(1, int(rules.get("recent_window_article_count") or 8))
    articles = [item for item in index_data.get("articles", []) if isinstance(item, dict)]
    articles.sort(
        key=lambda item: parse_iso_date(item.get("published_at") or item.get("publish_date") or "") or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    recent_articles = articles[:window_size]

    counts = {category_id: 0 for category_id in categories}
    for article in recent_articles:
        mix = classify_editorial_mix(article)
        counts[mix["primary"]] = counts.get(mix["primary"], 0) + 1

    total = len(recent_articles)
    shares = {
        category_id: (counts.get(category_id, 0) / total if total else 0.0)
        for category_id in categories
    }
    underrepresented = [
        category_id
        for category_id, config in categories.items()
        if shares.get(category_id, 0.0) < float(config.get("soft_min_share") or 0.0)
    ]
    overrepresented = [
        category_id
        for category_id, config in categories.items()
        if total and shares.get(category_id, 0.0) > float(config.get("soft_max_share") or 1.0)
    ]

    return {
        "window_article_count": total,
        "counts": counts,
        "shares": shares,
        "underrepresented": underrepresented,
        "overrepresented": overrepresented,
    }


def editorial_mix_adjustment(
    primary_category: str,
    *,
    rules: dict[str, Any],
    mix_state: dict[str, Any],
    stage: str,
    strong_opportunity: bool = False,
) -> tuple[int, str]:
    categories = dict(rules.get("categories") or {})
    category = categories.get(primary_category, categories.get(DEFAULT_CATEGORY, {}))
    share = float((mix_state.get("shares") or {}).get(primary_category, 0.0))
    total = int(mix_state.get("window_article_count") or 0)

    boost_key = "planning_boost" if stage == "planning" else "selection_boost"
    if total == 0:
        adjustment = int(category.get(boost_key) or 0)
        return adjustment, f"editorial mix: no recent baseline, {primary_category} gets startup support ({adjustment:+d})"

    min_share = float(category.get("soft_min_share") or 0.0)
    max_share = float(category.get("soft_max_share") or 1.0)

    if share < min_share:
        adjustment = int(category.get(boost_key) or 0)
        return adjustment, f"editorial mix: {primary_category} is underrepresented in recent publishing ({adjustment:+d})"

    if share > max_share and not strong_opportunity:
        adjustment = int(category.get("over_target_penalty") or 0)
        return adjustment, f"editorial mix: {primary_category} is overrepresented recently ({adjustment:+d})"

    return 0, ""


def round_selection_mix_bonus(
    primary_category: str,
    *,
    selected_counts: dict[str, int],
    selected_total: int,
    rules: dict[str, Any],
) -> tuple[int, str]:
    categories = dict(rules.get("categories") or {})
    category = categories.get(primary_category, categories.get(DEFAULT_CATEGORY, {}))
    if selected_total <= 0:
        return 0, ""

    target_share = float(category.get("target_share") or 0.0)
    current_share = selected_counts.get(primary_category, 0) / selected_total if selected_total else 0.0
    if current_share < target_share:
        bonus = max(2, int(category.get("selection_boost") or 0) // 2)
        return bonus, f"editorial mix: selection round still needs more {primary_category.replace('_', ' ')} ({bonus:+d})"

    if current_share > float(category.get("soft_max_share") or 1.0):
        penalty = min(-2, int(category.get("over_target_penalty") or -2))
        return penalty, f"editorial mix: selection round already leans heavily on {primary_category.replace('_', ' ')} ({penalty:+d})"

    return 0, ""
