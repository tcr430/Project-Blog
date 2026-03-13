from __future__ import annotations

import json
from pathlib import Path
from typing import Any

BASE_VARIANT_TYPES = ["trend_overview", "practical_tips", "product_led", "styling_angle"]
STYLE_OPTIONS = {
    "trend_overview": ["bottom-panel", "top-band"],
    "practical_tips": ["center-card", "top-band"],
    "product_led": ["product-focus", "bottom-panel"],
    "styling_angle": ["top-band", "center-card"],
}
SUMMARY_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_performance_summary.json"
ARTICLE_SCORES_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_article_scores.json"


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return {}

    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return data


def stable_hash(text: str) -> int:
    return sum(ord(char) for char in text)


def metric_score(record: dict[str, Any]) -> float:
    impressions = float(record.get("average_impressions") or 0)
    saves = float(record.get("average_saves") or 0)
    outbound_clicks = float(record.get("average_outbound_clicks") or 0)
    engagement_rate = float(record.get("average_engagement_rate") or 0)
    return round((outbound_clicks * 5.0) + (saves * 3.0) + (engagement_rate * 120.0) + (impressions / 2000.0), 4)


def build_lookup(items: list[dict[str, Any]], key_name: str) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for item in items:
        key = str(item.get(key_name) or "").strip()
        if key:
            lookup[key] = item
    return lookup


def load_strategy_context(
    summary_path: Path = SUMMARY_PATH,
    article_scores_path: Path = ARTICLE_SCORES_PATH,
) -> dict[str, Any]:
    summary = load_json(summary_path)
    article_scores = load_json(article_scores_path)

    return {
        "summary": summary,
        "article_scores": article_scores,
        "variant_lookup": build_lookup(summary.get("performance_by_variant_type", []), "variant_type"),
        "board_lookup": build_lookup(summary.get("performance_by_board", []), "board_key"),
        "article_lookup": build_lookup(article_scores.get("articles", []), "article_slug"),
    }


def candidate_boards_for_variant(
    *,
    variant_type: str,
    topic_board: dict[str, str],
    trend_board: dict[str, str],
    tips_board: dict[str, str],
    product_board: dict[str, str],
    default_board: dict[str, str],
    topic_text: str,
) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []

    if variant_type == "trend_overview":
        if "trend" in topic_text.lower() or "trending" in topic_text.lower():
            candidates.append(trend_board.copy())
        candidates.append(topic_board.copy())
    elif variant_type == "practical_tips":
        candidates.extend([tips_board.copy(), topic_board.copy()])
    elif variant_type == "product_led":
        candidates.extend([product_board.copy(), topic_board.copy()])
    elif variant_type == "styling_angle":
        candidates.extend([topic_board.copy(), tips_board.copy()])

    candidates.append(default_board.copy())

    unique: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.get("key") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def prioritize_board(
    *,
    article_slug: str,
    variant_type: str,
    duplicate_index: int,
    candidate_boards: list[dict[str, str]],
    board_lookup: dict[str, dict[str, Any]],
) -> tuple[dict[str, str], list[str]]:
    scored_candidates: list[tuple[float, dict[str, str]]] = []
    for candidate in candidate_boards:
        board_record = board_lookup.get(candidate["key"], {})
        scored_candidates.append((metric_score(board_record), candidate))

    scored_candidates.sort(key=lambda item: item[0], reverse=True)
    notes: list[str] = []

    if len(scored_candidates) > 1 and stable_hash(f"{article_slug}:{variant_type}:{duplicate_index}") % 5 == 0:
        selected = scored_candidates[1][1]
        notes.append(f"exploring board {selected['key']}")
        return selected, notes

    selected = scored_candidates[0][1]
    if scored_candidates[0][0] > 0:
        notes.append(f"selecting boards based on performance: {selected['key']}")
    return selected, notes


def determine_bonus_slots(article_record: dict[str, Any] | None) -> tuple[int, list[str]]:
    if not article_record:
        return 0, []

    classification = str(article_record.get("classification") or "").strip().lower()
    score = int(article_record.get("score") or 0)
    notes: list[str] = []

    if classification == "strong_candidate":
        notes.append("scheduling high-performing articles with 2 bonus pins")
        return 2, notes
    if classification == "neutral" and score >= 3:
        notes.append("scheduling a moderate article boost with 1 bonus pin")
        return 1, notes
    return 0, notes


def rank_variant_types(variant_lookup: dict[str, dict[str, Any]]) -> list[str]:
    ranked = sorted(
        BASE_VARIANT_TYPES,
        key=lambda variant_type: (
            metric_score(variant_lookup.get(variant_type, {})),
            -BASE_VARIANT_TYPES.index(variant_type),
        ),
        reverse=True,
    )
    return ranked


def build_pin_distribution_strategy(
    *,
    article_slug: str,
    topic_text: str,
    topic_board: dict[str, str],
    trend_board: dict[str, str],
    tips_board: dict[str, str],
    product_board: dict[str, str],
    default_board: dict[str, str],
    summary_path: Path = SUMMARY_PATH,
    article_scores_path: Path = ARTICLE_SCORES_PATH,
) -> dict[str, Any]:
    context = load_strategy_context(summary_path=summary_path, article_scores_path=article_scores_path)
    variant_lookup = context["variant_lookup"]
    board_lookup = context["board_lookup"]
    article_record = context["article_lookup"].get(article_slug)

    ranked_variant_types = rank_variant_types(variant_lookup)
    variant_notes: list[str] = []
    if any(metric_score(variant_lookup.get(variant_type, {})) > 0 for variant_type in BASE_VARIANT_TYPES):
        variant_notes.append(f"prioritizing variant types: {', '.join(ranked_variant_types)}")
    else:
        variant_notes.append("applying performance weighting: no historical lift yet, using balanced defaults")

    bonus_slots, article_notes = determine_bonus_slots(article_record)
    planned_types = list(BASE_VARIANT_TYPES)
    for bonus_index in range(bonus_slots):
        planned_types.append(ranked_variant_types[bonus_index % len(ranked_variant_types)])

    duplicate_counts: dict[str, int] = {}
    plans: list[dict[str, Any]] = []
    board_notes: list[str] = []
    for order_index, variant_type in enumerate(planned_types):
        duplicate_index = duplicate_counts.get(variant_type, 0)
        duplicate_counts[variant_type] = duplicate_index + 1

        candidate_boards = candidate_boards_for_variant(
            variant_type=variant_type,
            topic_board=topic_board,
            trend_board=trend_board,
            tips_board=tips_board,
            product_board=product_board,
            default_board=default_board,
            topic_text=topic_text,
        )
        selected_board, selection_notes = prioritize_board(
            article_slug=article_slug,
            variant_type=variant_type,
            duplicate_index=duplicate_index,
            candidate_boards=candidate_boards,
            board_lookup=board_lookup,
        )
        board_notes.extend(selection_notes)

        style_options = STYLE_OPTIONS.get(variant_type, ["bottom-panel"])
        style_name = style_options[duplicate_index % len(style_options)]
        variant_score = metric_score(variant_lookup.get(variant_type, {}))
        board_score = metric_score(board_lookup.get(selected_board["key"], {}))
        plans.append(
            {
                "variant_type": variant_type,
                "duplicate_index": duplicate_index,
                "style_name": style_name,
                "board": selected_board,
                "priority_score": round((variant_score * 0.7) + (board_score * 0.3), 4),
                "base_order": order_index,
            }
        )

    plans.sort(key=lambda item: (-item["priority_score"], item["base_order"]))

    for index, plan in enumerate(plans):
        plan["schedule_rank"] = index

    return {
        "article_score": article_record,
        "bonus_slots": bonus_slots,
        "ranked_variant_types": ranked_variant_types,
        "plans": plans,
        "notes": variant_notes + article_notes + board_notes,
    }
