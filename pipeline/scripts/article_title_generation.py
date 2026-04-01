from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from normalize_keyword_phrase import normalize_text as normalize_phrase_text
from normalize_keyword_phrase import normalize_title, title_case_keyword


ARTICLE_METADATA_DIR = Path(__file__).resolve().parents[2] / "_data" / "article_metadata"
CURRENT_YEAR_PATTERN = re.compile(r"\b20\d{2}\b")
OVERUSED_OPENINGS = {"how to", "best", "complete guide", "ultimate guide"}


def tokenize(value: Any) -> set[str]:
    text = normalize_phrase_text(value)
    return {token for token in re.split(r"[^a-z0-9]+", text) if token}


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def normalize_spacing(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def polish_title_case(title: str) -> str:
    cleaned = normalize_spacing(title)
    if not cleaned:
        return cleaned
    return re.sub(r"(:\s+)([a-z])", lambda match: match.group(1) + match.group(2).upper(), cleaned)


def infer_title_family_from_text(title: str) -> str:
    normalized = normalize_phrase_text(title)
    if normalized.startswith("how to "):
        return "direct_guide"
    if normalized.startswith("best "):
        return "comparison"
    if normalized.startswith("what ") or normalized.endswith("?"):
        return "question_led"
    if "mistake" in normalized:
        return "problem_mistake_led"
    if "beginner" in normalized:
        return "beginner_friendly"
    if "guide" in normalized:
        return "framework_process"
    if ":" in title:
        return "insight_led"
    return "editorial_analysis"


def infer_opening_signature(title: str) -> str:
    normalized = normalize_phrase_text(title)
    if normalized.startswith("how to "):
        return "how to"
    if normalized.startswith("best "):
        return "best"
    if normalized.startswith("complete guide"):
        return "complete guide"
    if normalized.startswith("ultimate guide"):
        return "ultimate guide"
    if normalized.startswith("what "):
        return "what"
    if normalized.startswith("why "):
        return "why"
    if normalized.startswith("a "):
        return "a"
    first_two = normalized.split()[:2]
    return " ".join(first_two)


def build_structure_signature(title: str) -> str:
    normalized = normalize_phrase_text(title)
    normalized = CURRENT_YEAR_PATTERN.sub("<year>", normalized)
    normalized = re.sub(r"\b[a-z]{4,}\b", "x", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def load_existing_title_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not ARTICLE_METADATA_DIR.exists():
        return rows

    for path in sorted(ARTICLE_METADATA_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        rows.append(
            {
                "title": str(payload.get("title") or "").strip(),
                "seo_title": str(payload.get("seo_title") or "").strip(),
                "cluster_id": str(payload.get("cluster_id") or "").strip(),
            }
        )
    return rows


def build_title_context(
    *,
    primary_keyword: str,
    angle_id: str,
    intent_id: str,
    cluster_name: str,
    subtopic_name: str,
    cluster_id: str,
) -> dict[str, str]:
    keyword = normalize_spacing(primary_keyword)
    cluster = normalize_spacing(cluster_name)
    subtopic = normalize_spacing(subtopic_name)
    angle = normalize_spacing(angle_id).replace(" ", "_")
    intent = normalize_spacing(intent_id).replace(" ", "_")

    lower_keyword = normalize_phrase_text(keyword)
    lower_cluster = normalize_phrase_text(cluster)
    lower_subtopic = normalize_phrase_text(subtopic)

    if lower_keyword.startswith("how to "):
        core_subject = normalize_spacing(keyword[7:])
    elif lower_keyword.startswith("best "):
        core_subject = normalize_spacing(keyword[5:])
    else:
        core_subject = keyword

    replacements = [
        (" ideas", ""),
        (" mistakes to avoid", ""),
        (" mistake", ""),
        (" complete guide", ""),
        (" guide", ""),
    ]
    core_lower = normalize_phrase_text(core_subject)
    for suffix, replacement in replacements:
        if core_lower.endswith(suffix):
            core_subject = normalize_spacing(core_subject[: -len(suffix)] + replacement)
            core_lower = normalize_phrase_text(core_subject)

    if " for " in core_lower:
        topic_tail = core_subject.split(" for ", 1)[1]
    else:
        topic_tail = core_subject

    return {
        "primary_keyword": keyword,
        "angle_id": angle,
        "intent_id": intent,
        "cluster_name": cluster,
        "subtopic_name": subtopic,
        "cluster_id": cluster_id,
        "core_subject": title_case_keyword(core_subject or keyword),
        "topic_tail": title_case_keyword(topic_tail or core_subject or keyword),
        "cluster_phrase": title_case_keyword(cluster or topic_tail or keyword),
        "subtopic_phrase": title_case_keyword(subtopic or ""),
    }


def make_candidate(title: str, family: str, source: str) -> dict[str, Any]:
    cleaned = normalize_spacing(title).strip(" .")
    return {
        "title": cleaned,
        "family": family,
        "source": source,
    }


def generate_title_candidates(
    *,
    current_title: str,
    primary_keyword: str,
    angle_id: str,
    intent_id: str,
    cluster_name: str,
    subtopic_name: str,
    cluster_id: str,
) -> list[dict[str, Any]]:
    context = build_title_context(
        primary_keyword=primary_keyword,
        angle_id=angle_id,
        intent_id=intent_id,
        cluster_name=cluster_name,
        subtopic_name=subtopic_name,
        cluster_id=cluster_id,
    )
    pk_title = normalize_title("", primary_keyword=primary_keyword, angle=angle_id)
    model_title = normalize_title(current_title, primary_keyword=primary_keyword, angle=angle_id)
    core_subject = context["core_subject"]
    topic_tail = context["topic_tail"]
    cluster_phrase = context["cluster_phrase"]

    candidates: list[dict[str, Any]] = []
    if model_title:
        candidates.append(make_candidate(model_title, infer_title_family_from_text(model_title), "model"))
    if pk_title:
        candidates.append(make_candidate(pk_title, infer_title_family_from_text(pk_title), "keyword"))

    angle = context["angle_id"]
    if angle == "how_to":
        candidates.extend(
            [
                make_candidate(f"How to {core_subject}", "direct_guide", "rules"),
                make_candidate(f"How to {core_subject}: A Clear Starting Point", "beginner_friendly", "rules"),
                make_candidate(f"{cluster_phrase}: How to Get It Right", "framework_process", "rules"),
                make_candidate(f"What Makes {cluster_phrase} Work?", "question_led", "rules"),
                make_candidate(f"{cluster_phrase}: The Details That Matter Most", "insight_led", "rules"),
            ]
        )
    elif angle == "best_options":
        candidates.extend(
            [
                make_candidate(f"{pk_title}: What to Look For", "comparison", "rules"),
                make_candidate(f"How to Choose {core_subject}", "framework_process", "rules"),
                make_candidate(f"{cluster_phrase}: What Actually Matters", "insight_led", "rules"),
                make_candidate(f"{core_subject}: A Practical Buying Guide", "beginner_friendly", "rules"),
                make_candidate(f"Where to Start with {core_subject}", "outcome_first", "rules"),
            ]
        )
    elif angle == "mistakes":
        candidates.extend(
            [
                make_candidate(pk_title, "problem_mistake_led", "rules"),
                make_candidate(f"{cluster_phrase}: The Mistakes That Throw It Off", "problem_mistake_led", "rules"),
                make_candidate(f"What Goes Wrong with {cluster_phrase}?", "question_led", "rules"),
                make_candidate(f"{cluster_phrase}: The Fixes That Make the Difference", "framework_process", "rules"),
                make_candidate(f"{cluster_phrase}: Common Mistakes to Avoid", "direct_guide", "rules"),
            ]
        )
    elif angle == "ideas":
        candidates.extend(
            [
                make_candidate(pk_title, "direct_guide", "rules"),
                make_candidate(f"{cluster_phrase}: Ideas Worth Starting With", "insight_led", "rules"),
                make_candidate(f"{cluster_phrase}: Fresh Ways to Make It Feel More Pulled Together", "editorial_analysis", "rules"),
                make_candidate(f"What Actually Looks Good in a {topic_tail}?", "question_led", "rules"),
                make_candidate(f"{cluster_phrase}: Where to Start", "beginner_friendly", "rules"),
            ]
        )
    else:
        candidates.extend(
            [
                make_candidate(pk_title, "direct_guide", "rules"),
                make_candidate(f"{cluster_phrase}: What to Know Before You Start", "framework_process", "rules"),
                make_candidate(f"{cluster_phrase}: The Choices That Matter Most", "insight_led", "rules"),
                make_candidate(f"What Makes {cluster_phrase} Work?", "question_led", "rules"),
                make_candidate(f"{cluster_phrase}: A Clear Practical Guide", "beginner_friendly", "rules"),
            ]
        )

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        title = normalize_spacing(candidate["title"])
        if not title:
            continue
        key = normalize_phrase_text(title)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def score_title_candidate(
    *,
    candidate: dict[str, Any],
    primary_keyword: str,
    angle_id: str,
    cluster_id: str,
    existing_rows: list[dict[str, str]],
) -> dict[str, Any]:
    title = candidate["title"]
    title_tokens = tokenize(title)
    primary_tokens = tokenize(primary_keyword)
    normalized_title = normalize_phrase_text(title)
    family = candidate["family"]
    opening = infer_opening_signature(title)
    structure = build_structure_signature(title)

    score = 100.0
    seo_score = 100.0
    notes: list[str] = []

    length = len(title)
    if length < 34:
        score -= 18
        seo_score -= 14
        notes.append("too short")
    elif length > 72:
        score -= 12
        seo_score -= 18
        notes.append("truncation risk")
    elif length > 62:
        score -= 3
        seo_score -= 6
        notes.append("slightly long")
    else:
        score += 4
        seo_score += 6

    overlap = jaccard_similarity(title_tokens, primary_tokens)
    score += overlap * 18
    seo_score += overlap * 26
    if overlap < 0.32:
        score -= 18
        seo_score -= 24
        notes.append("weak keyword/topic coverage")

    if CURRENT_YEAR_PATTERN.search(normalized_title):
        score -= 18
        seo_score -= 18
        notes.append("unnecessary year token")

    if any(phrase in normalized_title for phrase in ["ultimate guide", "complete guide", "you'll love", "in 2026"]):
        score -= 18
        seo_score -= 16
        notes.append("formulaic phrase")

    if title.count(":") > 1:
        score -= 10
        seo_score -= 10
        notes.append("over-structured punctuation")

    if re.search(r"\b(\w+)\s+\1\b", normalized_title):
        score -= 16
        seo_score -= 16
        notes.append("duplicate word")

    if opening in OVERUSED_OPENINGS:
        score -= 6
        seo_score -= 4
        notes.append("templated opening")

    family_count = 0
    opening_count = 0
    structure_count = 0
    max_similarity = 0.0
    cluster_structure_count = 0

    for row in existing_rows:
        existing_title = row.get("title") or row.get("seo_title") or ""
        if not existing_title:
            continue
        if infer_title_family_from_text(existing_title) == family:
            family_count += 1
        if infer_opening_signature(existing_title) == opening:
            opening_count += 1
        if build_structure_signature(existing_title) == structure:
            structure_count += 1
            if row.get("cluster_id") == cluster_id and cluster_id:
                cluster_structure_count += 1
        similarity = jaccard_similarity(title_tokens, tokenize(existing_title))
        max_similarity = max(max_similarity, similarity)

    if family_count >= 8:
        score -= min(12, family_count - 7)
        notes.append("family overused site-wide")
    if opening_count >= 8:
        score -= min(12, opening_count - 7)
        seo_score -= min(8, opening_count - 7)
        notes.append("opening overused site-wide")
    if structure_count >= 2:
        score -= 16
        seo_score -= 12
        notes.append("repeated title skeleton")
    if cluster_structure_count >= 1:
        score -= 18
        seo_score -= 14
        notes.append("cluster title skeleton already used")
    if max_similarity >= 0.72:
        score -= 22
        seo_score -= 18
        notes.append("too similar to existing title")
    elif max_similarity >= 0.58:
        score -= 10
        seo_score -= 8
        notes.append("close to existing title")

    if family in {"insight_led", "editorial_analysis", "question_led"}:
        score += 6
    if family in {"direct_guide", "comparison", "framework_process"}:
        seo_score += 4

    if angle_id == "best_options" and family in {"comparison", "framework_process", "beginner_friendly"}:
        score += 7
        seo_score += 8
    elif angle_id == "how_to" and family in {"direct_guide", "framework_process", "beginner_friendly"}:
        score += 7
        seo_score += 8
    elif angle_id == "mistakes" and family in {"problem_mistake_led", "question_led"}:
        score += 7
        seo_score += 5
    elif angle_id == "ideas" and family in {"editorial_analysis", "insight_led", "direct_guide"}:
        score += 7
        seo_score += 4

    return {
        **candidate,
        "score": round(score, 2),
        "seo_score": round(seo_score, 2),
        "opening": opening,
        "max_similarity": round(max_similarity, 3),
        "notes": notes,
    }


def choose_title_set(
    *,
    current_title: str,
    primary_keyword: str,
    angle_id: str,
    intent_id: str,
    cluster_name: str,
    subtopic_name: str,
    cluster_id: str,
) -> dict[str, Any]:
    existing_rows = load_existing_title_rows()
    candidates = generate_title_candidates(
        current_title=current_title,
        primary_keyword=primary_keyword,
        angle_id=angle_id,
        intent_id=intent_id,
        cluster_name=cluster_name,
        subtopic_name=subtopic_name,
        cluster_id=cluster_id,
    )
    scored = [
        score_title_candidate(
            candidate=candidate,
            primary_keyword=primary_keyword,
            angle_id=angle_id,
            cluster_id=cluster_id,
            existing_rows=existing_rows,
        )
        for candidate in candidates
    ]
    scored.sort(key=lambda item: (-float(item["score"]), -float(item["seo_score"]), item["title"]))
    display = scored[0] if scored else {"title": normalize_title(current_title, primary_keyword=primary_keyword, angle=angle_id)}
    if scored:
        top_score = float(display["score"])
        preferred_display = display
        for candidate in scored[1:]:
            if top_score - float(candidate["score"]) > 8:
                break
            if infer_opening_signature(display["title"]) in OVERUSED_OPENINGS and infer_opening_signature(candidate["title"]) not in OVERUSED_OPENINGS:
                preferred_display = candidate
                break
            if display["family"] in {"direct_guide", "comparison"} and candidate["family"] in {"insight_led", "editorial_analysis", "framework_process", "outcome_first"}:
                preferred_display = candidate
                break
        display = preferred_display

    seo_ranked = sorted(scored, key=lambda item: (-float(item["seo_score"]), -float(item["score"]), item["title"]))
    seo_pick = display
    for candidate in seo_ranked:
        if len(candidate["title"]) <= 65:
            seo_pick = candidate
            break
    if not seo_pick:
        seo_pick = display

    display_title = polish_title_case(normalize_title(display["title"], primary_keyword=primary_keyword, angle=angle_id))
    seo_title = polish_title_case(normalize_title(seo_pick["title"], primary_keyword=primary_keyword, angle=angle_id))

    return {
        "display_title": display_title,
        "seo_title": seo_title,
        "title_family": display.get("family", infer_title_family_from_text(display_title)),
        "seo_title_family": seo_pick.get("family", infer_title_family_from_text(seo_title)),
        "candidates": scored,
    }


def evaluate_title_set(
    *,
    display_title: str,
    seo_title: str,
    title_candidates: list[dict[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    if not display_title:
        warnings.append("Display title is empty.")
    if not seo_title:
        warnings.append("SEO title is empty.")
    if len(seo_title) > 68:
        warnings.append("SEO title is long and may truncate in search results.")
    if infer_opening_signature(display_title) in OVERUSED_OPENINGS:
        warnings.append("Display title still uses an overused opening pattern.")
    if title_candidates:
        top_notes = title_candidates[0].get("notes", [])
        if any(note in {"repeated title skeleton", "too similar to existing title"} for note in top_notes):
            warnings.append("Chosen title is still close to an existing site title pattern.")
    return sorted(dict.fromkeys(warnings))
