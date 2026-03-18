from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any


DATA_DIR = Path(__file__).resolve().parents[1] / "data"
STYLE_PROFILES_PATH = DATA_DIR / "image_style_profiles.json"
SHOT_LIBRARIES_PATH = DATA_DIR / "image_shot_libraries.json"
ARTICLE_METADATA_DIR = Path(__file__).resolve().parents[2] / "_data" / "article_metadata"
RECENT_METADATA_LIMIT = 12
SIMILARITY_WARNING_THRESHOLD = 0.62


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic image prompts from article context.")
    parser.add_argument("title", type=str, help="Article title.")
    parser.add_argument("--cluster", type=str, default="")
    parser.add_argument("--cluster-id", type=str, default="")
    parser.add_argument("--primary-keyword", type=str, default="")
    parser.add_argument("--angle", type=str, default="")
    parser.add_argument("--intent", type=str, default="")
    parser.add_argument("--season", type=str, default="")
    parser.add_argument(
        "--section",
        action="append",
        default=[],
        help="Section heading. Provide up to five --section values.",
    )
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def normalize_text(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", normalized).strip()


def normalize_id(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


def tokenize(value: Any) -> set[str]:
    return {token for token in normalize_text(value).split() if token}


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def validate_sections(sections: list[str]) -> list[str]:
    cleaned = [item.strip() for item in sections if item.strip()]
    if len(cleaned) != 5:
        raise ValueError("Exactly five section headings are required via --section.")
    return cleaned


def infer_room(cluster: str, primary_keyword: str, title: str) -> str:
    haystack = " ".join([cluster, primary_keyword, title]).lower()
    for room in [
        "living room",
        "bedroom",
        "bathroom",
        "kitchen",
        "dining room",
        "entryway",
        "home office",
        "nursery",
        "patio",
        "balcony",
    ]:
        if room in haystack:
            return room
    return "interior"


def load_style_profiles() -> dict[str, Any]:
    payload = load_json(STYLE_PROFILES_PATH, {})
    return payload if isinstance(payload, dict) else {}


def load_shot_libraries() -> dict[str, Any]:
    payload = load_json(SHOT_LIBRARIES_PATH, {})
    return payload if isinstance(payload, dict) else {}


def build_visual_family_lookup(style_profiles: dict[str, Any]) -> dict[str, dict[str, str]]:
    families = style_profiles.get("visual_families", []) if isinstance(style_profiles, dict) else []
    lookup: dict[str, dict[str, str]] = {}
    for family in families:
        if not isinstance(family, dict):
            continue
        family_id = normalize_id(family.get("id", ""))
        if family_id:
            lookup[family_id] = {
                "id": family_id,
                "mood": str(family.get("mood", "")).strip(),
                "lighting": str(family.get("lighting", "")).strip(),
                "palette": str(family.get("palette", "")).strip(),
            }
    return lookup


def select_intent_profile(style_profiles: dict[str, Any], angle: str, intent: str) -> dict[str, Any]:
    profiles = style_profiles.get("intent_profiles", {}) if isinstance(style_profiles, dict) else {}
    normalized_intent = normalize_id(intent)
    if normalized_intent and isinstance(profiles.get(normalized_intent), dict):
        return profiles[normalized_intent]

    angle_to_intent = {
        "ideas": "inspiration",
        "style_specific": "inspiration",
        "how_to": "implementation",
        "mistakes": "problem_solving",
        "best_options": "comparison",
        "budget": "decision_making",
        "small_space": "decision_making",
    }
    fallback_intent = angle_to_intent.get(normalize_id(angle), "inspiration")
    return profiles.get(fallback_intent, {})


def select_shot_library(shot_data: dict[str, Any], cluster: str, cluster_id: str, title: str, primary_keyword: str) -> dict[str, Any]:
    libraries = shot_data.get("libraries", []) if isinstance(shot_data, dict) else []
    haystack = normalize_text(" ".join([cluster, cluster_id, title, primary_keyword]))
    best_match: dict[str, Any] | None = None
    best_score = -1

    for library in libraries:
        if not isinstance(library, dict):
            continue
        tokens = [normalize_text(token) for token in library.get("match_tokens", []) if normalize_text(token)]
        if not tokens:
            if best_match is None and normalize_id(library.get("library_id", "")) == normalize_id(shot_data.get("default_library_id", "")):
                best_match = library
            continue
        score = sum(1 for token in tokens if token in haystack)
        if score > best_score:
            best_score = score
            best_match = library

    if best_match is not None and best_score > 0:
        return best_match

    default_library_id = normalize_id(shot_data.get("default_library_id", ""))
    for library in libraries:
        if normalize_id(library.get("library_id", "")) == default_library_id:
            return library
    return libraries[0] if libraries else {}


def deterministic_choice(options: list[str], seed_text: str, fallback: str = "") -> str:
    cleaned_options = [str(item).strip() for item in options if str(item).strip()]
    if not cleaned_options:
        return fallback
    digest = hashlib.sha256(seed_text.encode("utf-8")).hexdigest() if seed_text else "0"
    return cleaned_options[int(digest, 16) % len(cleaned_options)]


def select_visual_family(
    *,
    cluster: str,
    cluster_id: str,
    angle: str,
    intent: str,
    season: str,
    intent_profile: dict[str, Any],
    family_lookup: dict[str, dict[str, str]],
) -> dict[str, str]:
    preferred_families = [
        normalize_id(item)
        for item in intent_profile.get("preferred_families", [])
        if normalize_id(item)
    ]
    if season and any(token in normalize_text(season) for token in {"spring", "summer"}):
        preferred_families = ["airy_editorial", *preferred_families]

    available = [family_lookup[family_id] for family_id in preferred_families if family_id in family_lookup]
    if not available:
        available = list(family_lookup.values())

    seed = normalize_text(" ".join([cluster, cluster_id, angle, intent, season]))
    if not available:
        return {"id": "default", "mood": "editorial interior styling", "lighting": "natural daylight", "palette": "balanced neutrals"}
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest() if seed else "0"
    return available[int(digest, 16) % len(available)]


def build_base_style_block(
    *,
    visual_family: dict[str, str],
    room: str,
    intent_profile: dict[str, Any],
    shot_library: dict[str, Any],
    composition_style: str,
    alternate_composition: str,
) -> str:
    visual_language = str(intent_profile.get("visual_language", "")).strip()
    subject_emphasis = str(shot_library.get("hero_subject_emphasis", "")).strip()
    return (
        f"Style direction: {visual_family['mood']}. "
        f"Visual language: {visual_language}. "
        f"Room context: {room}. "
        f"Lighting: {visual_family['lighting']}. "
        f"Palette: {visual_family['palette']}. "
        f"Subject emphasis: {subject_emphasis}. "
        f"Primary composition: {composition_style}. "
        f"Occasional variation: {alternate_composition}. "
        "Medium: editorial interior photography. "
        "Constraints: realistic materials, no text, no logos, no people, no artificial collage effects."
    )


def build_hero_image_prompt(
    *,
    title: str,
    cluster: str,
    cluster_id: str,
    primary_keyword: str,
    angle: str,
    intent: str,
    season: str,
    intent_profile: dict[str, Any],
    shot_library: dict[str, Any],
    visual_family: dict[str, str],
) -> str:
    room = infer_room(cluster=cluster, primary_keyword=primary_keyword, title=title)
    composition_preferences = [str(item).strip() for item in shot_library.get("composition_preferences", []) if str(item).strip()]
    intent_compositions = [str(item).strip() for item in intent_profile.get("composition_styles", []) if str(item).strip()]
    composition_style = deterministic_choice(
        composition_preferences + intent_compositions,
        seed_text=f"{title}|hero|{cluster_id}|{angle}|{intent}",
        fallback="wide establishing room shot",
    )
    alternate_composition = deterministic_choice(
        intent_compositions or composition_preferences,
        seed_text=f"{title}|hero-alt|{cluster_id}|{angle}|{intent}",
        fallback="soft asymmetrical framing",
    )
    style = build_base_style_block(
        visual_family=visual_family,
        room=room,
        intent_profile=intent_profile,
        shot_library=shot_library,
        composition_style=composition_style,
        alternate_composition=alternate_composition,
    )
    hero_emphasis = str(intent_profile.get("hero_emphasis", "")).strip()
    return (
        f"Create a hero image for the article '{title}'. "
        f"Show a {composition_style} that expresses '{cluster or primary_keyword or title}' in a magazine-worthy but believable way. "
        f"{hero_emphasis.capitalize()}. "
        "Prioritize one memorable focal point, clear spatial depth, and an image that feels editorial rather than staged for ecommerce. "
        f"{style}"
    )


def build_section_image_prompt(
    *,
    title: str,
    cluster: str,
    primary_keyword: str,
    section_heading: str,
    section_index: int,
    visual_family: dict[str, str],
    intent_profile: dict[str, Any],
    shot_library: dict[str, Any],
    composition_style: str,
    alternate_composition: str,
) -> str:
    room = infer_room(cluster=cluster, primary_keyword=primary_keyword, title=title)
    section_roles = [str(item).strip() for item in shot_library.get("section_roles", []) if str(item).strip()]
    subject_emphasis = [str(item).strip() for item in shot_library.get("subject_emphasis", []) if str(item).strip()]
    detail_focus = [str(item).strip() for item in intent_profile.get("detail_focus", []) if str(item).strip()]

    shot_role = section_roles[min(section_index - 1, len(section_roles) - 1)] if section_roles else "specific editorial section vignette"
    emphasis = subject_emphasis[min(section_index - 1, len(subject_emphasis) - 1)] if subject_emphasis else "section-specific styling detail"
    detail = detail_focus[min((section_index - 1) % max(len(detail_focus), 1), len(detail_focus) - 1)] if detail_focus else "distinct styling detail"
    style = build_base_style_block(
        visual_family=visual_family,
        room=room,
        intent_profile=intent_profile,
        shot_library=shot_library,
        composition_style=composition_style,
        alternate_composition=alternate_composition,
    )
    section_emphasis = str(intent_profile.get("section_emphasis", "")).strip()
    return (
        f"Create section image {section_index} for the article '{title}'. "
        f"Focus on '{section_heading}'. "
        f"Use a {shot_role}. "
        f"Let the main emphasis be {emphasis}. "
        f"Highlight {detail}. "
        f"{section_emphasis.capitalize()}. "
        "The image should clearly support this specific section rather than repeat the hero image. "
        "Vary focal distance, crop, and visual weight so the image set feels intentionally diverse. "
        f"{style}"
    )


def load_recent_metadata(limit: int = RECENT_METADATA_LIMIT) -> list[dict[str, Any]]:
    if not ARTICLE_METADATA_DIR.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(ARTICLE_METADATA_DIR.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
        if len(rows) >= limit:
            break
    return rows


def build_sameness_warnings(
    *,
    hero_prompt: str,
    visual_family: dict[str, str],
    shot_library: dict[str, Any],
    intent_id: str,
    article_signatures: list[str],
) -> list[str]:
    warnings: list[str] = []
    prompt_signatures = [tokenize(signature) for signature in article_signatures]

    for left_index in range(len(prompt_signatures)):
        for right_index in range(left_index + 1, len(prompt_signatures)):
            similarity = jaccard_similarity(prompt_signatures[left_index], prompt_signatures[right_index])
            if similarity >= SIMILARITY_WARNING_THRESHOLD:
                warnings.append(
                    f"Prompt sameness warning: image {left_index + 1} and image {right_index + 1} are too close in composition and subject emphasis."
                )
                break

    recent_metadata = load_recent_metadata()
    same_profile_count = 0
    hero_similarity_hits = 0
    current_family = normalize_id(visual_family.get("id", ""))
    current_library = normalize_id(shot_library.get("library_id", ""))
    current_hero_tokens = tokenize(hero_prompt)

    for row in recent_metadata:
        visual_direction = row.get("visual_direction", {}) if isinstance(row.get("visual_direction"), dict) else {}
        if (
            normalize_id(visual_direction.get("family_id", "")) == current_family
            and normalize_id(visual_direction.get("shot_library_id", "")) == current_library
            and normalize_id(visual_direction.get("intent_id", "")) == normalize_id(intent_id)
        ):
            same_profile_count += 1

        existing_hero = str(row.get("hero_image_prompt") or "").strip()
        if existing_hero and jaccard_similarity(current_hero_tokens, tokenize(existing_hero)) >= 0.72:
            hero_similarity_hits += 1

    if same_profile_count >= 3:
        warnings.append(
            "Recent-article sameness warning: this visual family, shot library, and intent combination has appeared frequently in recent posts."
        )
    if hero_similarity_hits >= 2:
        warnings.append(
            "Recent-article sameness warning: the hero prompt is very close to multiple recent hero prompts."
        )

    return list(dict.fromkeys(warnings))


def generate_image_prompts(
    *,
    title: str,
    section_headings: list[str],
    cluster: str = "",
    cluster_id: str = "",
    primary_keyword: str = "",
    angle: str = "",
    intent: str = "",
    season: str = "",
) -> dict[str, Any]:
    style_profiles = load_style_profiles()
    shot_data = load_shot_libraries()
    family_lookup = build_visual_family_lookup(style_profiles)
    intent_profile = select_intent_profile(style_profiles, angle=angle, intent=intent)
    shot_library = select_shot_library(
        shot_data,
        cluster=cluster,
        cluster_id=cluster_id,
        title=title,
        primary_keyword=primary_keyword,
    )
    visual_family = select_visual_family(
        cluster=cluster,
        cluster_id=cluster_id,
        angle=angle,
        intent=intent,
        season=season,
        intent_profile=intent_profile,
        family_lookup=family_lookup,
    )

    hero_image_prompt = build_hero_image_prompt(
        title=title,
        cluster=cluster,
        cluster_id=cluster_id,
        primary_keyword=primary_keyword,
        angle=angle,
        intent=intent,
        season=season,
        intent_profile=intent_profile,
        shot_library=shot_library,
        visual_family=visual_family,
    )
    article_signatures = [
        f"hero {shot_library.get('library_id', '')} {intent} {visual_family['id']}"
    ]

    section_image_prompts: list[str] = []
    composition_preferences = [str(item).strip() for item in shot_library.get("composition_preferences", []) if str(item).strip()]
    intent_compositions = [str(item).strip() for item in intent_profile.get("composition_styles", []) if str(item).strip()]
    for index, heading in enumerate(section_headings, start=1):
        composition_style = deterministic_choice(
            composition_preferences + intent_compositions,
            seed_text=f"{title}|{cluster_id}|{angle}|{intent}|{index}",
            fallback="editorial section view",
        )
        alternate_composition = deterministic_choice(
            list(reversed(intent_compositions or composition_preferences)),
            seed_text=f"{title}|{cluster_id}|{angle}|{intent}|alt|{index}",
            fallback="subtle alternate crop",
        )
        if alternate_composition == composition_style:
            alternate_composition = deterministic_choice(
                composition_preferences,
                seed_text=f"{title}|{cluster_id}|{angle}|{intent}|alt-fallback|{index}",
                fallback="off-center framing",
            )

        article_signatures.append(
            " ".join(
                [
                    str(shot_library.get("library_id", "")),
                    str(intent or intent_profile.get("profile_id", "")),
                    composition_style,
                    alternate_composition,
                    heading,
                ]
            )
        )
        section_image_prompts.append(
            build_section_image_prompt(
                title=title,
                cluster=cluster,
                primary_keyword=primary_keyword,
                section_heading=heading,
                section_index=index,
                visual_family=visual_family,
                intent_profile=intent_profile,
                shot_library=shot_library,
                composition_style=composition_style,
                alternate_composition=alternate_composition,
            )
        )

    sameness_warnings = build_sameness_warnings(
        hero_prompt=hero_image_prompt,
        visual_family=visual_family,
        shot_library=shot_library,
        intent_id=intent or "",
        article_signatures=article_signatures,
    )

    return {
        "visual_direction": {
            "family_id": visual_family["id"],
            "mood": visual_family["mood"],
            "lighting": visual_family["lighting"],
            "palette": visual_family["palette"],
            "intent_id": normalize_id(intent) or normalize_id(intent_profile.get("profile_id", "")),
            "profile_id": normalize_id(intent_profile.get("profile_id", "")),
            "shot_library_id": normalize_id(shot_library.get("library_id", "")),
        },
        "hero_image_prompt": hero_image_prompt,
        "section_image_prompts": section_image_prompts,
        "image_prompt_diagnostics": {
            "sameness_warnings": sameness_warnings,
            "recent_articles_checked": len(load_recent_metadata()),
        },
    }


def main() -> int:
    args = parse_args()

    title = args.title.strip()
    if not title:
        print("Error: title cannot be empty.", file=sys.stderr)
        return 1

    try:
        section_headings = validate_sections(args.section)
        output = generate_image_prompts(
            title=title,
            section_headings=section_headings,
            cluster=args.cluster,
            cluster_id=args.cluster_id,
            primary_keyword=args.primary_keyword,
            angle=args.angle,
            intent=args.intent,
            season=args.season,
        )
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
