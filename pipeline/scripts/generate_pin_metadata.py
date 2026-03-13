from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

DEFAULT_VARIANT_COUNT = 4
DEFAULT_BOARD_KEY = "home-decor"
DEFAULT_BOARD_NAME = "Home Decor"
MAX_TITLE_LENGTH = 90
MAX_DESCRIPTION_LENGTH = 260

VARIANT_SPECS = [
    {"type": "trend_overview", "style": "bottom-panel"},
    {"type": "practical_tips", "style": "center-card"},
    {"type": "product_led", "style": "product-focus"},
    {"type": "styling_angle", "style": "top-band"},
]

BOARD_RULES = [
    {"key": "kitchen-decor", "name": "Kitchen Decor", "keywords": ["kitchen"]},
    {"key": "living-room-decor", "name": "Living Room Decor", "keywords": ["living room", "sofa"]},
    {"key": "bedroom-decor", "name": "Bedroom Decor", "keywords": ["bedroom", "nightstand", "bedside"]},
    {"key": "bathroom-decor", "name": "Bathroom Decor", "keywords": ["bathroom", "vanity"]},
    {"key": "nursery-kids-decor", "name": "Kids Decor", "keywords": ["nursery", "kids", "playroom"]},
]

TREND_BOARD = {"key": "decor-trends", "name": "Decor Trends"}
TIPS_BOARD = {"key": "styling-tips", "name": "Styling Tips"}
PRODUCT_BOARD = {"key": "decor-finds", "name": "Decor Finds"}
DEFAULT_BOARD = {"key": DEFAULT_BOARD_KEY, "name": DEFAULT_BOARD_NAME}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Pinterest-ready metadata from published article metadata."
    )
    parser.add_argument("metadata_json_path", type=str, help="Path to article metadata JSON.")
    parser.add_argument(
        "--variants",
        type=int,
        default=DEFAULT_VARIANT_COUNT,
        help=f"How many Pinterest variants to prepare (default: {DEFAULT_VARIANT_COUNT}).",
    )
    return parser.parse_args()


def load_env(project_root: Path) -> None:
    load_dotenv(project_root / ".env")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return data


def load_site_config(project_root: Path) -> dict[str, str]:
    config_path = project_root / "_config.yml"
    if not config_path.exists():
        return {"url": "", "baseurl": ""}

    result = {"url": "", "baseurl": ""}
    for line in config_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("url:"):
            result["url"] = stripped.split(":", 1)[1].strip().strip('"')
        elif stripped.startswith("baseurl:"):
            result["baseurl"] = stripped.split(":", 1)[1].strip().strip('"')
    return result


def build_site_root_url(site_url: str, baseurl: str) -> str:
    clean_site_url = site_url.rstrip("/")
    clean_baseurl = baseurl.strip()
    if clean_baseurl in {"", "/"}:
        return clean_site_url
    return f"{clean_site_url}/{clean_baseurl.strip('/')}"


def build_article_url(site_url: str, baseurl: str, article_relative_url: str) -> str:
    site_root_url = build_site_root_url(site_url=site_url, baseurl=baseurl)
    relative_part = article_relative_url if article_relative_url.startswith("/") else f"/{article_relative_url}"
    return f"{site_root_url}{relative_part}"


def normalize_copy(text: str) -> str:
    compact = re.sub(r"\s+", " ", str(text).strip())
    return compact.strip(" .")


def normalize_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_copy(text).lower())


def truncate_text(text: str, max_length: int) -> str:
    cleaned = normalize_copy(text)
    if len(cleaned) <= max_length:
        return cleaned
    shortened = cleaned[: max_length - 3].rsplit(" ", 1)[0].strip()
    return (shortened or cleaned[: max_length - 3]).rstrip(" ,;:-") + "..."


def ensure_terminal_punctuation(text: str) -> str:
    cleaned = normalize_copy(text)
    if not cleaned:
        return cleaned
    if cleaned.endswith((".", "!", "?")):
        return cleaned
    return f"{cleaned}."


def dedupe_preserving_order(values: list[str], max_length: int, add_punctuation: bool = False) -> list[str]:
    distinct: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = truncate_text(value, max_length)
        if add_punctuation:
            normalized = ensure_terminal_punctuation(normalized)
        key = normalize_key(normalized)
        if not normalized or key in seen:
            continue
        seen.add(key)
        distinct.append(normalized)
    return distinct


def title_case_slug(slug: str) -> str:
    return normalize_copy(slug.replace("-", " ")).title()


def infer_topic_phrase(article_title: str, slug: str) -> str:
    cleaned_title = normalize_copy(article_title)
    if cleaned_title:
        return cleaned_title
    return title_case_slug(slug)


def infer_topic_board(topic_text: str) -> dict[str, str]:
    haystack = normalize_copy(topic_text).lower()
    for rule in BOARD_RULES:
        if any(keyword in haystack for keyword in rule["keywords"]):
            return {"key": rule["key"], "name": rule["name"]}
    return DEFAULT_BOARD.copy()


def select_variant_count(requested_count: int) -> int:
    if requested_count < 4:
        return 4
    return 4


def validate_article_metadata(data: dict[str, Any], variant_count: int) -> dict[str, Any]:
    required_fields = [
        "title",
        "slug",
        "meta_description",
        "hero_image_path",
        "pinterest_titles",
        "pinterest_descriptions",
        "article_relative_url",
    ]
    missing = [field for field in required_fields if field not in data]
    if missing:
        raise ValueError(f"Article metadata missing required fields: {', '.join(missing)}")

    pinterest_titles = [str(item).strip() for item in data["pinterest_titles"] if str(item).strip()]
    pinterest_descriptions = [
        str(item).strip() for item in data["pinterest_descriptions"] if str(item).strip()
    ]

    requested_variant_count = select_variant_count(variant_count)
    topic_phrase = infer_topic_phrase(article_title=str(data["title"]), slug=str(data["slug"]))

    title_candidates = dedupe_preserving_order(
        pinterest_titles + [str(data["title"]), title_case_slug(str(data["slug"]))],
        max_length=MAX_TITLE_LENGTH,
    )
    description_candidates = dedupe_preserving_order(
        pinterest_descriptions + [str(data["meta_description"])],
        max_length=MAX_DESCRIPTION_LENGTH,
        add_punctuation=True,
    )

    if not title_candidates:
        raise ValueError("No Pinterest title ideas were available.")
    if not description_candidates:
        raise ValueError("No Pinterest description ideas were available.")

    return {
        "title": normalize_copy(str(data["title"])),
        "slug": normalize_copy(str(data["slug"])),
        "meta_description": ensure_terminal_punctuation(truncate_text(str(data["meta_description"]), MAX_DESCRIPTION_LENGTH)),
        "hero_image_path": str(data["hero_image_path"]).strip(),
        "article_relative_url": str(data["article_relative_url"]).strip(),
        "pinterest_titles": title_candidates,
        "pinterest_descriptions": description_candidates,
        "variant_count": requested_variant_count,
        "topic_phrase": topic_phrase,
    }


def build_pin_image_path(slug: str, index: int) -> str:
    return f"/assets/pins/{slug}/pin-{index}.svg"


def pick_candidate(candidates: list[str], index: int) -> str:
    return candidates[index % len(candidates)]


def build_variant_title(variant_type: str, topic_phrase: str, title_candidates: list[str], index: int) -> str:
    candidate = pick_candidate(title_candidates, index)
    templates = {
        "trend_overview": candidate,
        "practical_tips": f"{topic_phrase}: Practical Tips That Make It Work",
        "product_led": f"4 Pieces That Elevate {topic_phrase}",
        "styling_angle": f"How to Style {topic_phrase} for a More Collected Look",
    }
    return truncate_text(templates.get(variant_type, candidate), MAX_TITLE_LENGTH)


def build_variant_description(
    variant_type: str,
    topic_phrase: str,
    description_candidates: list[str],
    meta_description: str,
    index: int,
) -> str:
    candidate = pick_candidate(description_candidates, index)
    templates = {
        "trend_overview": candidate,
        "practical_tips": f"Use these practical design moves to make {topic_phrase.lower()} feel balanced, useful, and easy to live with.",
        "product_led": f"Discover decor pieces and finishing details that help {topic_phrase.lower()} feel more intentional without looking overdone.",
        "styling_angle": f"See how color, texture, and room styling can shape {topic_phrase.lower()} into a softer, more editorial look.",
    }
    base = templates.get(variant_type, meta_description or candidate)
    return ensure_terminal_punctuation(truncate_text(base, MAX_DESCRIPTION_LENGTH))


def assign_board(variant_type: str, topic_board: dict[str, str], topic_text: str) -> dict[str, str]:
    haystack = normalize_copy(topic_text).lower()
    if variant_type == "trend_overview":
        if "trend" in haystack or "trending" in haystack:
            return TREND_BOARD.copy()
        return topic_board.copy()
    if variant_type == "practical_tips":
        return TIPS_BOARD.copy()
    if variant_type == "product_led":
        return PRODUCT_BOARD.copy()
    if variant_type == "styling_angle":
        return topic_board.copy() if topic_board["key"] != DEFAULT_BOARD["key"] else TIPS_BOARD.copy()
    return DEFAULT_BOARD.copy()


def build_variant_payloads(article_metadata: dict[str, Any]) -> list[dict[str, Any]]:
    topic_text = f"{article_metadata['title']} {article_metadata['meta_description']} {article_metadata['slug']}"
    topic_board = infer_topic_board(topic_text)
    variants: list[dict[str, Any]] = []

    for index, spec in enumerate(VARIANT_SPECS[: article_metadata["variant_count"]], start=1):
        variant_type = spec["type"]
        board = assign_board(variant_type=variant_type, topic_board=topic_board, topic_text=topic_text)
        variants.append(
            {
                "variant_key": f"pin-{index}",
                "variant_type": variant_type,
                "style_name": spec["style"],
                "title": build_variant_title(
                    variant_type=variant_type,
                    topic_phrase=article_metadata["topic_phrase"],
                    title_candidates=article_metadata["pinterest_titles"],
                    index=index - 1,
                ),
                "description": build_variant_description(
                    variant_type=variant_type,
                    topic_phrase=article_metadata["topic_phrase"],
                    description_candidates=article_metadata["pinterest_descriptions"],
                    meta_description=article_metadata["meta_description"],
                    index=index - 1,
                ),
                "image_path": build_pin_image_path(slug=article_metadata["slug"], index=index),
                "board": board,
            }
        )

    return variants


def build_pinterest_payload(article_metadata: dict[str, Any], project_root: Path) -> dict[str, Any]:
    site_config = load_site_config(project_root)

    site_root_url = build_site_root_url(
        site_url=site_config.get("url", ""),
        baseurl=site_config.get("baseurl", ""),
    )
    article_url = build_article_url(
        site_url=site_config.get("url", ""),
        baseurl=site_config.get("baseurl", ""),
        article_relative_url=article_metadata["article_relative_url"],
    )
    variants = build_variant_payloads(article_metadata)

    return {
        "article_title": article_metadata["title"],
        "article_slug": article_metadata["slug"],
        "article_relative_url": article_metadata["article_relative_url"],
        "article_url": article_url,
        "meta_description": article_metadata["meta_description"],
        "hero_image_path": article_metadata["hero_image_path"],
        "site_root_url": site_root_url,
        "variant_count": article_metadata["variant_count"],
        "variants": variants,
    }


def build_output_path(project_root: Path, slug: str) -> Path:
    return project_root / "_data" / "pinterest" / f"{slug}.json"


def generate_pinterest_metadata(metadata_path: Path, variant_count: int = DEFAULT_VARIANT_COUNT) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    load_env(project_root)

    article_metadata = load_json(metadata_path)
    normalized = validate_article_metadata(article_metadata, variant_count=variant_count)
    payload = build_pinterest_payload(normalized, project_root=project_root)

    output_path = build_output_path(project_root=project_root, slug=normalized["slug"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def main() -> int:
    args = parse_args()
    try:
        output_path = generate_pinterest_metadata(
            metadata_path=Path(args.metadata_json_path),
            variant_count=args.variants,
        )
        print(output_path)
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
