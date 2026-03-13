from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from pinterest_strategy import build_pin_distribution_strategy

DEFAULT_VARIANT_COUNT = 4
BOARD_CONFIG_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_boards.json"
MAX_TITLE_LENGTH = 90
MAX_DESCRIPTION_LENGTH = 260

BOARD_RULES = [
    {"category": "kitchen", "keywords": ["kitchen"]},
    {"category": "living_room", "keywords": ["living room", "sofa"]},
    {"category": "bedroom", "keywords": ["bedroom", "nightstand", "bedside"]},
    {"category": "bathroom", "keywords": ["bathroom", "vanity"]},
    {"category": "kids_room", "keywords": ["nursery", "kids", "playroom"]},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Pinterest-ready metadata from published article metadata."
    )
    parser.add_argument("metadata_json_path", type=str, help="Path to article metadata JSON.")
    parser.add_argument(
        "--variants",
        type=int,
        default=DEFAULT_VARIANT_COUNT,
        help=f"Minimum Pinterest variants to prepare (default: {DEFAULT_VARIANT_COUNT}).",
    )
    return parser.parse_args()


def load_env(project_root: Path) -> None:
    load_dotenv(project_root / ".env")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    raw = path.read_text(encoding="utf-8-sig")
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


def load_board_config(path: Path = BOARD_CONFIG_PATH) -> dict[str, str]:
    data = load_json(path)
    normalized: dict[str, str] = {}
    for key, value in data.items():
        key_text = str(key).strip()
        value_text = str(value).strip()
        if key_text and value_text:
            normalized[key_text] = value_text

    if "default" not in normalized:
        normalized["default"] = "Home Decor Inspiration"
    return normalized


def board_from_category(board_config: dict[str, str], category: str) -> dict[str, str]:
    name = board_config.get(category) or board_config["default"]
    return {"key": category, "name": name}


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


def infer_topic_board(topic_text: str, board_config: dict[str, str]) -> dict[str, str]:
    haystack = normalize_copy(topic_text).lower()
    for rule in BOARD_RULES:
        if any(keyword in haystack for keyword in rule["keywords"]):
            return board_from_category(board_config, rule["category"])
    return board_from_category(board_config, "default")


def select_variant_count(requested_count: int) -> int:
    return max(DEFAULT_VARIANT_COUNT, requested_count)


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
        "meta_description": ensure_terminal_punctuation(
            truncate_text(str(data["meta_description"]), MAX_DESCRIPTION_LENGTH)
        ),
        "hero_image_path": str(data["hero_image_path"]).strip(),
        "article_relative_url": str(data["article_relative_url"]).strip(),
        "pinterest_titles": title_candidates,
        "pinterest_descriptions": description_candidates,
        "minimum_variant_count": requested_variant_count,
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
        "practical_tips": (
            f"Use these practical design moves to make {topic_phrase.lower()} feel balanced, useful, "
            "and easy to live with."
        ),
        "product_led": (
            f"Discover decor pieces and finishing details that help {topic_phrase.lower()} feel more "
            "intentional without looking overdone."
        ),
        "styling_angle": (
            f"See how color, texture, and room styling can shape {topic_phrase.lower()} into a softer, "
            "more editorial look."
        ),
    }
    base = templates.get(variant_type, meta_description or candidate)
    return ensure_terminal_punctuation(truncate_text(base, MAX_DESCRIPTION_LENGTH))


def build_variant_payloads(article_metadata: dict[str, Any], board_config: dict[str, str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    topic_text = f"{article_metadata['title']} {article_metadata['meta_description']} {article_metadata['slug']}"
    topic_board = infer_topic_board(topic_text, board_config=board_config)
    strategy = build_pin_distribution_strategy(
        article_slug=article_metadata["slug"],
        topic_text=topic_text,
        topic_board=topic_board,
        trend_board=board_from_category(board_config, "decor_trends"),
        tips_board=topic_board,
        product_board=board_from_category(board_config, "home_products"),
        default_board=board_from_category(board_config, "default"),
    )

    print("[pinterest] applying performance weighting")
    for note in strategy["notes"]:
        print(f"[pinterest] {note}")

    plans = strategy["plans"]
    minimum_variant_count = article_metadata["minimum_variant_count"]
    if len(plans) < minimum_variant_count:
        raise ValueError("Pinterest strategy returned fewer variants than the minimum required.")

    variants: list[dict[str, Any]] = []
    for index, plan in enumerate(plans, start=1):
        variant_type = str(plan["variant_type"])
        variants.append(
            {
                "variant_key": f"pin-{index}",
                "variant_type": variant_type,
                "style_name": str(plan["style_name"]),
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
                "board": dict(plan["board"]),
                "priority_score": plan["priority_score"],
                "schedule_rank": plan["schedule_rank"],
            }
        )

    return variants, {
        "minimum_variant_count": minimum_variant_count,
        "generated_variant_count": len(variants),
        "bonus_slots": strategy["bonus_slots"],
        "ranked_variant_types": strategy["ranked_variant_types"],
        "article_score": strategy["article_score"],
        "notes": strategy["notes"],
        "board_config_path": str(BOARD_CONFIG_PATH),
    }


def build_pinterest_payload(article_metadata: dict[str, Any], project_root: Path) -> dict[str, Any]:
    site_config = load_site_config(project_root)
    board_config = load_board_config()

    site_root_url = build_site_root_url(
        site_url=site_config.get("url", ""),
        baseurl=site_config.get("baseurl", ""),
    )
    article_url = build_article_url(
        site_url=site_config.get("url", ""),
        baseurl=site_config.get("baseurl", ""),
        article_relative_url=article_metadata["article_relative_url"],
    )
    variants, strategy_summary = build_variant_payloads(article_metadata, board_config=board_config)

    return {
        "article_title": article_metadata["title"],
        "article_slug": article_metadata["slug"],
        "article_relative_url": article_metadata["article_relative_url"],
        "article_url": article_url,
        "meta_description": article_metadata["meta_description"],
        "hero_image_path": article_metadata["hero_image_path"],
        "site_root_url": site_root_url,
        "variant_count": len(variants),
        "strategy": strategy_summary,
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
