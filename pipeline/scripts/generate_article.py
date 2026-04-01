from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

from article_title_generation import choose_title_set
from content_architecture import resolve_intent_id
from fetch_products import Product, fetch_products_for_trend
from generate_image_prompts import generate_image_prompts
from internal_linking import build_internal_link_suggestions
from monetization_profiles import (
    limit_products_for_profile,
    resolve_affiliate_section_indexes,
    resolve_monetization_profile,
)
from normalize_keyword_phrase import normalize_phrase, normalize_title
from topic_clusters import TopicCandidate, build_manual_topic_candidate, normalize_text as normalize_topic_text
from validate_article_concept import ConceptValidationError, ensure_valid_article_concept


SYSTEM_PROMPT = """
You are an experienced decor editorial writer for a lifestyle blog.
Write natural, human-sounding content that feels warm, practical, and trustworthy.
Focus on clarity, useful advice, and strong structure.

Return only valid JSON with this exact shape:
{
  "title": "string",
  "slug": "string",
  "meta_description": "string",
  "keywords": ["string", "string"],
  "estimated_reading_time": "string",
  "hero_image_prompt": "string",
  "section_image_prompts": ["string", "string", "string", "string", "string"],
  "pinterest_titles": ["string", "string", "string", "string", "string"],
  "pinterest_descriptions": ["string", "string", "string", "string", "string"],
  "article_markdown": "string"
}

Rules:
- The article must focus on exactly one decor trend.
- Preferred length target: 1000 to 1300 words (hard valid range: 950 to 1600).
- Include: introduction, exactly 5 main sections, a short FAQ section, and conclusion.
- Do not label the introduction with a heading. Start with plain introductory paragraphs, then begin the 5 main sections with H2 headings.
- Do not include an H1 title inside article_markdown; the site layout renders the post title separately.
- Use markdown headings for structure with H2 headings for the 5 main sections and H3 only when useful inside a section.
- Add a short FAQ section near the end using an H2 heading like "## FAQ" or "## Frequently Asked Questions" with 3 to 5 concise H3 question headings and short answers.
- Use the supplied angle brief to shape the section rhythm so different article angles do not feel interchangeable.
- Keep tone editorial, practical, and human.
- Avoid generic openings, bland headings, and advice that could fit almost any decor article.
- Avoid fake data, fake citations, and keyword stuffing.
- Do not include any text outside the JSON.
""".strip()


ARTICLE_TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "prompts" / "article_template.md"
ARTICLE_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "article_packages"
BRAND_VOICE_GUIDE_PATH = Path(__file__).resolve().parents[1] / "data" / "brand_voice_guide.json"

SHORT_RETRY_PROMPT_TEMPLATE = """
The previous draft was too short at {word_count} words, below the minimum of {min_words}.
This is retry {retry_number} of {retry_limit} for a short draft.
Rewrite the full article package and expand it with richer, practical detail.

Mandatory structure and word budget:
- Introduction: 120-180 words
- 5 main sections: each 190-240 words
- Conclusion: 100-150 words
- Total article_markdown hard valid range: {min_words}-{max_words} words (preferred target: 1000-1300 words)

Every section must include:
- one specific decor idea
- why it works visually
- practical application guidance
- relevant colors/materials/textures/room context
- enough explanation to avoid thin content
- Preserve the angle-sensitive structure and section rhythm from the original brief.

Additional instruction for this retry:
{extra_strength_instruction}

Do not add filler. Add useful, concrete depth.
Return only the required JSON object.
""".strip()


PRODUCT_RETRY_PROMPT_TEMPLATE = """
The previous draft did not follow the product-link rules.
Rewrite the full article package and follow these rules exactly:
- Use each provided product exactly once in a different main section.
- Only sections with a provided product may include an affiliate URL.
- Sections without a provided product must stay editorial-only with no external links.
- In each affiliate-enabled section, include one visible markdown link in the prose.
- Use the product's exact title as the markdown link anchor text.
- Use only provided affiliate URLs.
- Do not invent products.
- Do not invent links.
- Do not include external URLs beyond the provided affiliate URLs.

Return only the required JSON object.
""".strip()


OUTPUT_REQUIREMENTS_PROMPT = """
Output requirements:
- Keep existing fields unchanged: title, slug, meta_description, keywords, article_markdown.
- Add estimated_reading_time as a short string like "6 min read".
- Add hero_image_prompt as one interior-design image prompt.
- Add section_image_prompts as exactly 5 interior-design image prompts.
- Add pinterest_titles as exactly 5 distinct title options.
- Add pinterest_descriptions as exactly 5 distinct description options.

Image prompt constraints (hero and section prompts):
- editorial interior photography
- natural daylight
- realistic materials and textures
- no text
- no logos
- no people
""".strip()


FORMAT_FILE_MAP = {
    "trend guide": "trend_guide.md",
    "ideas article": "ideas_article.md",
    "how to guide": "how_to_guide.md",
    "styling advice": "styling_advice.md",
    "best options": "best_options.md",
    "mistakes and fixes": "mistakes_and_fixes.md",
}

PERSONA_FILE_MAP = {
    "elena hart": "elena_hart.md",
    "sophie bennett": "sophie_bennett.md",
    "marco alvarez": "marco_alvarez.md",
}

MIN_WORDS = 950
MAX_WORDS = 1600
PREFERRED_MIN_WORDS = 1000
PREFERRED_MAX_WORDS = 1300
SECTION_COUNT = 5
SHORT_RETRY_LIMIT = 1
PRODUCT_RETRY_LIMIT = 1
PINTEREST_ITEM_COUNT = 5
# Preferred guidance range is 1000-1300 words. Hard validation uses MIN_WORDS/MAX_WORDS.

ANGLE_FORMAT_MAP = {
    "ideas": "ideas article",
    "how_to": "how to guide",
    "mistakes": "mistakes and fixes",
    "best_options": "best options",
}

ANGLE_STRUCTURE_GUIDANCE = {
    "ideas": {
        "name": "ideas-led",
        "outline": [
            "Introduction: open with an aspirational but grounded read on why this look is appealing right now and what kind of home it suits.",
            "Section 1: establish the strongest foundational idea readers should understand first.",
            "Section 2: introduce a contrasting or complementary idea that changes the mood, palette, or material story.",
            "Section 3: shift into a room-zone, furniture, or styling layer that helps the look feel livable.",
            "Section 4: show a more specific application, styling twist, or variation for a real home context.",
            "Section 5: finish with a unifying idea that helps readers edit the look and choose what to try first.",
            "Conclusion: leave the reader with a styling takeaway, not just a recap.",
        ],
        "heading_style": "Make H2s feel like distinct decor directions or styling lenses rather than generic tips.",
        "faq_style": "FAQ should answer practical follow-up questions about applying the ideas in real rooms.",
    },
    "how_to": {
        "name": "how-to",
        "outline": [
            "Introduction: frame the decorating problem clearly and explain what readers will be able to do by the end.",
            "Section 1: define the first practical decision or setup step.",
            "Section 2: walk through the next decision with clear implementation guidance.",
            "Section 3: cover the most important adjustment readers often overlook in practice.",
            "Section 4: explain how to refine, balance, or troubleshoot the look once the basics are in place.",
            "Section 5: close the implementation path with finishing decisions, edits, or a simplified checklist in prose.",
            "Conclusion: summarize the process in a calm, confidence-building way.",
        ],
        "heading_style": "Make H2s feel action-led and practical, with verbs like choose, layer, place, balance, or style where natural.",
        "faq_style": "FAQ should answer common execution and troubleshooting questions.",
    },
    "mistakes": {
        "name": "mistakes-and-fixes",
        "outline": [
            "Introduction: explain briefly why this look often goes wrong and why the details matter.",
            "Section 1: identify one common mistake and immediately explain the fix.",
            "Section 2: cover a second mistake with a different kind of correction or styling adjustment.",
            "Section 3: move into a more subtle or easy-to-miss mistake that affects cohesion.",
            "Section 4: address a practical mistake around proportion, materials, light, or placement.",
            "Section 5: finish with the mistake that most affects the final feel of the room and how to correct it.",
            "Conclusion: leave readers with a simple avoid/fix checklist tone, not alarmism.",
        ],
        "heading_style": "Let H2s signal either the mistake, the correction, or both, without making every heading read like a formula.",
        "faq_style": "FAQ should answer short corrective questions readers might ask after realizing they made one of the mistakes.",
    },
    "best_options": {
        "name": "best-options",
        "outline": [
            "Introduction: explain what makes choosing the right option difficult and what criteria matter most.",
            "Section 1: establish the most important selection criteria or baseline standards.",
            "Section 2: cover one strong option category, material family, or use case.",
            "Section 3: compare another option category or a different reader need.",
            "Section 4: help readers match options to room size, layout, budget, or style context.",
            "Section 5: finish with buying or selection guidance that helps readers narrow the field confidently.",
            "Conclusion: summarize how to choose the best fit rather than naming a fake universal winner.",
        ],
        "heading_style": "Make H2s feel recommendation-led, comparative, or criteria-led rather than purely inspirational.",
        "faq_style": "FAQ should cover selection, fit, sizing, material, or buying concerns.",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one decor article package from a trend using OpenAI."
    )
    parser.add_argument("trend", type=str, help='Trend text, e.g. "terracotta kitchen decor"')
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4.1-mini",
        help="OpenAI model to use (default: gpt-4.1-mini).",
    )
    parser.add_argument(
        "--format",
        dest="format_name",
        type=str,
        default=None,
        help=(
            "Optional article format name: trend guide, ideas article, "
            "styling advice, mistakes and fixes."
        ),
    )
    parser.add_argument(
        "--persona",
        dest="persona_name",
        type=str,
        default=None,
        help=(
            "Optional persona name: elena hart, sophie bennett, marco alvarez. "
            "If omitted, persona rotates deterministically."
        ),
    )
    parser.add_argument(
        "--products-file",
        type=str,
        default=None,
        help="Optional JSON file containing a product list.",
    )
    return parser.parse_args()


def load_openai_api_key() -> str:
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    load_dotenv(env_path)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(f"OPENAI_API_KEY was not found in {env_path}")
    return api_key


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9\s-]", "", text).strip().lower()
    slug = re.sub(r"[\s_-]+", "-", cleaned).strip("-")
    return slug or "decor-article"


def normalize_format_name(format_name: str) -> str:
    normalized = format_name.strip().lower().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", normalized)


def normalize_persona_name(persona_name: str) -> str:
    normalized = persona_name.strip().lower().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", normalized)


def normalize_identifier(value: Any) -> str:
    return normalize_topic_text(value).replace(" ", "_")


def load_article_template() -> str:
    if not ARTICLE_TEMPLATE_PATH.exists():
        raise RuntimeError(f"Article prompt template not found: {ARTICLE_TEMPLATE_PATH}")

    content = ARTICLE_TEMPLATE_PATH.read_text(encoding="utf-8").strip()
    if not content:
        raise RuntimeError(f"Article prompt template is empty: {ARTICLE_TEMPLATE_PATH}")

    return content


def normalize_topic_candidate(topic_context: TopicCandidate | dict[str, Any] | None, trend: str) -> TopicCandidate:
    if topic_context is None:
        return build_manual_topic_candidate(trend)

    normalized: TopicCandidate = {
        "domain_id": normalize_identifier(topic_context.get("domain_id", "")),
        "cluster_id": normalize_identifier(topic_context.get("cluster_id", "")),
        "trend_cluster": normalize_topic_text(topic_context.get("trend_cluster", trend)),
        "trend_keyword": normalize_topic_text(topic_context.get("trend_keyword", trend)),
        "primary_keyword": normalize_topic_text(topic_context.get("primary_keyword", trend)),
        "secondary_keywords": [
            normalize_topic_text(item)
            for item in topic_context.get("secondary_keywords", [])
            if normalize_topic_text(item)
        ],
        "cluster_keywords": [
            normalize_topic_text(item)
            for item in topic_context.get("cluster_keywords", [])
            if normalize_topic_text(item)
        ],
        "search_intent": normalize_topic_text(topic_context.get("search_intent", "")) or "styling_advice",
        "intent_id": normalize_identifier(topic_context.get("intent_id", "")),
        "season": normalize_topic_text(topic_context.get("season", "")),
        "holiday": normalize_topic_text(topic_context.get("holiday", "")),
        "source": normalize_topic_text(topic_context.get("source", "")) or "manual",
        "subtopic_id": normalize_identifier(topic_context.get("subtopic_id", "")),
        "subtopic_name": normalize_topic_text(topic_context.get("subtopic_name", "")),
        "angle_id": normalize_identifier(topic_context.get("angle_id", "")),
        "modifier": normalize_topic_text(topic_context.get("modifier", "")),
    }

    normalized["intent_id"] = (
        normalized["intent_id"]
        or resolve_intent_id(
            angle_id=normalized["angle_id"],
            explicit_intent_id=topic_context.get("intent_id", ""),
        )
    )

    normalized["primary_keyword"] = normalize_phrase(
        normalized["primary_keyword"] or trend,
        cluster=normalized["trend_cluster"],
        subtopic=normalized["subtopic_name"],
        angle=normalized["angle_id"],
    )
    normalized["trend_keyword"] = normalize_phrase(
        normalized["trend_keyword"] or normalized["primary_keyword"] or trend,
        cluster=normalized["trend_cluster"],
        subtopic=normalized["subtopic_name"],
        angle=normalized["angle_id"],
    )
    normalized["secondary_keywords"] = list(
        dict.fromkeys(
            normalize_phrase(
                item,
                cluster=normalized["trend_cluster"],
                subtopic=normalized["subtopic_name"],
                angle=normalized["angle_id"],
            )
            for item in normalized["secondary_keywords"]
        )
    )
    normalized["secondary_keywords"] = [
        item for item in normalized["secondary_keywords"] if item and item != normalized["primary_keyword"]
    ]
    normalized["cluster_keywords"] = list(
        dict.fromkeys(
            normalize_phrase(
                item,
                cluster=normalized["trend_cluster"],
                subtopic=normalized["subtopic_name"],
                angle=normalized["angle_id"],
            )
            for item in normalized["cluster_keywords"]
        )
    )
    normalized["cluster_keywords"] = [item for item in normalized["cluster_keywords"] if item]

    if not normalized["cluster_keywords"]:
        normalized["cluster_keywords"] = [
            normalized["primary_keyword"],
            *normalized["secondary_keywords"],
        ]

    if not normalized["trend_cluster"]:
        normalized["trend_cluster"] = normalized["primary_keyword"]
    if not normalized["trend_keyword"]:
        normalized["trend_keyword"] = normalized["primary_keyword"]
    if not normalized["primary_keyword"]:
        normalized["primary_keyword"] = normalize_topic_text(trend)
    if not normalized["secondary_keywords"]:
        normalized["secondary_keywords"] = [
            keyword
            for keyword in normalized["cluster_keywords"]
            if keyword != normalized["primary_keyword"]
        ][:4]

    return normalized


def build_article_cache_key(
    trend: str,
    model: str,
    format_name: str,
    persona_name: str,
    article_template: str,
    format_prompt: str,
    persona_prompt: str,
    topic_context: TopicCandidate,
    products: list[Product],
    monetization_profile: dict[str, Any],
) -> str:
    payload = {
        "trend": trend.strip(),
        "model": model.strip(),
        "format_name": format_name,
        "persona_name": persona_name,
        "article_template": article_template,
        "brand_voice_prompt": build_brand_voice_prompt(),
        "format_prompt": format_prompt,
        "persona_prompt": persona_prompt,
        "topic_context": topic_context,
        "products": products,
        "monetization_profile": monetization_profile,
        "title_generation_version": 2,
        "min_words": MIN_WORDS,
        "max_words": MAX_WORDS,
        "section_count": SECTION_COUNT,
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_article_cache_path(cache_key: str) -> Path:
    return ARTICLE_CACHE_DIR / f"{cache_key}.json"


def build_cache_artifacts(
    trend: str,
    model: str,
    format_name: str,
    persona_name: str,
    article_template: str,
    format_prompt: str,
    persona_prompt: str,
    topic_context: TopicCandidate,
    products: list[Product],
    monetization_profile: dict[str, Any],
) -> tuple[str, Path]:
    cache_key = build_article_cache_key(
        trend=trend,
        model=model,
        format_name=format_name,
        persona_name=persona_name,
        article_template=article_template,
        format_prompt=format_prompt,
        persona_prompt=persona_prompt,
        topic_context=topic_context,
        products=products,
        monetization_profile=monetization_profile,
    )
    return cache_key, build_article_cache_path(cache_key)


def load_cached_article_package(
    cache_path: Path,
    products: list[Product],
    topic_context: TopicCandidate,
    monetization_profile: dict[str, Any],
) -> dict[str, Any] | None:
    if not cache_path.exists():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None

    package = payload.get("package")
    if not isinstance(package, dict):
        return None

    try:
        return normalize_and_validate(
            package,
            products=products,
            topic_context=topic_context,
            monetization_profile=monetization_profile,
        )
    except Exception:
        return None


def save_cached_article_package(cache_path: Path, package: dict[str, Any]) -> Path:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "package": package,
    }
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return cache_path


def get_format_prompts() -> dict[str, str]:
    prompts_dir = Path(__file__).resolve().parents[1] / "prompts" / "formats"
    prompts: dict[str, str] = {}

    for format_name, file_name in FORMAT_FILE_MAP.items():
        file_path = prompts_dir / file_name
        if not file_path.exists():
            raise RuntimeError(f"Format prompt file not found: {file_path}")

        content = file_path.read_text(encoding="utf-8").strip()
        if not content:
            raise RuntimeError(f"Format prompt file is empty: {file_path}")

        prompts[format_name] = content

    return prompts


def load_brand_voice_guide() -> dict[str, Any]:
    raw = BRAND_VOICE_GUIDE_PATH.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("brand_voice_guide.json must contain an object.")
    return data


def get_persona_prompts() -> dict[str, str]:
    prompts_dir = Path(__file__).resolve().parents[1] / "prompts" / "personas"
    prompts: dict[str, str] = {}

    for persona_name, file_name in PERSONA_FILE_MAP.items():
        file_path = prompts_dir / file_name
        if not file_path.exists():
            raise RuntimeError(f"Persona prompt file not found: {file_path}")

        content = file_path.read_text(encoding="utf-8").strip()
        if not content:
            raise RuntimeError(f"Persona prompt file is empty: {file_path}")

        prompts[persona_name] = content

    return prompts


def choose_format_name(trend: str, format_names: list[str]) -> str:
    trend_key = trend.strip().lower()
    if not trend_key:
        return format_names[0]

    digest = hashlib.sha256(trend_key.encode("utf-8")).hexdigest()
    index = int(digest, 16) % len(format_names)
    return format_names[index]


def resolve_angle_structure(angle_id: str) -> dict[str, Any]:
    normalized_angle = normalize_identifier(angle_id or "")
    return ANGLE_STRUCTURE_GUIDANCE.get(normalized_angle, ANGLE_STRUCTURE_GUIDANCE["ideas"])


def choose_persona_name(trend: str, persona_names: list[str]) -> str:
    trend_key = trend.strip().lower()
    if not trend_key:
        return persona_names[0]

    digest = hashlib.sha256(f"persona::{trend_key}".encode("utf-8")).hexdigest()
    index = int(digest, 16) % len(persona_names)
    return persona_names[index]


def resolve_format_prompt(
    trend: str,
    format_name: str | None,
    topic_context: TopicCandidate | None = None,
) -> tuple[str, str]:
    prompts = get_format_prompts()
    available_names = sorted(prompts.keys())

    if format_name:
        normalized = normalize_format_name(format_name)
        if normalized not in prompts:
            valid = ", ".join(available_names)
            raise ValueError(f"Unknown format '{format_name}'. Valid formats: {valid}")
        selected_name = normalized
    else:
        inferred_angle = normalize_identifier((topic_context or {}).get("angle_id", ""))
        angle_format = ANGLE_FORMAT_MAP.get(inferred_angle, "")
        if angle_format and angle_format in prompts:
            selected_name = angle_format
        else:
            selected_name = choose_format_name(trend=trend, format_names=available_names)

    return selected_name, prompts[selected_name]


def resolve_persona_prompt(trend: str, persona_name: str | None) -> tuple[str, str]:
    prompts = get_persona_prompts()
    available_names = sorted(prompts.keys())

    if persona_name:
        normalized = normalize_persona_name(persona_name)
        if normalized not in prompts:
            valid = ", ".join(available_names)
            raise ValueError(f"Unknown persona '{persona_name}'. Valid personas: {valid}")
        selected_name = normalized
    else:
        selected_name = choose_persona_name(trend=trend, persona_names=available_names)

    return selected_name, prompts[selected_name]


def normalize_product(raw: dict[str, Any]) -> Product:
    required_fields = ("title", "affiliate_url", "image_url")
    missing = [field for field in required_fields if field not in raw]
    if missing:
        raise ValueError(f"Product is missing required fields: {', '.join(missing)}")

    short_reason = str(raw.get("short_reason") or raw.get("reason_for_recommendation") or "").strip()
    if not short_reason:
        raise ValueError(
            "Product must include short_reason (or legacy reason_for_recommendation)."
        )

    product: Product = {
        "title": str(raw["title"]).strip(),
        "affiliate_url": str(raw["affiliate_url"]).strip(),
        "image_url": str(raw["image_url"]).strip(),
        "short_reason": short_reason,
        "price": str(raw["price"]).strip() if raw.get("price") is not None else None,
        "source": str(raw.get("source") or "custom").strip(),
        "reason_for_recommendation": short_reason,
    }

    if not product["title"]:
        raise ValueError("Product title cannot be empty.")
    if not product["affiliate_url"].startswith("http"):
        raise ValueError(f"Invalid affiliate_url: {product['affiliate_url']}")
    if not product["image_url"].startswith("http"):
        raise ValueError(f"Invalid image_url: {product['image_url']}")

    return product


def validate_products(products: list[dict[str, Any]] | list[Product]) -> list[Product]:
    normalized_products = [normalize_product(dict(item)) for item in products]

    if not normalized_products:
        return []

    affiliate_urls = [item["affiliate_url"] for item in normalized_products]
    if len(set(affiliate_urls)) != len(affiliate_urls):
        raise ValueError("Each product must have a unique affiliate_url.")

    return normalized_products


def load_products_from_file(products_file: str | None) -> list[Product] | None:
    if not products_file:
        return None

    file_path = Path(products_file)
    if not file_path.exists():
        raise FileNotFoundError(f"Products file not found: {file_path}")

    raw_text = file_path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Products file is not valid JSON: {file_path}") from exc

    if not isinstance(data, list):
        raise ValueError("Products file must contain a JSON array.")

    return validate_products(data)


def build_products_prompt(products: list[Product], monetization_profile: dict[str, Any]) -> str:
    if not products:
        return (
            "Product placement rules:\n"
            "- No validated affiliate products are available for this draft.\n"
            "- Keep all 5 sections editorial-only.\n"
            "- Do not include external URLs.\n"
        )

    serialized = json.dumps(products[:SECTION_COUNT], ensure_ascii=False, indent=2)
    product_count = min(
        len(products),
        int(monetization_profile.get("max_products") or SECTION_COUNT),
        int(monetization_profile.get("max_inline_links") or SECTION_COUNT),
        SECTION_COUNT,
    )
    remaining_sections = SECTION_COUNT - product_count
    section_indexes = resolve_affiliate_section_indexes(monetization_profile, product_count)
    section_labels = ", ".join(str(index) for index in section_indexes)
    return (
        "Product placement rules:\n"
        f"- Monetization profile: {monetization_profile.get('name', 'Editorial Soft')}.\n"
        f"- Profile guidance: {monetization_profile.get('prompt_guidance', '').strip()}\n"
        f"- You have {product_count} validated affiliate products available.\n"
        f"- Use each provided product exactly once in these main section(s): {section_labels}.\n"
        "- Only those selected sections may include an affiliate URL.\n"
        "- Each section may contain at most one affiliate URL.\n"
        f"- Keep the remaining {remaining_sections} section(s) editorial-only with no external links.\n"
        "- In each affiliate-enabled section, add one visible markdown link in the prose only where it fits naturally.\n"
        "- Use the product's exact title as the markdown link anchor text.\n"
        "- Use the product's exact title and exact affiliate_url in markdown links.\n"
        "- Use only the provided products and links.\n"
        "- Do not invent products, product names, or URLs.\n"
        "- Do not include any other external links.\n\n"
        "Provided products (JSON):\n"
        f"{serialized}"
    )


def build_search_strategy_prompt(topic_context: TopicCandidate) -> str:
    secondary_keywords = topic_context["secondary_keywords"][:4]
    cluster_keywords = topic_context["cluster_keywords"][:6]
    secondary_lines = "\n".join(f"- {keyword}" for keyword in secondary_keywords) or "- None provided"
    cluster_lines = "\n".join(f"- {keyword}" for keyword in cluster_keywords) or f"- {topic_context['primary_keyword']}"

    return (
        "Google SEO targeting rules:\n"
        f"- Domain: {topic_context.get('domain_id', '') or 'general decor'}\n"
        f"- Cluster ID: {topic_context.get('cluster_id', '') or topic_context['trend_cluster']}\n"
        f"- Primary keyword: {topic_context['primary_keyword']}\n"
        f"- Search intent: {topic_context['search_intent']}\n"
        f"- Refined intent: {topic_context.get('intent_id', '') or 'not specified'}\n"
        f"- Topical cluster: {topic_context['trend_cluster']}\n"
        f"- Subtopic: {topic_context.get('subtopic_name', '') or topic_context.get('subtopic_id', '') or 'not specified'}\n"
        f"- Angle: {topic_context.get('angle_id', '') or 'not specified'}\n"
        "- Secondary/supporting keywords:\n"
        f"{secondary_lines}\n"
        "- Broader cluster keyword set:\n"
        f"{cluster_lines}\n"
        "- Use the primary keyword naturally in the title, slug, opening paragraphs, and at least one H2.\n"
        "- Let at least 2 main section headings clearly signal the topic cluster or search intent, but keep them readable and editorial rather than formulaic.\n"
        "- Avoid vague H2s like 'Bring in Texture' or 'Choose Better Decor' with no topic context; make headings specific to the room, style, or styling problem being solved.\n"
        "- Weave 2 to 4 supporting keywords naturally into the body copy where they genuinely fit; do not force exact-match phrasing in every section.\n"
        "- If a supporting keyword feels awkward as written, echo its idea in natural language close to the same meaning instead of sounding robotic.\n"
        "- Keep phrasing editorial and human; do not stuff keywords.\n"
        "- Aim to satisfy search intent with practical, specific answers and room-focused guidance.\n"
        "- Add a short FAQ section near the end that answers 3 to 5 realistic search follow-up questions.\n"
    )


def build_intent_prompt(topic_context: TopicCandidate) -> str:
    intent_id = normalize_identifier(topic_context.get("intent_id", "")) or "inspiration"
    guidance_map = {
        "inspiration": {
            "goal": "help readers imagine distinct, attractive ways to approach the look",
            "focus": "surface visually different styling directions, moods, and combinations instead of repeating the same idea five times",
            "heading_style": "Headings should feel evocative and topic-specific, not like generic decor filler.",
        },
        "decision_making": {
            "goal": "help readers decide what fits their room, budget, and constraints best",
            "focus": "highlight tradeoffs, scenarios, and practical differences that make the choice easier",
            "heading_style": "Headings should clarify decisions, options, or tradeoffs in plain language.",
        },
        "problem_solving": {
            "goal": "help readers recognize what is going wrong and fix it clearly",
            "focus": "connect common problems to their causes and give specific corrections that feel realistic",
            "heading_style": "Headings should name problems or corrections clearly without sounding repetitive.",
        },
        "comparison": {
            "goal": "help readers compare strong options and choose the best fit",
            "focus": "make differences, strengths, weaknesses, and use cases easy to scan and trust",
            "heading_style": "Headings should feel criteria-led, recommendation-led, or comparative.",
        },
        "implementation": {
            "goal": "help readers actually execute the look in a usable order",
            "focus": "move through decisions in a practical sequence with enough detail to follow in a real room",
            "heading_style": "Headings should feel actionable, concrete, and process-aware.",
        },
    }
    guidance = guidance_map.get(intent_id, guidance_map["inspiration"])
    return (
        "Intent guidance:\n"
        f"- Intent ID: {intent_id}\n"
        f"- Core job: {guidance['goal']}.\n"
        f"- Editorial focus: {guidance['focus']}.\n"
        f"- Heading guidance: {guidance['heading_style']}\n"
        "- Make the article's purpose obvious enough that it would not be confused with a nearby post in the same cluster.\n"
    )


def build_internal_linking_prompt(topic_context: TopicCandidate, article_title: str = "", article_slug: str = "") -> str:
    suggestions = build_internal_link_suggestions(
        topic_context=topic_context,
        article_title=article_title,
        article_slug=article_slug,
        limit=4,
    )
    if not suggestions:
        return (
            "Internal linking guidance:\n"
            "- No strong architecture-aware matches were found.\n"
            "- If a natural internal link opportunity comes to mind from existing site coverage, keep it light and relevant.\n"
        )

    suggestion_lines = []
    for suggestion in suggestions:
        suggestion_lines.append(
            f"- {suggestion['relationship']}: link to \"{suggestion['title']}\" "
            f"({suggestion['permalink']}) using natural anchor text such as \"{suggestion['anchor_text']}\". "
            f"Why: {suggestion['blurb']}"
        )
    return (
        "Internal linking guidance:\n"
        "- If it fits naturally, include 1 to 3 internal markdown links to existing site articles.\n"
        "- Prioritize one same-cluster or adjacent-subtopic link first, then a complementary angle, then a related-cluster link if it improves the reader journey.\n"
        "- Use only the provided internal URLs. Do not invent site URLs.\n"
        "- Keep anchor text varied, specific, and editorial rather than repetitive exact-match phrases.\n"
        "- Do not force links into every section; only use them where they genuinely help the reader.\n"
        f"{chr(10).join(suggestion_lines)}\n"
    )


def build_brand_voice_prompt() -> str:
    guide = load_brand_voice_guide()
    brand_name = str(guide.get("brand_name") or "The Livin' Edit").strip()
    editorial_positioning = str(guide.get("editorial_positioning") or "").strip()
    tone = guide.get("tone", {}) if isinstance(guide.get("tone"), dict) else {}
    vocabulary = guide.get("vocabulary", {}) if isinstance(guide.get("vocabulary"), dict) else {}
    headline_style = guide.get("headline_style", {}) if isinstance(guide.get("headline_style"), dict) else {}
    intro_style = guide.get("intro_style", {}) if isinstance(guide.get("intro_style"), dict) else {}
    conclusion_style = guide.get("conclusion_style", {}) if isinstance(guide.get("conclusion_style"), dict) else {}
    point_of_view = guide.get("editorial_point_of_view", {}) if isinstance(guide.get("editorial_point_of_view"), dict) else {}
    variation_rules = guide.get("variation_rules", {}) if isinstance(guide.get("variation_rules"), dict) else {}

    def bullet_lines(values: list[Any]) -> str:
        cleaned = [f"- {str(item).strip()}" for item in values if str(item).strip()]
        return "\n".join(cleaned) or "- None specified"

    return (
        "Publication brand voice rules:\n"
        f"- Brand: {brand_name}\n"
        f"- Editorial positioning: {editorial_positioning}\n"
        "- Core tone traits:\n"
        f"{bullet_lines(list(tone.get('core_traits', [])))}\n"
        "- The writing should feel like:\n"
        f"{bullet_lines(list(tone.get('should_feel_like', [])))}\n"
        "- The writing should not feel like:\n"
        f"{bullet_lines(list(tone.get('should_not_feel_like', [])))}\n"
        "- Headline style:\n"
        f"- {str(headline_style.get('description') or '').strip()}\n"
        f"{bullet_lines(list(headline_style.get('prefer', [])))}\n"
        "- Intro style:\n"
        f"- {str(intro_style.get('description') or '').strip()}\n"
        f"{bullet_lines(list(intro_style.get('must_do', [])))}\n"
        "- Conclusion style:\n"
        f"- {str(conclusion_style.get('description') or '').strip()}\n"
        f"{bullet_lines(list(conclusion_style.get('prefer', [])))}\n"
        "- Prefer vocabulary and ideas like:\n"
        f"{bullet_lines(list(vocabulary.get('prefer', [])))}\n"
        "- Avoid phrases and tones like:\n"
        f"{bullet_lines(list(vocabulary.get('avoid', [])) + list(headline_style.get('avoid', [])) + list(intro_style.get('avoid', [])) + list(conclusion_style.get('avoid', [])))}\n"
        "- Recurring editorial point of view:\n"
        f"{bullet_lines(list(point_of_view.get('principles', [])))}\n"
        "- Variation rules:\n"
        f"- {str(variation_rules.get('description') or '').strip()}\n"
        f"{bullet_lines(list(variation_rules.get('rules', [])))}\n"
        "- Apply this voice consistently, but do not repeat signature phrases mechanically.\n"
        "- Let the voice shape the title, intro, and conclusion most clearly while still fitting the article's search intent and angle.\n"
    )


def build_angle_structure_prompt(topic_context: TopicCandidate) -> str:
    angle_id = normalize_identifier(topic_context.get("angle_id", "")) or "ideas"
    structure = resolve_angle_structure(angle_id)
    outline_lines = "\n".join(f"- {line}" for line in structure["outline"])
    return (
        "Angle-sensitive structure rules:\n"
        f"- Angle ID: {angle_id}\n"
        f"- Treat this as a {structure['name']} piece with its own editorial rhythm.\n"
        "- Keep the overall markdown shape compatible with the pipeline: plain introduction, exactly 5 H2 main sections, FAQ, and conclusion.\n"
        "- Do not make the article feel templated; use the following section rhythm as editorial guidance, not mechanical labels.\n"
        f"{outline_lines}\n"
        f"- Heading style: {structure['heading_style']}\n"
        f"- FAQ style: {structure['faq_style']}\n"
        "- Let the intro, H2 sequence, and conclusion reflect the angle clearly enough that a reader can feel the difference from other article types.\n"
    )


def strip_code_fences(text: str) -> str:
    match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL)
    return match.group(1).strip() if match else text.strip()


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def extract_main_headings(article_markdown: str) -> list[str]:
    headings: list[str] = []
    for match in re.finditer(r"(?m)^##\s+(.+)$", article_markdown):
        full_heading = match.group(0)
        if (
            is_intro_section_heading(full_heading)
            or is_conclusion_section_heading(full_heading)
            or is_faq_section_heading(full_heading)
        ):
            continue
        headings.append(match.group(1).strip())
    return headings


def extract_urls(text: str) -> set[str]:
    matches = re.findall(r"https?://[^\s\]\)\>\"']+", text)
    cleaned = {match.rstrip(".,;:") for match in matches}
    return cleaned


def count_provided_url_occurrences(text: str, provided_urls: set[str]) -> int:
    count = 0
    for url in provided_urls:
        count += text.count(url)
    return count


def count_markdown_links(text: str) -> int:
    return len(re.findall(r"\[[^\]]+\]\(https?://[^)]+\)", text))


def strip_provided_links_from_text(text: str, provided_urls: set[str]) -> str:
    cleaned = text
    for url in provided_urls:
        escaped_url = re.escape(url)
        cleaned = re.sub(rf"\[([^\]]+)\]\({escaped_url}\)", r"\1", cleaned)
        cleaned = cleaned.replace(url, "")
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def build_affiliate_sentence(product: Product, monetization_profile: dict[str, Any], section_index: int) -> str:
    reason = str(product.get("short_reason") or product.get("reason_for_recommendation") or "").strip()
    templates = monetization_profile.get("inline_sentence_templates", [])
    template_index = 0
    if templates:
        template_index = (max(section_index, 1) - 1) % len(templates)
        template = str(templates[template_index]).strip()
        sentence = template.format(title=product["title"], affiliate_url=product["affiliate_url"])
    else:
        sentence = f"A practical option here is [{product['title']}]({product['affiliate_url']})."

    if reason:
        reason = reason[:1].lower() + reason[1:]
        if sentence.endswith("."):
            sentence = sentence[:-1]
        sentence = f"{sentence}, and {reason}."

    return sentence


def repair_affiliate_links(
    article_markdown: str,
    products: list[Product],
    monetization_profile: dict[str, Any],
) -> str:
    provided_urls = {item["affiliate_url"] for item in products[:SECTION_COUNT]}
    split_parts = re.split(r"(?m)(^##\s+.+$)", article_markdown)
    if len(split_parts) < 3:
        return article_markdown

    repaired_parts: list[str] = [strip_provided_links_from_text(split_parts[0], provided_urls)]
    body_section_index = 0
    expected_product_count = min(
        len(products),
        int(monetization_profile.get("max_products") or SECTION_COUNT),
        int(monetization_profile.get("max_inline_links") or SECTION_COUNT),
        SECTION_COUNT,
    )
    target_section_indexes = set(resolve_affiliate_section_indexes(monetization_profile, expected_product_count))
    product_lookup = {
        section_index: products[offset]
        for offset, section_index in enumerate(sorted(target_section_indexes))
        if offset < expected_product_count
    }

    for index in range(1, len(split_parts), 2):
        heading = split_parts[index]
        body = split_parts[index + 1] if index + 1 < len(split_parts) else ""
        cleaned_body = strip_provided_links_from_text(body, provided_urls)

        repaired_parts.append(heading)
        if is_intro_section_heading(heading) or is_conclusion_section_heading(heading):
            repaired_parts.append(f"\n{cleaned_body}")
            continue

        section_number = body_section_index + 1
        if section_number in target_section_indexes and section_number in product_lookup:
            affiliate_sentence = build_affiliate_sentence(
                product_lookup[section_number],
                monetization_profile=monetization_profile,
                section_index=section_number,
            )
            cleaned_body = f"{cleaned_body.rstrip()}\n\n{affiliate_sentence}"

        repaired_parts.append(f"\n{cleaned_body}")
        body_section_index += 1

    repaired_markdown = "".join(repaired_parts).strip()
    return repaired_markdown


def normalize_section_heading(heading_text: str) -> str:
    cleaned = re.sub(r"^##\s+", "", heading_text).strip().lower()
    cleaned = re.sub(r"^[0-9]+[\.\)]\s*", "", cleaned)
    return re.sub(r"\s+", " ", cleaned)


def is_intro_section_heading(heading_text: str) -> bool:
    normalized = normalize_section_heading(heading_text)
    return normalized in {"introduction", "intro"} or normalized.startswith("introduction ")


def is_conclusion_section_heading(heading_text: str) -> bool:
    normalized = normalize_section_heading(heading_text)
    return normalized in {"conclusion", "final thoughts", "closing thoughts", "wrap-up"} or normalized.startswith(
        "conclusion "
    )


def is_faq_section_heading(heading_text: str) -> bool:
    normalized = normalize_section_heading(heading_text)
    return normalized in {"faq", "frequently asked questions"} or normalized.startswith("faq ")


def split_main_sections(article_markdown: str) -> list[str]:
    heading_matches = [
        match
        for match in re.finditer(r"(?m)^##\s+.+$", article_markdown)
        if not is_intro_section_heading(match.group(0))
        and not is_conclusion_section_heading(match.group(0))
        and not is_faq_section_heading(match.group(0))
    ]
    if len(heading_matches) < SECTION_COUNT:
        return []

    sections: list[str] = []
    for index in range(SECTION_COUNT):
        start = heading_matches[index].start()
        if index + 1 < len(heading_matches):
            end = heading_matches[index + 1].start()
        else:
            end = len(article_markdown)
        sections.append(article_markdown[start:end])

    return sections



def normalize_string_list(value: Any, field_name: str, expected_count: int) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list.")

    items: list[str] = []
    for entry in value:
        if isinstance(entry, str):
            text = entry.strip()
        elif isinstance(entry, dict) and isinstance(entry.get("prompt"), str):
            text = entry["prompt"].strip()
        else:
            text = str(entry).strip()

        if text:
            items.append(text)

    if len(items) != expected_count:
        raise ValueError(f"{field_name} must contain exactly {expected_count} items.")

    return items


def ensure_image_prompt_constraints(prompt: str) -> str:
    required_chunks = [
        "editorial interior photography",
        "natural daylight",
        "realistic materials",
        "no text",
        "no logos",
        "no people",
    ]

    result = prompt.strip()
    lower = result.lower()
    missing = [chunk for chunk in required_chunks if chunk not in lower]
    if missing:
        result = f"{result}. Editorial interior photography, natural daylight, realistic materials, no text, no logos, no people."

    return result


def build_estimated_reading_time(word_count: int) -> str:
    minutes = max(1, (word_count + 199) // 200)
    return f"{minutes} min read"


def has_faq_section(article_markdown: str) -> bool:
    return bool(
        re.search(
            r"(?im)^##\s+(faq|frequently asked questions)\s*$",
            article_markdown,
        )
    )


class ArticleLengthError(ValueError):
    def __init__(self, word_count: int, min_words: int, max_words: int) -> None:
        super().__init__(
            f"article_markdown word count ({word_count}) is outside target range "
            f"{min_words}-{max_words}."
        )
        self.word_count = word_count


class ProductLinkError(ValueError):
    pass


def validate_angle_structure(article_markdown: str, topic_context: TopicCandidate) -> None:
    angle_id = normalize_topic_text(topic_context.get("angle_id", "")) or "ideas"
    headings = extract_main_headings(article_markdown)
    heading_text = " ".join(headings)
    body_text = normalize_topic_text(article_markdown)
    combined_text = f"{normalize_topic_text(heading_text)} {body_text}"

    signal_sets = {
        "ideas": {"idea", "palette", "look", "style", "layout", "layer", "mix"},
        "how_to": {"how", "choose", "style", "layer", "place", "balance", "start", "use"},
        "mistakes": {"mistake", "mistakes", "avoid", "fix", "fixes", "wrong", "problem"},
        "best_options": {"best", "choose", "right", "options", "option", "for", "fit", "compare"},
    }
    required_matches = {
        "ideas": 3,
        "how_to": 4,
        "mistakes": 4,
        "best_options": 4,
    }

    signals = signal_sets.get(angle_id)
    if not signals:
        return

    signal_hits = sum(1 for signal in signals if f" {signal} " in f" {combined_text} ")
    if signal_hits < required_matches.get(angle_id, 3):
        raise ValueError(
            f"article_markdown does not reflect the '{angle_id}' structure strongly enough."
        )

    if angle_id == "mistakes":
        mistake_like_headings = sum(
            1
            for heading in headings
            if any(token in normalize_topic_text(heading).split() for token in {"mistake", "avoid", "fix", "problem"})
        )
        if mistake_like_headings < 2:
            raise ValueError(
                "Mistakes articles should signal the mistake/fix pattern in at least two main section headings."
            )

    if angle_id == "how_to":
        action_headings = sum(
            1
            for heading in headings
            if any(token in normalize_topic_text(heading).split() for token in {"how", "choose", "style", "layer", "place", "balance"})
        )
        if action_headings < 2:
            raise ValueError(
                "How-to articles should use action-led H2s in at least two main sections."
            )

    if angle_id == "best_options":
        selection_headings = sum(
            1
            for heading in headings
            if any(token in normalize_topic_text(heading).split() for token in {"best", "choose", "right", "fit", "options"})
        )
        if selection_headings < 2:
            raise ValueError(
                "Best-options articles should signal selection or comparison language in at least two main section headings."
            )


def build_short_retry_instruction(retry_number: int, word_count: int) -> str:
    if retry_number == 1:
        extra_strength_instruction = (
            "Expand each main section with more concrete guidance, including small layout or styling "
            "examples readers can apply immediately."
        )
    else:
        extra_strength_instruction = (
            "Increase depth further: in every main section add at least two practical, specific "
            "implementation tips and clearer visual reasoning."
        )

    return SHORT_RETRY_PROMPT_TEMPLATE.format(
        word_count=word_count,
        min_words=MIN_WORDS,
        max_words=MAX_WORDS,
        retry_number=retry_number,
        retry_limit=SHORT_RETRY_LIMIT,
        extra_strength_instruction=extra_strength_instruction,
    )


def request_article_json(
    client: OpenAI,
    trend: str,
    model: str,
    article_template: str,
    topic_context: TopicCandidate,
    persona_name: str,
    persona_prompt: str,
    format_name: str,
    format_prompt: str,
    products: list[Product],
    monetization_profile: dict[str, Any],
    extra_instruction: str | None = None,
) -> dict[str, Any]:
    try:
        article_prompt = article_template.format(trend=trend)
    except KeyError as exc:
        raise RuntimeError(f"Missing template placeholder value: {exc}") from exc

    user_prompt = (
        f"Use this writing persona ({persona_name}):\n"
        f"{persona_prompt}\n\n"
        f"{article_prompt}\n\n"
        f"{build_brand_voice_prompt()}\n\n"
        f"{build_search_strategy_prompt(topic_context)}\n\n"
        f"{build_intent_prompt(topic_context)}\n\n"
        f"{build_internal_linking_prompt(topic_context)}\n\n"
        f"{build_angle_structure_prompt(topic_context)}\n\n"
        f"Use this article format template ({format_name}):\n"
        f"{format_prompt}\n\n"
        f"{build_products_prompt(products, monetization_profile=monetization_profile)}\n\n"
        f"{OUTPUT_REQUIREMENTS_PROMPT}"
    )

    if extra_instruction:
        user_prompt = f"{user_prompt}\n\n{extra_instruction}"

    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )

    raw_text = (response.output_text or "").strip()
    if not raw_text:
        raise RuntimeError("OpenAI returned an empty response.")

    json_text = strip_code_fences(raw_text)
    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("OpenAI response was not valid JSON.") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("OpenAI JSON response must be an object.")

    return payload


def validate_affiliate_link_usage(
    article_markdown: str,
    products: list[Product],
    monetization_profile: dict[str, Any],
) -> None:
    expected_product_count = min(
        len(products),
        int(monetization_profile.get("max_products") or SECTION_COUNT),
        int(monetization_profile.get("max_inline_links") or SECTION_COUNT),
        SECTION_COUNT,
    )
    provided_urls = {item["affiliate_url"] for item in products[:SECTION_COUNT]}
    article_urls = extract_urls(article_markdown)

    unexpected_urls = sorted(url for url in article_urls if url not in provided_urls)
    if unexpected_urls:
        unexpected = ", ".join(unexpected_urls)
        raise ProductLinkError(f"article_markdown contains unexpected URLs: {unexpected}")

    main_sections = split_main_sections(article_markdown)
    if len(main_sections) != SECTION_COUNT:
        raise ProductLinkError(
            "article_markdown must include exactly 5 main sections as markdown H2 headings."
        )

    if expected_product_count == 0:
        if article_urls:
            raise ProductLinkError("article_markdown cannot include affiliate URLs when no products are available.")
        return

    target_sections = set(resolve_affiliate_section_indexes(monetization_profile, expected_product_count))
    used_urls_by_section: list[str] = []
    sections_with_links = 0

    for index, section_text in enumerate(main_sections, start=1):
        url_count = count_provided_url_occurrences(section_text, provided_urls)
        if url_count > 1:
            raise ProductLinkError(
                "Each main section may contain at most one provided affiliate URL."
            )

        if url_count == 1:
            if index not in target_sections:
                allowed = ", ".join(str(item) for item in sorted(target_sections))
                raise ProductLinkError(
                    f"affiliate links are only allowed in section(s) {allowed} for this monetization profile."
                )
            sections_with_links += 1
            section_urls = [url for url in provided_urls if url in section_text]
            used_urls_by_section.extend(section_urls)

    distinct_urls = set(used_urls_by_section)
    if sections_with_links != expected_product_count:
        raise ProductLinkError(
            f"article_markdown must contain affiliate URLs in exactly {expected_product_count} section(s)."
        )

    if len(distinct_urls) != expected_product_count:
        raise ProductLinkError(
            "Each affiliate-enabled section must use a different provided product affiliate URL."
        )

    total_occurrences = count_provided_url_occurrences(article_markdown, provided_urls)
    if total_occurrences != expected_product_count:
        raise ProductLinkError(
            "article_markdown must use each provided affiliate URL exactly once and nowhere outside the selected sections."
        )


def normalize_and_validate(
    payload: dict[str, Any],
    products: list[Product],
    topic_context: TopicCandidate,
    monetization_profile: dict[str, Any],
) -> dict[str, Any]:
    required_fields = {
        "title",
        "slug",
        "meta_description",
        "keywords",
        "hero_image_prompt",
        "section_image_prompts",
        "pinterest_titles",
        "pinterest_descriptions",
        "article_markdown",
    }
    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    raw_title = normalize_title(
        str(payload["title"]).strip(),
        primary_keyword=topic_context["primary_keyword"],
        angle=str(topic_context.get("angle_id", "")),
    )
    title_set = choose_title_set(
        current_title=raw_title,
        primary_keyword=topic_context["primary_keyword"],
        angle_id=str(topic_context.get("angle_id", "")),
        intent_id=str(topic_context.get("intent_id", "")),
        cluster_name=topic_context["trend_cluster"],
        subtopic_name=str(topic_context.get("subtopic_name", "")),
        cluster_id=str(topic_context.get("cluster_id", "")),
    )
    title = title_set["display_title"]
    seo_title = title_set["seo_title"]
    slug = str(payload["slug"]).strip()
    meta_description = str(payload["meta_description"]).strip()
    article_markdown = str(payload["article_markdown"]).strip()

    keywords_raw = payload["keywords"]
    if isinstance(keywords_raw, list):
        keywords = [str(item).strip() for item in keywords_raw if str(item).strip()]
    elif isinstance(keywords_raw, str):
        keywords = [item.strip() for item in keywords_raw.split(",") if item.strip()]
    else:
        raise ValueError("keywords must be a list of strings or a comma-separated string.")

    if not title:
        raise ValueError("title cannot be empty.")
    if not meta_description:
        raise ValueError("meta_description cannot be empty.")
    if not article_markdown:
        raise ValueError("article_markdown cannot be empty.")
    if not keywords:
        raise ValueError("keywords cannot be empty.")

    if not slug:
        slug = slugify(title)
    else:
        slug = slugify(normalize_title(slug, primary_keyword=topic_context["primary_keyword"], angle=str(topic_context.get("angle_id", ""))))

    word_count = count_words(article_markdown)
    if word_count < MIN_WORDS or word_count > MAX_WORDS:
        raise ArticleLengthError(word_count=word_count, min_words=MIN_WORDS, max_words=MAX_WORDS)

    if not has_faq_section(article_markdown):
        raise ValueError("article_markdown must include a short FAQ section near the end.")

    validate_affiliate_link_usage(
        article_markdown=article_markdown,
        products=products,
        monetization_profile=monetization_profile,
    )
    validate_angle_structure(article_markdown=article_markdown, topic_context=topic_context)

    estimated_reading_time_raw = str(payload.get("estimated_reading_time", "")).strip()
    estimated_reading_time = estimated_reading_time_raw or build_estimated_reading_time(word_count)

    section_headings = extract_main_headings(article_markdown)
    if len(section_headings) != SECTION_COUNT:
        raise ValueError("article_markdown must include exactly 5 H2 main sections.")

    generated_image_prompt_package = generate_image_prompts(
        title=title,
        section_headings=section_headings,
        cluster=topic_context["trend_cluster"],
        cluster_id=str(topic_context.get("cluster_id", "")),
        primary_keyword=topic_context["primary_keyword"],
        angle=str(topic_context.get("angle_id", "")),
        intent=str(topic_context.get("intent_id", "")),
        season=topic_context["season"],
    )
    hero_image_prompt = ensure_image_prompt_constraints(
        str(generated_image_prompt_package["hero_image_prompt"]).strip()
    )
    section_image_prompts = [
        ensure_image_prompt_constraints(item)
        for item in generated_image_prompt_package["section_image_prompts"]
    ]

    pinterest_titles = normalize_string_list(
        payload["pinterest_titles"],
        field_name="pinterest_titles",
        expected_count=PINTEREST_ITEM_COUNT,
    )
    pinterest_descriptions = normalize_string_list(
        payload["pinterest_descriptions"],
        field_name="pinterest_descriptions",
        expected_count=PINTEREST_ITEM_COUNT,
    )
    internal_link_suggestions = build_internal_link_suggestions(
        topic_context=topic_context,
        article_title=title,
        article_slug=slug,
        limit=4,
    )

    return {
        "title": title,
        "seo_title": seo_title,
        "title_family": title_set["title_family"],
        "seo_title_family": title_set["seo_title_family"],
        "title_candidates": title_set["candidates"],
        "slug": slug,
        "meta_description": meta_description,
        "keywords": keywords,
        "affiliate_products": products[:SECTION_COUNT],
        "monetization_profile": monetization_profile,
        "primary_keyword": topic_context["primary_keyword"],
        "secondary_keywords": topic_context["secondary_keywords"],
        "topical_cluster": topic_context["trend_cluster"],
        "cluster_keywords": topic_context["cluster_keywords"],
        "search_intent": topic_context["search_intent"],
        "intent_id": str(topic_context.get("intent_id", "")),
        "domain_id": str(topic_context.get("domain_id", "")),
        "cluster_id": str(topic_context.get("cluster_id", "")),
        "subtopic_id": str(topic_context.get("subtopic_id", "")),
        "subtopic_name": str(topic_context.get("subtopic_name", "")),
        "angle_id": str(topic_context.get("angle_id", "")),
        "modifier": str(topic_context.get("modifier", "")),
        "internal_link_suggestions": internal_link_suggestions,
        "estimated_reading_time": estimated_reading_time,
        "hero_image_prompt": hero_image_prompt,
        "section_image_prompts": section_image_prompts,
        "visual_direction": generated_image_prompt_package["visual_direction"],
        "image_prompt_diagnostics": generated_image_prompt_package.get("image_prompt_diagnostics", {}),
        "pinterest_titles": pinterest_titles,
        "pinterest_descriptions": pinterest_descriptions,
        "article_markdown": article_markdown,
    }


def generate_article_package(
    client: OpenAI,
    trend: str,
    model: str,
    format_name: str | None = None,
    persona_name: str | None = None,
    topic_context: TopicCandidate | dict[str, Any] | None = None,
    products: list[Product] | None = None,
) -> dict[str, Any]:
    package, _ = generate_article_package_with_report(
        client=client,
        trend=trend,
        model=model,
        format_name=format_name,
        persona_name=persona_name,
        topic_context=topic_context,
        products=products,
    )
    return package


def generate_article_package_with_report(
    client: OpenAI,
    trend: str,
    model: str,
    format_name: str | None = None,
    persona_name: str | None = None,
    topic_context: TopicCandidate | dict[str, Any] | None = None,
    products: list[Product] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    last_error: Exception | None = None
    extra_instruction: str | None = None
    short_retry_count = 0
    product_retry_count = 0
    generation_calls = 0
    normalized_topic_context = normalize_topic_candidate(topic_context=topic_context, trend=trend)
    monetization_profile = resolve_monetization_profile(
        angle_id=str(normalized_topic_context.get("angle_id", "")),
        intent_id=str(normalized_topic_context.get("intent_id", "")),
    )

    selected_format_name, selected_format_prompt = resolve_format_prompt(
        trend=trend,
        format_name=format_name,
        topic_context=normalized_topic_context,
    )
    selected_persona_name, selected_persona_prompt = resolve_persona_prompt(
        trend=trend,
        persona_name=persona_name,
    )

    selected_products = (
        products
        if products is not None
        else fetch_products_for_trend(trend=trend, limit=SECTION_COUNT)
    )
    selected_products = validate_products(selected_products)
    selected_products = limit_products_for_profile(selected_products, monetization_profile)
    try:
        concept_validation = ensure_valid_article_concept(normalized_topic_context)
    except ConceptValidationError as exc:
        raise RuntimeError(f"Article concept validation failed: {exc}") from exc
    if concept_validation["compatibility_class"] in {"valid_with_constraints", "soft_warn"}:
        warning_text = "; ".join(concept_validation["warnings"]) or "Context-sensitive concept."
        print(
            f"[article][compatibility][{concept_validation['compatibility_class']}] "
            f"{normalized_topic_context['primary_keyword']}: {warning_text}"
        )

    minimum_products = int(monetization_profile.get("min_products_to_enable") or 0)
    if len(selected_products) < minimum_products:
        print(
            f"[article] affiliate mode disabled: only {len(selected_products)} validated product(s) available "
            f"(minimum {minimum_products} for {monetization_profile.get('profile_id', 'editorial_soft')})"
        )
        selected_products = []
    else:
        print(
            f"[article] affiliate mode enabled with {len(selected_products)} product(s) "
            f"using {monetization_profile.get('profile_id', 'editorial_soft')}"
        )

    article_template = load_article_template()
    _, cache_path = build_cache_artifacts(
        trend=trend,
        model=model,
        format_name=selected_format_name,
        persona_name=selected_persona_name,
        article_template=article_template,
        format_prompt=selected_format_prompt,
        persona_prompt=selected_persona_prompt,
        topic_context=normalized_topic_context,
        products=selected_products,
        monetization_profile=monetization_profile,
    )
    cached_package = load_cached_article_package(
        cache_path=cache_path,
        products=selected_products,
        topic_context=normalized_topic_context,
        monetization_profile=monetization_profile,
    )
    if cached_package:
        cached_link_count = count_markdown_links(cached_package["article_markdown"])
        print(f"[article] cache hit: {cache_path.name}")
        print(f"[article] primary keyword: {normalized_topic_context['primary_keyword']}")
        print(f"[article] generated markdown visible affiliate links: {cached_link_count}")
        return cached_package, {
            "cache_hit": True,
            "cache_path": str(cache_path),
            "model": model,
            "generation_calls": 0,
            "short_retries": 0,
             "product_retries": 0,
             "selected_products": len(selected_products),
             "affiliate_mode": bool(selected_products),
             "monetization_profile_id": monetization_profile.get("profile_id", "editorial_soft"),
             "generated_link_count": cached_link_count,
            "primary_keyword": normalized_topic_context["primary_keyword"],
            "topical_cluster": normalized_topic_context["trend_cluster"],
        }

    print(f"[article] cache miss: {cache_path.name}")
    print(f"[article] primary keyword: {normalized_topic_context['primary_keyword']}")

    # One initial draft plus retries for short drafts or product-link rule failures.
    for _ in range(1 + SHORT_RETRY_LIMIT + PRODUCT_RETRY_LIMIT):
        try:
            generation_calls += 1
            payload = request_article_json(
                client=client,
                trend=trend,
                model=model,
                article_template=article_template,
                topic_context=normalized_topic_context,
                persona_name=selected_persona_name,
                persona_prompt=selected_persona_prompt,
                format_name=selected_format_name,
                format_prompt=selected_format_prompt,
                products=selected_products,
                monetization_profile=monetization_profile,
                extra_instruction=extra_instruction,
            )
            try:
                normalized_package = normalize_and_validate(
                    payload,
                    products=selected_products,
                    topic_context=normalized_topic_context,
                    monetization_profile=monetization_profile,
                )
            except ProductLinkError:
                if selected_products and isinstance(payload.get("article_markdown"), str):
                    repaired_payload = dict(payload)
                    repaired_payload["article_markdown"] = repair_affiliate_links(
                        article_markdown=str(payload["article_markdown"]),
                        products=selected_products,
                        monetization_profile=monetization_profile,
                    )
                    print("[article] attempted automatic affiliate link repair")
                    normalized_package = normalize_and_validate(
                        repaired_payload,
                        products=selected_products,
                        topic_context=normalized_topic_context,
                        monetization_profile=monetization_profile,
                    )
                else:
                    raise
            generated_link_count = count_markdown_links(normalized_package["article_markdown"])
            print(f"[article] generated markdown visible affiliate links: {generated_link_count}")
            save_cached_article_package(cache_path=cache_path, package=normalized_package)
            return normalized_package, {
                "cache_hit": False,
                "cache_path": str(cache_path),
                "model": model,
                "generation_calls": generation_calls,
                "short_retries": short_retry_count,
                "product_retries": product_retry_count,
                "selected_products": len(selected_products),
                "affiliate_mode": bool(selected_products),
                "monetization_profile_id": monetization_profile.get("profile_id", "editorial_soft"),
                "generated_link_count": generated_link_count,
                "primary_keyword": normalized_topic_context["primary_keyword"],
                "topical_cluster": normalized_topic_context["trend_cluster"],
            }
        except ArticleLengthError as exc:
            if exc.word_count < MIN_WORDS and short_retry_count < SHORT_RETRY_LIMIT:
                short_retry_count += 1
                extra_instruction = build_short_retry_instruction(
                    retry_number=short_retry_count,
                    word_count=exc.word_count,
                )
                last_error = exc
                continue
            last_error = exc
        except ProductLinkError as exc:
            print(f"[article] affiliate validation failed: {exc}")
            if product_retry_count < PRODUCT_RETRY_LIMIT:
                product_retry_count += 1
                extra_instruction = PRODUCT_RETRY_PROMPT_TEMPLATE
                last_error = exc
                continue
            if selected_products and len(selected_products) >= minimum_products:
                print(
                    "[article] falling back to editorial-only mode after affiliate validation failures"
                )
                selected_products = []
                product_retry_count = 0
                monetization_profile = resolve_monetization_profile(value={"profile_id": "editorial_soft"})
                extra_instruction = (
                    "The affiliate placement draft could not be validated. Rewrite the full article package "
                    "in editorial-only mode with no external URLs and no product links. Keep the same trend focus and structure."
                )
                _, cache_path = build_cache_artifacts(
                    trend=trend,
                    model=model,
                    format_name=selected_format_name,
                    persona_name=selected_persona_name,
                    article_template=article_template,
                    format_prompt=selected_format_prompt,
                    persona_prompt=selected_persona_prompt,
                    topic_context=normalized_topic_context,
                    products=selected_products,
                    monetization_profile=monetization_profile,
                )
                cached_editorial_package = load_cached_article_package(
                    cache_path=cache_path,
                    products=selected_products,
                    topic_context=normalized_topic_context,
                    monetization_profile=monetization_profile,
                )
                if cached_editorial_package:
                    cached_link_count = count_markdown_links(cached_editorial_package["article_markdown"])
                    print(f"[article] cache hit after editorial fallback: {cache_path.name}")
                    print(f"[article] generated markdown visible affiliate links: {cached_link_count}")
                    return cached_editorial_package, {
                        "cache_hit": True,
                        "cache_path": str(cache_path),
                        "model": model,
                        "generation_calls": generation_calls,
                        "short_retries": short_retry_count,
                        "product_retries": product_retry_count,
                        "selected_products": len(selected_products),
                        "affiliate_mode": bool(selected_products),
                        "monetization_profile_id": monetization_profile.get("profile_id", "editorial_soft"),
                        "generated_link_count": cached_link_count,
                        "primary_keyword": normalized_topic_context["primary_keyword"],
                        "topical_cluster": normalized_topic_context["trend_cluster"],
                    }
                last_error = exc
                continue
            last_error = exc
        except (RuntimeError, ValueError) as exc:
            last_error = exc

    raise RuntimeError(f"Failed to generate a valid article package: {last_error}")

def main() -> int:
    args = parse_args()
    trend = args.trend.strip()
    if not trend:
        print("Error: trend cannot be empty.", file=sys.stderr)
        return 1

    try:
        api_key = load_openai_api_key()
        client = OpenAI(api_key=api_key)
        products = load_products_from_file(args.products_file)
        result = generate_article_package(
            client=client,
            trend=trend,
            model=args.model,
            format_name=args.format_name,
            persona_name=args.persona_name,
            products=products,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())





