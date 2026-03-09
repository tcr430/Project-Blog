from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

from fetch_products import Product, fetch_products_for_trend


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
- Include: introduction, exactly 5 main sections, and conclusion.
- Use markdown headings for structure.
- Keep tone editorial, practical, and human.
- Avoid fake data, fake citations, and keyword stuffing.
- Do not include any text outside the JSON.
""".strip()


ARTICLE_TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "prompts" / "article_template.md"

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

Additional instruction for this retry:
{extra_strength_instruction}

Do not add filler. Add useful, concrete depth.
Return only the required JSON object.
""".strip()


PRODUCT_RETRY_PROMPT_TEMPLATE = """
The previous draft did not follow the product-link rules.
Rewrite the full article package and follow these rules exactly:
- Use one provided product per main section (5 sections total).
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
    "styling advice": "styling_advice.md",
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
SHORT_RETRY_LIMIT = 2
PRODUCT_RETRY_LIMIT = 2
PINTEREST_ITEM_COUNT = 5
# Preferred guidance range is 1000-1300 words. Hard validation uses MIN_WORDS/MAX_WORDS.


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


def load_article_template() -> str:
    if not ARTICLE_TEMPLATE_PATH.exists():
        raise RuntimeError(f"Article prompt template not found: {ARTICLE_TEMPLATE_PATH}")

    content = ARTICLE_TEMPLATE_PATH.read_text(encoding="utf-8").strip()
    if not content:
        raise RuntimeError(f"Article prompt template is empty: {ARTICLE_TEMPLATE_PATH}")

    return content


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


def choose_persona_name(trend: str, persona_names: list[str]) -> str:
    trend_key = trend.strip().lower()
    if not trend_key:
        return persona_names[0]

    digest = hashlib.sha256(f"persona::{trend_key}".encode("utf-8")).hexdigest()
    index = int(digest, 16) % len(persona_names)
    return persona_names[index]


def resolve_format_prompt(trend: str, format_name: str | None) -> tuple[str, str]:
    prompts = get_format_prompts()
    available_names = sorted(prompts.keys())

    if format_name:
        normalized = normalize_format_name(format_name)
        if normalized not in prompts:
            valid = ", ".join(available_names)
            raise ValueError(f"Unknown format '{format_name}'. Valid formats: {valid}")
        selected_name = normalized
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

    if len(normalized_products) < SECTION_COUNT:
        raise ValueError(
            f"At least {SECTION_COUNT} products are required for one product per section."
        )

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


def build_products_prompt(products: list[Product]) -> str:
    serialized = json.dumps(products[:SECTION_COUNT], ensure_ascii=False, indent=2)
    return (
        "Product placement rules:\n"
        f"- You must use exactly one provided product in each of the {SECTION_COUNT} main sections.\n"
        "- Use the product's exact title and exact affiliate_url in markdown links.\n"
        "- Use only the provided products and links.\n"
        "- Do not invent products, product names, or URLs.\n"
        "- Do not include any other external links.\n\n"
        "Provided products (JSON):\n"
        f"{serialized}"
    )


def strip_code_fences(text: str) -> str:
    match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL)
    return match.group(1).strip() if match else text.strip()


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def extract_urls(text: str) -> set[str]:
    matches = re.findall(r"https?://[^\s\]\)\>\"']+", text)
    cleaned = {match.rstrip(".,;:") for match in matches}
    return cleaned
def count_provided_url_occurrences(text: str, provided_urls: set[str]) -> int:
    count = 0
    for url in provided_urls:
        count += text.count(url)
    return count


def split_main_sections(article_markdown: str) -> list[str]:
    heading_matches = list(re.finditer(r"(?m)^##\s+.+$", article_markdown))
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


class ArticleLengthError(ValueError):
    def __init__(self, word_count: int, min_words: int, max_words: int) -> None:
        super().__init__(
            f"article_markdown word count ({word_count}) is outside target range "
            f"{min_words}-{max_words}."
        )
        self.word_count = word_count


class ProductLinkError(ValueError):
    pass


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
    persona_name: str,
    persona_prompt: str,
    format_name: str,
    format_prompt: str,
    products: list[Product],
    extra_instruction: str | None = None,
) -> dict[str, Any]:
    article_template = load_article_template()
    try:
        article_prompt = article_template.format(trend=trend)
    except KeyError as exc:
        raise RuntimeError(f"Missing template placeholder value: {exc}") from exc

    user_prompt = (
        f"Use this writing persona ({persona_name}):\n"
        f"{persona_prompt}\n\n"
        f"{article_prompt}\n\n"
        f"Use this article format template ({format_name}):\n"
        f"{format_prompt}\n\n"
        f"{build_products_prompt(products)}\n\n"
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


def validate_affiliate_link_usage(article_markdown: str, products: list[Product]) -> None:
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

    used_urls_by_section: list[str] = []
    for section_text in main_sections:
        url_count = count_provided_url_occurrences(section_text, provided_urls)
        if url_count != 1:
            raise ProductLinkError(
                "Each main section must contain exactly one provided affiliate URL."
            )

        section_urls = [url for url in provided_urls if url in section_text]
        used_urls_by_section.extend(section_urls)

    distinct_urls = set(used_urls_by_section)
    if len(distinct_urls) != SECTION_COUNT:
        raise ProductLinkError(
            "Each section must use a different provided product affiliate URL."
        )

    total_occurrences = count_provided_url_occurrences(article_markdown, provided_urls)
    if total_occurrences != SECTION_COUNT:
        raise ProductLinkError(
            "article_markdown must use exactly one provided affiliate URL per section and nowhere else."
        )


def normalize_and_validate(payload: dict[str, Any], products: list[Product]) -> dict[str, Any]:
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

    title = str(payload["title"]).strip()
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
        slug = slugify(slug)

    word_count = count_words(article_markdown)
    if word_count < MIN_WORDS or word_count > MAX_WORDS:
        raise ArticleLengthError(word_count=word_count, min_words=MIN_WORDS, max_words=MAX_WORDS)

    validate_affiliate_link_usage(article_markdown=article_markdown, products=products)

    estimated_reading_time_raw = str(payload.get("estimated_reading_time", "")).strip()
    estimated_reading_time = estimated_reading_time_raw or build_estimated_reading_time(word_count)

    hero_image_prompt = ensure_image_prompt_constraints(str(payload["hero_image_prompt"]).strip())
    if not hero_image_prompt:
        raise ValueError("hero_image_prompt cannot be empty.")

    section_image_prompts_raw = normalize_string_list(
        payload["section_image_prompts"],
        field_name="section_image_prompts",
        expected_count=SECTION_COUNT,
    )
    section_image_prompts = [ensure_image_prompt_constraints(item) for item in section_image_prompts_raw]

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

    return {
        "title": title,
        "slug": slug,
        "meta_description": meta_description,
        "keywords": keywords,
        "estimated_reading_time": estimated_reading_time,
        "hero_image_prompt": hero_image_prompt,
        "section_image_prompts": section_image_prompts,
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
    products: list[Product] | None = None,
) -> dict[str, Any]:
    last_error: Exception | None = None
    extra_instruction: str | None = None
    short_retry_count = 0
    product_retry_count = 0

    selected_format_name, selected_format_prompt = resolve_format_prompt(
        trend=trend,
        format_name=format_name,
    )
    selected_persona_name, selected_persona_prompt = resolve_persona_prompt(
        trend=trend,
        persona_name=persona_name,
    )

    selected_products = (
        products
        if products is not None
        else fetch_products_for_trend(trend=trend, limit=5)
    )
    selected_products = validate_products(selected_products)

    # One initial draft plus retries for short drafts or product-link rule failures.
    for _ in range(1 + SHORT_RETRY_LIMIT + PRODUCT_RETRY_LIMIT):
        try:
            payload = request_article_json(
                client=client,
                trend=trend,
                model=model,
                persona_name=selected_persona_name,
                persona_prompt=selected_persona_prompt,
                format_name=selected_format_name,
                format_prompt=selected_format_prompt,
                products=selected_products,
                extra_instruction=extra_instruction,
            )
            return normalize_and_validate(payload, products=selected_products)
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
            if product_retry_count < PRODUCT_RETRY_LIMIT:
                product_retry_count += 1
                extra_instruction = PRODUCT_RETRY_PROMPT_TEMPLATE
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





