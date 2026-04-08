from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

DEFAULT_MODEL = "gpt-4.1-mini"
INTERNAL_VARIANT_COUNT = 5
OUTPUT_VARIANT_COUNT = 3
HOOK_MIN_WORDS = 4
HOOK_MAX_WORDS = 12
GENERIC_HOOK_PATTERNS = [
    r"^best furniture for\b",
    r"^a guide to\b",
    r"^decor ideas you(?:'|’)ll love\b",
    r"^home decor ideas you(?:'|’)ll love\b",
]
FILLER_WORDS = {
    "really",
    "very",
    "beautiful",
    "stunning",
    "lovely",
    "amazing",
    "perfect",
    "ultimate",
    "complete",
}
HOOK_PATTERN_LIBRARY = {
    "list": "7 Ways to Make a Small Bedroom Feel Calmer",
    "problem": "Small Room? Try This Layout Shift",
    "transformation": "From Flat to Layered in One Swap",
    "curiosity": "This Styling Trick Changes the Whole Room",
}

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIN_TEMPLATES_PATH = PROJECT_ROOT / "pipeline" / "data" / "pin_templates.json"
PIN_HOOK_STYLES_PATH = PROJECT_ROOT / "pipeline" / "data" / "pin_hook_styles.json"

SYSTEM_PROMPT = """
You are a Pinterest strategy agent for an editorial home decor brand.
Your job is to produce high-performing pin concepts from lightweight article metadata.

Return only valid JSON with this exact shape:
{
  "variants": [
    {
      "variant_id": "v1",
      "hook_text": "string",
      "hook_style": "string",
      "template_id": "string",
      "image_focus": "string",
      "support_text": "string",
      "brand_text": "THE LIVIN' EDIT",
      "reason": "string"
    }
  ]
}

Rules:
- Generate exactly 5 variants.
- Hooks must be 4 to 12 words.
- Do not reuse the full article title.
- Hooks must feel scroll-stopping, useful, truthful, and Pinterest-native.
- Hooks must be readable in under 1 second.
- Use short, concrete phrasing with no filler.
- Favor outcome, problem, transformation, or curiosity.
- Each variant must use a meaningfully different hook structure.
- Support text should stay concise and readable.
- Use only provided hook_style ids and template ids.
- brand_text must always be exactly THE LIVIN' EDIT.
- Return JSON only, with no markdown fences or commentary.

Avoid these weak patterns:
- "Best Furniture for..."
- "A Guide to..."
- "Decor Ideas You'll Love"

Prefer patterns like:
- list: "7 Ways to..."
- problem: "Small Room? Try This"
- transformation: "From Plain to..."
- curiosity: "This Trick Changes Everything"
""".strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Pinterest pin strategy variants from article metadata."
    )
    parser.add_argument("article_package_path", type=str, help="Path to article package JSON.")
    parser.add_argument(
        "--image-candidates-path",
        type=str,
        default=None,
        help="Optional JSON file with image candidate descriptions.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Optional OpenAI model override. Defaults to PIN_STRATEGY_MODEL or gpt-4.1-mini.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional path to save the generated pin strategy JSON.",
    )
    return parser.parse_args()


def load_env(project_root: Path = PROJECT_ROOT) -> None:
    load_dotenv(project_root / ".env")


def load_json_object(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return data


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def load_openai_api_key() -> str:
    load_env()
    api_key = normalize_text(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for pin strategy generation.")
    return api_key


def resolve_pin_strategy_model(cli_model: str | None = None) -> str:
    load_env()
    if normalize_text(cli_model):
        return normalize_text(cli_model)
    env_model = normalize_text(os.getenv("PIN_STRATEGY_MODEL"))
    return env_model or DEFAULT_MODEL


def load_pin_templates(path: Path = PIN_TEMPLATES_PATH) -> list[dict[str, Any]]:
    data = load_json_object(path)
    items = data.get("templates")
    if not isinstance(items, list) or not items:
        raise ValueError("pin_templates.json must contain a non-empty templates list.")
    return [item for item in items if isinstance(item, dict)]


def load_hook_styles(path: Path = PIN_HOOK_STYLES_PATH) -> list[dict[str, Any]]:
    data = load_json_object(path)
    items = data.get("hook_styles")
    if not isinstance(items, list) or not items:
        raise ValueError("pin_hook_styles.json must contain a non-empty hook_styles list.")
    return [item for item in items if isinstance(item, dict)]


def summarize_image_prompt(prompt: str) -> str:
    cleaned = normalize_text(prompt)
    cleaned = re.sub(r"^Create a hero image for the article .*?\. ", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^Create a section image .*?\. ", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bConstraints:.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bEditorial interior photography.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if len(cleaned) > 180:
        shortened = cleaned[:177].rsplit(" ", 1)[0].strip()
        return (shortened or cleaned[:177]).rstrip(" ,;:-") + "..."
    return cleaned


def derive_image_candidates(article_package: dict[str, Any]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    hero_prompt = normalize_text(article_package.get("hero_image_prompt"))
    if hero_prompt:
        candidates.append(
            {
                "id": "hero",
                "description": summarize_image_prompt(hero_prompt),
            }
        )
    for index, prompt in enumerate(article_package.get("section_image_prompts") or [], start=1):
        prompt_text = normalize_text(prompt)
        if not prompt_text:
            continue
        candidates.append(
            {
                "id": f"section_{index}",
                "description": summarize_image_prompt(prompt_text),
            }
        )
    return candidates[:6]


def build_strategy_input(
    article_package: dict[str, Any],
    image_candidates: list[dict[str, str]],
    templates: list[dict[str, Any]],
    hook_styles: list[dict[str, Any]],
) -> dict[str, Any]:
    summary = normalize_text(
        article_package.get("meta_description")
        or article_package.get("summary")
        or article_package.get("excerpt")
    )
    editorial_mix: dict[str, Any] = {}
    if isinstance(article_package.get("editorial_mix"), dict):
        editorial_mix = article_package["editorial_mix"]
    elif normalize_text(article_package.get("editorial_mix_primary")):
        editorial_mix = {
            "primary": normalize_text(article_package.get("editorial_mix_primary")),
            "tags": article_package.get("editorial_mix_tags") or [],
        }

    return {
        "article_title": normalize_text(article_package.get("title") or article_package.get("article_title")),
        "article_summary": summary,
        "cluster_id": normalize_text(article_package.get("cluster_id")),
        "subtopic_id": normalize_text(article_package.get("subtopic_id")),
        "angle_id": normalize_text(article_package.get("angle_id")),
        "primary_keyword": normalize_text(article_package.get("primary_keyword")),
        "editorial_mix": editorial_mix,
        "image_candidates": image_candidates,
        "available_templates": templates,
        "hook_styles": hook_styles,
    }


def build_user_prompt(strategy_input: dict[str, Any]) -> str:
    return (
        "Create exactly 5 Pinterest pin concepts for this article.\n\n"
        f"{json.dumps(strategy_input, ensure_ascii=False, indent=2)}\n\n"
        "Decision priorities:\n"
        "- strong hook first\n"
        "- do not mirror the article title\n"
        "- hook must read instantly on mobile\n"
        "- avoid filler words and long setup phrases\n"
        "- do not start multiple hooks the same way\n"
        "- avoid weak article-title patterns like 'Best Furniture for', 'A Guide to', and 'Decor Ideas You'll Love'\n"
        "- use a mix of structures across variants: list, problem, transformation, curiosity\n"
        "- pick the best template for each hook\n"
        "- choose an image focus that makes sense for the hook\n"
        "- keep support text concise and complementary\n"
        "- vary framing across variants\n"
        "- make all 5 usable, distinct options rather than minor rewrites\n"
    )


def strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def normalize_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_text(text).lower())


def hook_matches_generic_pattern(hook_text: str) -> bool:
    cleaned = normalize_text(hook_text).lower()
    return any(re.search(pattern, cleaned) for pattern in GENERIC_HOOK_PATTERNS)


def classify_hook_structure(hook_text: str) -> str:
    cleaned = normalize_text(hook_text)
    lowered = cleaned.lower()
    if re.match(r"^\d+\s+", cleaned):
        return "list"
    if "?" in cleaned:
        return "problem"
    if lowered.startswith("from "):
        return "transformation"
    if lowered.startswith("this ") or "changes" in lowered:
        return "curiosity"
    if lowered.startswith("how to "):
        return "instructional"
    return "statement"


def count_filler_words(hook_text: str) -> int:
    words = re.findall(r"[a-zA-Z']+", normalize_text(hook_text).lower())
    return sum(1 for word in words if word in FILLER_WORDS)


def similarity_ratio(left: str, right: str) -> float:
    left_words = set(re.findall(r"[a-z0-9]+", normalize_text(left).lower()))
    right_words = set(re.findall(r"[a-z0-9]+", normalize_text(right).lower()))
    if not left_words or not right_words:
        return 0.0
    overlap = len(left_words & right_words)
    return overlap / max(len(left_words), len(right_words))


def score_hook_clarity(hook_text: str) -> float:
    score = 100.0
    word_total = len(hook_text.split())
    if word_total > 9:
        score -= (word_total - 9) * 4.0
    if len(hook_text) > 58:
        score -= min(18.0, (len(hook_text) - 58) * 0.8)
    score -= count_filler_words(hook_text) * 8.0
    if hook_matches_generic_pattern(hook_text):
        score -= 25.0
    if classify_hook_structure(hook_text) == "statement":
        score -= 10.0
    if normalize_text(hook_text).endswith("."):
        score -= 4.0
    return score


def score_variant_quality(variant: dict[str, str]) -> float:
    score = score_hook_clarity(variant["hook_text"])
    support_text = normalize_text(variant["support_text"])
    if not support_text:
        score -= 10.0
    elif len(support_text) > 120:
        score -= min(12.0, (len(support_text) - 120) * 0.2)
    if len(support_text.split()) > 18:
        score -= 6.0
    if classify_hook_structure(variant["hook_text"]) in {"list", "problem", "transformation", "curiosity"}:
        score += 6.0
    return score


def select_best_variants(variants: list[dict[str, str]]) -> list[dict[str, str]]:
    if len(variants) <= OUTPUT_VARIANT_COUNT:
        return variants

    ranked = sorted(
        variants,
        key=lambda item: (
            score_variant_quality(item),
            -len(item["hook_text"]),
        ),
        reverse=True,
    )

    selected: list[dict[str, str]] = []
    seen_styles: set[str] = set()
    seen_structures: set[str] = set()
    seen_templates: set[str] = set()

    for variant in ranked:
        if len(selected) >= OUTPUT_VARIANT_COUNT:
            break
        structure = classify_hook_structure(variant["hook_text"])
        style = variant["hook_style"]
        template_id = variant["template_id"]
        if style in seen_styles or structure in seen_structures:
            continue
        selected.append(variant)
        seen_styles.add(style)
        seen_structures.add(structure)
        seen_templates.add(template_id)

    for variant in ranked:
        if len(selected) >= OUTPUT_VARIANT_COUNT:
            break
        if any(normalize_key(existing["hook_text"]) == normalize_key(variant["hook_text"]) for existing in selected):
            continue
        if any(similarity_ratio(existing["hook_text"], variant["hook_text"]) >= 0.55 for existing in selected):
            continue
        if variant["template_id"] in seen_templates and len(selected) < 2:
            continue
        selected.append(variant)
        seen_styles.add(variant["hook_style"])
        seen_structures.add(classify_hook_structure(variant["hook_text"]))
        seen_templates.add(variant["template_id"])

    for variant in ranked:
        if len(selected) >= OUTPUT_VARIANT_COUNT:
            break
        if any(normalize_key(existing["hook_text"]) == normalize_key(variant["hook_text"]) for existing in selected):
            continue
        selected.append(variant)

    return selected[:OUTPUT_VARIANT_COUNT]


def validate_strategy_payload(
    payload: dict[str, Any],
    *,
    hook_style_ids: set[str],
    template_ids: set[str],
    article_title: str,
) -> dict[str, Any]:
    variants = payload.get("variants")
    if not isinstance(variants, list):
        raise RuntimeError("Pin strategy response must contain a variants list.")
    if len(variants) != INTERNAL_VARIANT_COUNT:
        raise RuntimeError(f"Pin strategy must return exactly {INTERNAL_VARIANT_COUNT} variants.")

    validated: list[dict[str, str]] = []
    seen_hook_keys: set[str] = set()
    seen_structures: set[str] = set()
    article_title_key = normalize_key(article_title)
    for index, item in enumerate(variants, start=1):
        if not isinstance(item, dict):
            raise RuntimeError(f"Variant {index} must be an object.")
        variant = {key: normalize_text(item.get(key)) for key in [
            "variant_id",
            "hook_text",
            "hook_style",
            "template_id",
            "image_focus",
            "support_text",
            "brand_text",
            "reason",
        ]}
        missing = [key for key, value in variant.items() if not value]
        if missing:
            raise RuntimeError(f"Variant {index} is missing required fields: {', '.join(missing)}")
        word_count = len(variant["hook_text"].split())
        if word_count < HOOK_MIN_WORDS or word_count > HOOK_MAX_WORDS:
            raise RuntimeError(
                f"Variant {index} hook_text must be {HOOK_MIN_WORDS} to {HOOK_MAX_WORDS} words."
            )
        if variant["hook_style"] not in hook_style_ids:
            raise RuntimeError(f"Variant {index} uses unknown hook_style '{variant['hook_style']}'.")
        if variant["template_id"] not in template_ids:
            raise RuntimeError(f"Variant {index} uses unknown template_id '{variant['template_id']}'.")
        if variant["brand_text"] != "THE LIVIN' EDIT":
            raise RuntimeError(f"Variant {index} brand_text must be exactly THE LIVIN' EDIT.")
        hook_key = normalize_key(variant["hook_text"])
        if hook_key == article_title_key:
            raise RuntimeError(f"Variant {index} hook_text reuses the article title.")
        if hook_key in seen_hook_keys:
            raise RuntimeError(f"Variant {index} hook_text duplicates another hook.")
        if hook_matches_generic_pattern(variant["hook_text"]):
            raise RuntimeError(f"Variant {index} hook_text uses a banned generic pattern.")
        if count_filler_words(variant["hook_text"]) >= 2:
            raise RuntimeError(f"Variant {index} hook_text is too padded with filler words.")
        if len(variant["hook_text"]) > 68:
            raise RuntimeError(f"Variant {index} hook_text is too long to read instantly.")
        structure = classify_hook_structure(variant["hook_text"])
        if structure in seen_structures and len(variants) >= 3:
            raise RuntimeError(f"Variant {index} repeats the '{structure}' hook structure.")
        for existing in validated:
            if similarity_ratio(existing["hook_text"], variant["hook_text"]) >= 0.7:
                raise RuntimeError(f"Variant {index} hook_text is too similar to another variant.")
        seen_hook_keys.add(hook_key)
        seen_structures.add(structure)
        validated.append(variant)
    return {"variants": select_best_variants(validated)}


def generate_pin_strategy(
    article_package: dict[str, Any],
    image_candidates: list[dict[str, str]] | None = None,
    *,
    model: str | None = None,
) -> dict[str, Any]:
    templates = load_pin_templates()
    hook_styles = load_hook_styles()
    resolved_image_candidates = image_candidates or derive_image_candidates(article_package)
    strategy_input = build_strategy_input(
        article_package=article_package,
        image_candidates=resolved_image_candidates,
        templates=templates,
        hook_styles=hook_styles,
    )

    client = OpenAI(api_key=load_openai_api_key())
    response = client.responses.create(
        model=resolve_pin_strategy_model(model),
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(strategy_input)},
        ],
    )
    raw_text = normalize_text(response.output_text)
    if not raw_text:
        raise RuntimeError("OpenAI returned an empty response for pin strategy generation.")

    try:
        payload = json.loads(strip_code_fences(raw_text))
    except json.JSONDecodeError as exc:
        raise RuntimeError("Pin strategy response was not valid JSON.") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Pin strategy response must be a JSON object.")

    return validate_strategy_payload(
        payload,
        hook_style_ids={normalize_text(item.get("id")) for item in hook_styles},
        template_ids={normalize_text(item.get("id")) for item in templates},
        article_title=normalize_text(article_package.get("title") or article_package.get("article_title")),
    )


def main() -> int:
    args = parse_args()
    article_package_path = Path(args.article_package_path)
    article_package = load_json_object(article_package_path)

    image_candidates: list[dict[str, str]] | None = None
    if args.image_candidates_path:
        image_payload = load_json_object(Path(args.image_candidates_path))
        raw_candidates = image_payload.get("image_candidates")
        if not isinstance(raw_candidates, list):
            raise RuntimeError("image_candidates_path JSON must contain an image_candidates list.")
        image_candidates = [
            {"id": normalize_text(item.get("id")), "description": normalize_text(item.get("description"))}
            for item in raw_candidates
            if isinstance(item, dict) and normalize_text(item.get("description"))
        ]

    strategy = generate_pin_strategy(
        article_package=article_package,
        image_candidates=image_candidates,
        model=args.model,
    )

    output_text = json.dumps(strategy, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(output_text, encoding="utf-8")
    print(output_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
