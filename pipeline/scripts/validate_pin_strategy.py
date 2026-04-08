from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from generate_pin_strategy import derive_image_candidates, load_json_object, load_pin_templates, normalize_text

HOOK_WORD_LIMIT = 12
GENERIC_HOOK_PATTERNS = [
    r"^decor ideas you'll love$",
    r"^home decor ideas you'll love$",
    r"^interior ideas you'll love$",
    r"^ideas you'll love$",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Pinterest pin strategy output against simple quality and consistency rules."
    )
    parser.add_argument("pin_strategy_path", type=str, help="Path to pin strategy JSON.")
    parser.add_argument("article_package_path", type=str, help="Path to article package JSON.")
    return parser.parse_args()


def normalize_key(text: str) -> str:
    cleaned = normalize_text(text).lower()
    return re.sub(r"[^a-z0-9]+", "", cleaned)


def normalize_similarity_text(text: str) -> str:
    cleaned = normalize_text(text).lower()
    return re.sub(r"[^a-z0-9 ]+", " ", cleaned).strip()


def word_count(text: str) -> int:
    return len([word for word in normalize_text(text).split() if word])


def similarity_ratio(left: str, right: str) -> float:
    left_words = set(normalize_similarity_text(left).split())
    right_words = set(normalize_similarity_text(right).split())
    if not left_words or not right_words:
        return 0.0
    overlap = len(left_words & right_words)
    baseline = max(len(left_words), len(right_words))
    return overlap / baseline if baseline else 0.0


def hook_is_generic(hook_text: str) -> bool:
    cleaned = normalize_text(hook_text).lower()
    return any(re.fullmatch(pattern, cleaned) for pattern in GENERIC_HOOK_PATTERNS)


def validate_pin_strategy(
    pin_strategy: dict[str, Any],
    article_package: dict[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    variants = pin_strategy.get("variants")
    if not isinstance(variants, list) or not variants:
        return {
            "status": "fail",
            "warnings": warnings,
            "errors": ["Pin strategy must contain a non-empty variants list."],
        }

    template_ids = {
        normalize_text(item.get("id"))
        for item in load_pin_templates()
        if normalize_text(item.get("id"))
    }
    image_candidate_ids = {
        normalize_text(item.get("id"))
        for item in derive_image_candidates(article_package)
        if normalize_text(item.get("id"))
    }
    image_candidate_descriptions = {
        normalize_text(item.get("description")).lower()
        for item in derive_image_candidates(article_package)
        if normalize_text(item.get("description"))
    }

    article_title = normalize_text(article_package.get("title") or article_package.get("article_title"))
    article_title_key = normalize_key(article_title)

    hook_keys: dict[str, int] = {}
    hook_texts: list[tuple[int, str]] = []

    for index, variant in enumerate(variants, start=1):
        if not isinstance(variant, dict):
            errors.append(f"Variant {index} must be an object.")
            continue

        hook_text = normalize_text(variant.get("hook_text"))
        template_id = normalize_text(variant.get("template_id"))
        image_focus = normalize_text(variant.get("image_focus"))

        if not hook_text:
            errors.append(f"Variant {index} is missing hook_text.")
        else:
            hook_key = normalize_key(hook_text)
            hook_keys[hook_key] = hook_keys.get(hook_key, 0) + 1
            hook_texts.append((index, hook_text))

            if word_count(hook_text) > HOOK_WORD_LIMIT:
                warnings.append(f"Variant {index} hook exceeds {HOOK_WORD_LIMIT} words.")
            if normalize_key(hook_text) == article_title_key:
                warnings.append(f"Variant {index} hook mirrors the article title too closely.")
            if hook_is_generic(hook_text):
                warnings.append(f"Variant {index} hook is too generic.")

        if template_id not in template_ids:
            errors.append(f"Variant {index} uses unknown template_id '{template_id}'.")

        if not image_focus:
            errors.append(f"Variant {index} is missing image_focus.")
        else:
            image_focus_key = normalize_text(image_focus)
            image_focus_match = (
                image_focus_key in image_candidate_ids
                or image_focus_key.lower() in image_candidate_descriptions
            )
            if not image_focus_match:
                errors.append(f"Variant {index} image_focus '{image_focus}' does not match provided image candidates.")

    for hook_key, count in hook_keys.items():
        if hook_key and count > 1:
            errors.append("Pin strategy contains identical hooks across variants.")
            break

    for left_index, left_text in hook_texts:
        for right_index, right_text in hook_texts:
            if right_index <= left_index:
                continue
            if similarity_ratio(left_text, right_text) >= 0.8:
                warnings.append(
                    f"Variants {left_index} and {right_index} have near-duplicate hooks."
                )

    status = "pass"
    if errors:
        status = "fail"
    elif warnings:
        status = "warning"

    return {
        "status": status,
        "warnings": warnings,
        "errors": errors,
    }


def main() -> int:
    args = parse_args()
    pin_strategy = load_json_object(Path(args.pin_strategy_path))
    article_package = load_json_object(Path(args.article_package_path))
    result = validate_pin_strategy(pin_strategy=pin_strategy, article_package=article_package)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] != "fail" else 1


if __name__ == "__main__":
    raise SystemExit(main())
