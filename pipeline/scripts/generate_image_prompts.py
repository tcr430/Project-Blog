from __future__ import annotations

import argparse
import json
import sys
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a hero image prompt and five section image prompts."
    )
    parser.add_argument("title", type=str, help="Article title.")
    parser.add_argument(
        "--section",
        action="append",
        default=[],
        help="Section heading. Provide exactly five --section values.",
    )
    return parser.parse_args()


def validate_sections(sections: list[str]) -> list[str]:
    cleaned = [item.strip() for item in sections if item.strip()]
    if len(cleaned) != 5:
        raise ValueError("Exactly five section headings are required via --section.")
    return cleaned


def build_base_style_block() -> str:
    return (
        "Style: editorial interior photography. "
        "Lighting: natural daylight. "
        "Materials: realistic materials and textures. "
        "Constraints: no text, no logos, no people."
    )


def build_hero_image_prompt(title: str) -> str:
    style = build_base_style_block()
    return (
        f"Create a hero interior scene for the article '{title}'. "
        "The image should communicate the overall decor direction in a polished, magazine-like composition. "
        f"{style}"
    )


def build_section_image_prompt(title: str, section_heading: str, section_index: int) -> str:
    style = build_base_style_block()
    return (
        f"Create section image {section_index} for the article '{title}', focused on '{section_heading}'. "
        "Show a realistic home interior moment that supports this section's styling idea. "
        f"{style}"
    )


def generate_image_prompts(title: str, section_headings: list[str]) -> dict[str, Any]:
    hero_image_prompt = build_hero_image_prompt(title=title)

    section_image_prompts: list[str] = []
    for index, heading in enumerate(section_headings, start=1):
        section_image_prompts.append(
            build_section_image_prompt(
                title=title,
                section_heading=heading,
                section_index=index,
            )
        )

    return {
        "hero_image_prompt": hero_image_prompt,
        "section_image_prompts": section_image_prompts,
    }


def main() -> int:
    args = parse_args()

    title = args.title.strip()
    if not title:
        print("Error: title cannot be empty.", file=sys.stderr)
        return 1

    try:
        section_headings = validate_sections(args.section)
        output = generate_image_prompts(title=title, section_headings=section_headings)
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
