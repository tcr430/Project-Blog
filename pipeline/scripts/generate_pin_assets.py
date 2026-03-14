from __future__ import annotations

import argparse
import base64
import json
import re
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

PIN_WIDTH = 1000
PIN_HEIGHT = 1500
TITLE_FONT_SIZE = 68
TITLE_LINE_HEIGHT = 78
TITLE_STROKE_WIDTH = 7
BRAND_FONT_SIZE = 22
DESCRIPTION_FONT_SIZE = 19
TITLE_MAX_LENGTH = 110

TEMPLATE_ALIASES = {
    "bottom-panel": "bottom-title",
    "center-card": "centered-title",
    "product-focus": "minimalist-overlay",
    "top-band": "top-title",
}

STYLE_TEMPLATES = {
    "bottom-title": {
        "panel_x": 56,
        "panel_y": 924,
        "panel_width": 888,
        "panel_height": 430,
        "panel_fill": "rgba(248,244,238,0.92)",
        "brand_x": 96,
        "brand_y": 982,
        "title_x": 96,
        "title_y": 1082,
        "title_fill": "#181512",
        "title_anchor": "start",
        "description_x": 96,
        "description_y": 1310,
        "description_fill": "rgba(24,21,18,0.78)",
        "description_line_length": 78,
        "title_line_length": 24,
        "max_title_lines": 3,
        "gradient_mode": "bottom-heavy",
    },
    "top-title": {
        "panel_x": 72,
        "panel_y": 88,
        "panel_width": 856,
        "panel_height": 406,
        "panel_fill": "rgba(245,242,237,0.91)",
        "brand_x": 112,
        "brand_y": 152,
        "title_x": 112,
        "title_y": 252,
        "title_fill": "#181512",
        "title_anchor": "start",
        "description_x": 112,
        "description_y": 460,
        "description_fill": "rgba(24,21,18,0.80)",
        "description_line_length": 76,
        "title_line_length": 24,
        "max_title_lines": 3,
        "gradient_mode": "top-heavy",
    },
    "centered-title": {
        "panel_x": 88,
        "panel_y": 522,
        "panel_width": 824,
        "panel_height": 494,
        "panel_fill": "rgba(255,255,255,0.84)",
        "brand_x": 500,
        "brand_y": 590,
        "title_x": 500,
        "title_y": 708,
        "title_fill": "#171412",
        "title_anchor": "middle",
        "description_x": 500,
        "description_y": 924,
        "description_fill": "rgba(23,20,18,0.78)",
        "description_line_length": 64,
        "title_line_length": 20,
        "max_title_lines": 4,
        "gradient_mode": "center-focus",
    },
    "minimalist-overlay": {
        "panel_x": 0,
        "panel_y": 0,
        "panel_width": 0,
        "panel_height": 0,
        "panel_fill": "rgba(0,0,0,0)",
        "brand_x": 92,
        "brand_y": 122,
        "title_x": 92,
        "title_y": 1086,
        "title_fill": "#ffffff",
        "title_anchor": "start",
        "description_x": 92,
        "description_y": 1320,
        "description_fill": "rgba(255,255,255,0.88)",
        "description_line_length": 70,
        "title_line_length": 23,
        "max_title_lines": 3,
        "gradient_mode": "minimal-dark",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate branded Pinterest pin assets from Pinterest metadata."
    )
    parser.add_argument("pinterest_metadata_path", type=str, help="Path to Pinterest metadata JSON.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")

    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return data


def normalize_image_path(image_path: str) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    return project_root / image_path.lstrip("/")


def detect_mime_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".webp":
        return "image/webp"
    return "image/png"


def encode_image_as_data_uri(image_path: Path) -> str:
    if not image_path.exists():
        raise FileNotFoundError(f"Base hero image not found for pin generation: {image_path}")

    mime_type = detect_mime_type(image_path)
    encoded = base64.b64encode(image_path.read_bytes()).decode("utf-8")
    return f"data:{mime_type};base64,{encoded}"


def normalize_copy(text: str) -> str:
    return re.sub(r"\s+", " ", str(text).strip())


def truncate_text(text: str, max_length: int) -> str:
    cleaned = normalize_copy(text)
    if len(cleaned) <= max_length:
        return cleaned
    shortened = cleaned[: max_length - 3].rsplit(" ", 1)[0].strip()
    return (shortened or cleaned[: max_length - 3]).rstrip(" ,;:-") + "..."


def wrap_copy(text: str, line_length: int, max_lines: int) -> list[str]:
    words = normalize_copy(text).split()
    if not words:
        return []

    lines: list[str] = []
    current_line = ""
    index = 0

    while index < len(words):
        word = words[index]
        candidate = f"{current_line} {word}".strip()
        if not current_line or len(candidate) <= line_length:
            current_line = candidate
            index += 1
            continue

        lines.append(current_line)
        current_line = ""
        if len(lines) >= max_lines - 1:
            break

    remaining_words = words[index:]
    if current_line:
        remaining_words = current_line.split() + remaining_words

    if remaining_words and len(lines) < max_lines:
        tail = " ".join(remaining_words)
        lines.append(truncate_text(tail, line_length + 8))

    return [line for line in lines if line.strip()]


def wrap_title(title: str, line_length: int, max_lines: int) -> list[str]:
    shortened = truncate_text(title, TITLE_MAX_LENGTH)
    return wrap_copy(shortened, line_length=line_length, max_lines=max_lines) or [shortened]


def resolve_template(style_name: str) -> dict[str, Any]:
    canonical_name = TEMPLATE_ALIASES.get(style_name, style_name)
    return STYLE_TEMPLATES.get(canonical_name, STYLE_TEMPLATES["bottom-title"])


def build_title_svg(lines: list[str], x: int, y: int, fill: str, anchor: str) -> str:
    shadow_tspans: list[str] = []
    title_tspans: list[str] = []
    for index, line in enumerate(lines):
        line_y = y + index * TITLE_LINE_HEIGHT
        escaped = escape(line)
        shadow_tspans.append(f'<tspan x="{x}" y="{line_y}">{escaped}</tspan>')
        title_tspans.append(f'<tspan x="{x}" y="{line_y}">{escaped}</tspan>')

    shadow = (
        f'<text font-family="Georgia, Times New Roman, serif" '
        f'font-size="{TITLE_FONT_SIZE}" font-weight="700" text-anchor="{anchor}" '
        f'fill="rgba(0,0,0,0.18)" stroke="rgba(0,0,0,0.18)" stroke-width="{TITLE_STROKE_WIDTH}" '
        f'stroke-linejoin="round">{"".join(shadow_tspans)}</text>'
    )
    title_text = (
        f'<text font-family="Georgia, Times New Roman, serif" '
        f'font-size="{TITLE_FONT_SIZE}" font-weight="700" text-anchor="{anchor}" '
        f'fill="{fill}">{"".join(title_tspans)}</text>'
    )
    return shadow + title_text


def build_multiline_text_svg(
    *,
    lines: list[str],
    x: int,
    y: int,
    fill: str,
    anchor: str,
    font_size: int,
    line_height: int,
    font_family: str,
    font_weight: int,
) -> str:
    tspans: list[str] = []
    for index, line in enumerate(lines):
        line_y = y + index * line_height
        tspans.append(f'<tspan x="{x}" y="{line_y}">{escape(line)}</tspan>')
    return (
        f'<text font-family="{font_family}" font-size="{font_size}" font-weight="{font_weight}" '
        f'text-anchor="{anchor}" fill="{fill}">{"".join(tspans)}</text>'
    )


def build_description_svg(text: str, x: int, y: int, fill: str, anchor: str, line_length: int) -> str:
    lines = wrap_copy(text, line_length=line_length, max_lines=2)
    if not lines:
        return ""
    return build_multiline_text_svg(
        lines=lines,
        x=x,
        y=y,
        fill=fill,
        anchor=anchor,
        font_size=DESCRIPTION_FONT_SIZE,
        line_height=28,
        font_family="Segoe UI, Arial, sans-serif",
        font_weight=600,
    )


def build_brand_label_svg(brand_name: str, x: int, y: int, anchor: str) -> str:
    label = escape(brand_name.upper())
    if anchor == "middle":
        rect_x = x - 142
    else:
        rect_x = x - 18
    rect_y = y - 34
    return (
        f'<rect x="{rect_x}" y="{rect_y}" width="284" height="46" rx="23" ry="23" '
        f'fill="rgba(255,255,255,0.58)" />'
        f'<text x="{x}" y="{y}" font-family="Segoe UI, Arial, sans-serif" '
        f'font-size="{BRAND_FONT_SIZE}" font-weight="700" text-anchor="{anchor}" '
        f'fill="rgba(24,21,18,0.88)" letter-spacing="1.2">{label}</text>'
    )


def build_gradient_defs(gradient_mode: str) -> str:
    gradients = {
        "bottom-heavy": """
    <linearGradient id="imageShade" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="rgba(12,10,8,0.04)"/>
      <stop offset="58%" stop-color="rgba(12,10,8,0.08)"/>
      <stop offset="100%" stop-color="rgba(12,10,8,0.34)"/>
    </linearGradient>
""",
        "top-heavy": """
    <linearGradient id="imageShade" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="rgba(12,10,8,0.30)"/>
      <stop offset="36%" stop-color="rgba(12,10,8,0.12)"/>
      <stop offset="100%" stop-color="rgba(12,10,8,0.05)"/>
    </linearGradient>
""",
        "center-focus": """
    <radialGradient id="imageShade" cx="50%" cy="44%" r="72%">
      <stop offset="0%" stop-color="rgba(12,10,8,0.02)"/>
      <stop offset="62%" stop-color="rgba(12,10,8,0.14)"/>
      <stop offset="100%" stop-color="rgba(12,10,8,0.28)"/>
    </radialGradient>
""",
        "minimal-dark": """
    <linearGradient id="imageShade" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="rgba(10,9,8,0.16)"/>
      <stop offset="54%" stop-color="rgba(10,9,8,0.08)"/>
      <stop offset="100%" stop-color="rgba(10,9,8,0.56)"/>
    </linearGradient>
""",
    }
    return gradients.get(gradient_mode, gradients["bottom-heavy"])


def build_pin_svg(
    data_uri: str,
    title: str,
    brand_name: str,
    description: str,
    style_name: str,
    variant_type: str,
) -> str:
    style = resolve_template(style_name)
    anchor = str(style["title_anchor"])
    title_lines = wrap_title(
        title,
        line_length=int(style["title_line_length"]),
        max_lines=int(style["max_title_lines"]),
    )
    description_text = truncate_text(description, 160)
    title_svg = build_title_svg(
        lines=title_lines,
        x=int(style["title_x"]),
        y=int(style["title_y"]),
        fill=str(style["title_fill"]),
        anchor=anchor,
    )
    description_svg = build_description_svg(
        text=description_text,
        x=int(style["description_x"]),
        y=int(style["description_y"]),
        fill=str(style["description_fill"]),
        anchor=anchor,
        line_length=int(style["description_line_length"]),
    )
    brand_svg = build_brand_label_svg(
        brand_name=brand_name,
        x=int(style["brand_x"]),
        y=int(style["brand_y"]),
        anchor=anchor,
    )
    panel_svg = ""
    if int(style["panel_width"]) > 0 and int(style["panel_height"]) > 0:
        panel_svg = (
            f'<rect x="{style["panel_x"]}" y="{style["panel_y"]}" '
            f'width="{style["panel_width"]}" height="{style["panel_height"]}" '
            f'rx="32" ry="32" fill="{style["panel_fill"]}"/>'
        )

    gradient_defs = build_gradient_defs(str(style["gradient_mode"]))

    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{PIN_WIDTH}" height="{PIN_HEIGHT}" viewBox="0 0 {PIN_WIDTH} {PIN_HEIGHT}" role="img" aria-label="{escape(title)}">
  <defs>{gradient_defs}  </defs>
  <rect width="100%" height="100%" fill="#efe8df" rx="32" ry="32"/>
  <image href="{data_uri}" x="0" y="0" width="{PIN_WIDTH}" height="{PIN_HEIGHT}" preserveAspectRatio="xMidYMid slice" clip-path="inset(0 round 32px)"/>
  <rect width="100%" height="100%" fill="url(#imageShade)" rx="32" ry="32"/>
  {panel_svg}
  {brand_svg}
  {title_svg}
  {description_svg}
  <metadata>{escape(variant_type)}</metadata>
</svg>'''


def load_brand_name(project_root: Path) -> str:
    config_path = project_root / "_config.yml"
    for line in config_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("title:"):
            return stripped.split(":", 1)[1].strip().strip('"')
    return "The Livin' Edit"


def generate_pin_assets(pinterest_metadata_path: Path) -> list[Path]:
    project_root = Path(__file__).resolve().parents[2]
    payload = load_json(pinterest_metadata_path)

    hero_image_path = normalize_image_path(str(payload["hero_image_path"]).strip())
    data_uri = encode_image_as_data_uri(hero_image_path)
    slug = str(payload["article_slug"]).strip()
    brand_name = load_brand_name(project_root)

    output_dir = project_root / "assets" / "pins" / slug
    output_dir.mkdir(parents=True, exist_ok=True)

    saved_paths: list[Path] = []
    variants = payload.get("variants", [])
    if not isinstance(variants, list) or not variants:
        raise ValueError("Pinterest metadata must contain a non-empty variants list.")
    if len(variants) < 4:
        raise ValueError("Pinterest metadata must contain at least 4 variants.")

    for index, variant in enumerate(variants, start=1):
        title = normalize_copy(str(variant.get("title", "")))
        description = normalize_copy(str(variant.get("description", "")))
        style_name = str(variant.get("style_name", "bottom-title")).strip() or "bottom-title"
        variant_type = str(variant.get("variant_type", "")).strip() or f"variant_{index}"
        if not title:
            raise ValueError(f"Variant {index} is missing a title.")
        if not description:
            raise ValueError(f"Variant {index} is missing a description.")

        svg = build_pin_svg(
            data_uri=data_uri,
            title=title,
            brand_name=brand_name,
            description=description,
            style_name=style_name,
            variant_type=variant_type,
        )
        output_path = output_dir / f"pin-{index}.svg"
        output_path.write_text(svg, encoding="utf-8")
        print(f"[pinterest] generated {style_name} pin at {output_path}")
        saved_paths.append(output_path)

    return saved_paths


def main() -> int:
    args = parse_args()
    try:
        saved_paths = generate_pin_assets(Path(args.pinterest_metadata_path))
        for path in saved_paths:
            print(path)
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
