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
TITLE_LINE_LENGTH = 24
MAX_TITLE_LINES = 3
TITLE_FONT_SIZE = 56
TITLE_LINE_HEIGHT = 68
BRAND_FONT_SIZE = 20

STYLE_TEMPLATES = {
    "bottom-panel": {
        "panel_x": 58,
        "panel_y": 930,
        "panel_width": 884,
        "panel_height": 444,
        "panel_fill": "rgba(244,241,235,0.92)",
        "brand_x": 96,
        "brand_y": 996,
        "title_x": 96,
        "title_y": 1088,
        "title_fill": "#171717",
        "meta_x": 96,
        "meta_y": 1320,
    },
    "center-card": {
        "panel_x": 84,
        "panel_y": 742,
        "panel_width": 832,
        "panel_height": 516,
        "panel_fill": "rgba(255,255,255,0.90)",
        "brand_x": 126,
        "brand_y": 812,
        "title_x": 126,
        "title_y": 906,
        "title_fill": "#1a1a1a",
        "meta_x": 126,
        "meta_y": 1180,
    },
    "product-focus": {
        "panel_x": 64,
        "panel_y": 1014,
        "panel_width": 872,
        "panel_height": 360,
        "panel_fill": "rgba(255,255,255,0.88)",
        "brand_x": 104,
        "brand_y": 1078,
        "title_x": 104,
        "title_y": 1162,
        "title_fill": "#171717",
        "meta_x": 104,
        "meta_y": 1336,
    },
    "top-band": {
        "panel_x": 72,
        "panel_y": 108,
        "panel_width": 856,
        "panel_height": 404,
        "panel_fill": "rgba(245,244,241,0.90)",
        "brand_x": 114,
        "brand_y": 176,
        "title_x": 114,
        "title_y": 268,
        "title_fill": "#1a1a1a",
        "meta_x": 114,
        "meta_y": 476,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Pinterest pin assets from Pinterest metadata."
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


def wrap_title(title: str, line_length: int = TITLE_LINE_LENGTH, max_lines: int = MAX_TITLE_LINES) -> list[str]:
    words = normalize_copy(title).split()
    lines: list[str] = []
    current = ""

    for word in words:
        candidate = f"{current} {word}".strip()
        if not current or len(candidate) <= line_length:
            current = candidate
            continue

        lines.append(current)
        current = word
        if len(lines) >= max_lines - 1:
            break

    remaining_words = words[len(" ".join(lines + ([current] if current else [])).split()):]
    if current:
        tail = " ".join([current] + remaining_words).strip()
        if tail:
            lines.append(tail)
    elif remaining_words:
        lines.append(" ".join(remaining_words).strip())

    lines = [truncate_text(line, line_length + 12) for line in lines if line.strip()]
    if len(lines) > max_lines:
        lines = lines[:max_lines]

    return lines or [truncate_text(title, line_length + 12)]


def build_title_svg(lines: list[str], x: int, y: int, fill: str) -> str:
    tspans = []
    for index, line in enumerate(lines):
        line_y = y + index * TITLE_LINE_HEIGHT
        tspans.append(f'<tspan x="{x}" y="{line_y}">{escape(line)}</tspan>')
    return (
        f'<text font-family="Georgia, Times New Roman, serif" '
        f'font-size="{TITLE_FONT_SIZE}" font-weight="700" fill="{fill}">'
        f'{"".join(tspans)}</text>'
    )


def build_meta_svg(text: str, x: int, y: int, fill: str) -> str:
    return (
        f'<text x="{x}" y="{y}" font-family="Segoe UI, Arial, sans-serif" '
        f'font-size="18" font-weight="600" fill="{fill}" letter-spacing="0.3">{escape(text)}</text>'
    )


def build_pin_svg(
    data_uri: str,
    title: str,
    brand_name: str,
    description: str,
    style_name: str,
    variant_type: str,
) -> str:
    style = STYLE_TEMPLATES.get(style_name, STYLE_TEMPLATES["bottom-panel"])
    title_lines = wrap_title(title)
    meta_text = truncate_text(description, 92)
    title_svg = build_title_svg(
        lines=title_lines,
        x=style["title_x"],
        y=style["title_y"],
        fill=style["title_fill"],
    )
    meta_svg = build_meta_svg(
        text=meta_text,
        x=style["meta_x"],
        y=style["meta_y"],
        fill="rgba(26,26,26,0.78)",
    )

    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{PIN_WIDTH}" height="{PIN_HEIGHT}" viewBox="0 0 {PIN_WIDTH} {PIN_HEIGHT}" role="img" aria-label="{escape(title)}">
  <defs>
    <linearGradient id="imageShade" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="rgba(0,0,0,0.08)"/>
      <stop offset="65%" stop-color="rgba(0,0,0,0.02)"/>
      <stop offset="100%" stop-color="rgba(0,0,0,0.24)"/>
    </linearGradient>
  </defs>
  <rect width="100%" height="100%" fill="#f4f1eb" rx="32" ry="32"/>
  <image href="{data_uri}" x="0" y="0" width="{PIN_WIDTH}" height="{PIN_HEIGHT}" preserveAspectRatio="xMidYMid slice" clip-path="inset(0 round 32px)"/>
  <rect width="100%" height="100%" fill="url(#imageShade)" rx="32" ry="32"/>
  <rect x="{style['panel_x']}" y="{style['panel_y']}" width="{style['panel_width']}" height="{style['panel_height']}" rx="30" ry="30" fill="{style['panel_fill']}"/>
  <text x="{style['brand_x']}" y="{style['brand_y']}" font-family="Segoe UI, Arial, sans-serif" font-size="{BRAND_FONT_SIZE}" font-weight="700" fill="#1a1a1a" letter-spacing="1.5">{escape(brand_name.upper())}</text>
  {title_svg}
  {meta_svg}
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
        style_name = str(variant.get("style_name", "bottom-panel")).strip() or "bottom-panel"
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
