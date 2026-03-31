from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from PIL import Image, ImageColor, ImageDraw, ImageFont
from pinterest_pin_design import classify_topic_style, load_design_system

PIN_WIDTH = 1000
PIN_HEIGHT = 1500
TITLE_FONT_SIZE = 68
TITLE_LINE_HEIGHT = 78
TITLE_STROKE_WIDTH = 4
BRAND_FONT_SIZE = 22
DESCRIPTION_FONT_SIZE = 19
TITLE_MAX_LENGTH = 110
PIN_BORDER_RADIUS = 32
QUALITY_SCORE_MINIMUM = 70

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


def load_pinterest_design_system() -> dict[str, Any]:
    return load_design_system()


def normalize_image_path(image_path: str) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    return project_root / image_path.lstrip("/")


def load_base_image(image_path: Path) -> Image.Image:
    if not image_path.exists():
        raise FileNotFoundError(f"Base hero image not found for pin generation: {image_path}")
    return Image.open(image_path).convert("RGBA")


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


def resolve_template(style_name: str, *, template_family: str = "", design_system: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
    system = design_system or load_pinterest_design_system()
    templates = system.get("template_families", {})
    if template_family and template_family in templates:
        return template_family, dict(templates[template_family])

    canonical_name = TEMPLATE_ALIASES.get(style_name, style_name)
    fallback_map = {
        "bottom-title": "editorial_split",
        "top-title": "insight_band",
        "centered-title": "utility_stack",
        "minimalist-overlay": "minimal_frame",
    }
    mapped_family = fallback_map.get(canonical_name, "editorial_split")
    return mapped_family, dict(templates.get(mapped_family, {}))


def parse_rgba(value: str) -> tuple[int, int, int, int]:
    normalized = value.strip()
    if normalized.startswith("rgba(") and normalized.endswith(")"):
        parts = [part.strip() for part in normalized[5:-1].split(",")]
        if len(parts) == 4:
            red = int(float(parts[0]))
            green = int(float(parts[1]))
            blue = int(float(parts[2]))
            alpha = max(0, min(255, round(float(parts[3]) * 255)))
            return (red, green, blue, alpha)
    rgb = ImageColor.getrgb(normalized)
    return (rgb[0], rgb[1], rgb[2], 255)


def find_font(candidates: list[str], size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    font_paths = [
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("C:/Windows/Fonts/georgiab.ttf"),
        Path("C:/Windows/Fonts/segoeuib.ttf"),
        Path("C:/Windows/Fonts/segoeui.ttf"),
    ]
    for path in font_paths:
        if not path.exists():
            continue
        path_text = str(path).replace("\\", "/").lower()
        if candidates and not any(candidate in path_text for candidate in candidates):
            continue
        try:
            return ImageFont.truetype(str(path), size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def title_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    return find_font(["georgia", "dejavuserif"], size)


def ui_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    return find_font(["segoe", "dejavusans"], size)


def crop_to_pin_size(image: Image.Image, mode: str = "full_bleed") -> Image.Image:
    source_ratio = image.width / image.height
    target_ratio = PIN_WIDTH / PIN_HEIGHT
    if source_ratio > target_ratio:
        new_width = int(image.height * target_ratio)
        left_ratio_map = {
            "left_focus": 0.25,
            "right_weighted": 0.75,
        }
        left_ratio = left_ratio_map.get(mode, 0.5)
        left = max(0, int((image.width - new_width) * left_ratio))
        image = image.crop((left, 0, left + new_width, image.height))
    else:
        new_height = int(image.width / target_ratio)
        top_ratio_map = {
            "top_crop": 0.18,
            "bottom_showcase": 0.78,
        }
        top_ratio = top_ratio_map.get(mode, 0.5)
        top = max(0, int((image.height - new_height) * top_ratio))
        image = image.crop((0, top, image.width, top + new_height))
    return image.resize((PIN_WIDTH, PIN_HEIGHT), Image.Resampling.LANCZOS)


def apply_gradient_overlay(base: Image.Image, gradient_mode: str, overlay_strength: float = 0.22) -> Image.Image:
    overlay = Image.new("RGBA", (PIN_WIDTH, PIN_HEIGHT), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    max_alpha = max(20, min(170, int(255 * overlay_strength)))
    if gradient_mode == "top_editorial":
        for y in range(PIN_HEIGHT):
            alpha = int(max_alpha - (y / PIN_HEIGHT) * max_alpha)
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(10, 9, 8, alpha))
    elif gradient_mode == "soft_scrim":
        for y in range(PIN_HEIGHT):
            alpha = int(max_alpha * 0.45)
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(17, 15, 13, alpha))
    elif gradient_mode == "editorial_scrim":
        for y in range(PIN_HEIGHT):
            alpha = int((max_alpha * 0.25) + ((y / PIN_HEIGHT) * max_alpha * 0.75))
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(12, 10, 8, alpha))
    elif gradient_mode == "deep_bottom_band":
        for y in range(PIN_HEIGHT):
            alpha = int((y / PIN_HEIGHT) ** 2 * max_alpha * 1.2)
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(10, 9, 8, alpha))
    elif gradient_mode == "top-heavy":
        for y in range(PIN_HEIGHT):
            alpha = int(max(12, max_alpha - (y / PIN_HEIGHT) * max_alpha))
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(10, 9, 8, alpha))
    elif gradient_mode == "center-focus":
        center_y = PIN_HEIGHT * 0.44
        for y in range(PIN_HEIGHT):
            distance = abs(y - center_y) / PIN_HEIGHT
            alpha = int(min(max_alpha, max(8, distance * max_alpha * 1.2)))
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(12, 10, 8, alpha))
    elif gradient_mode == "minimal-dark":
        for y in range(PIN_HEIGHT):
            alpha = int(max_alpha * 0.35 + (y / PIN_HEIGHT) * max_alpha)
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(10, 9, 8, alpha))
    else:
        for y in range(PIN_HEIGHT):
            alpha = int(max_alpha * 0.2 + (y / PIN_HEIGHT) * max_alpha * 0.8)
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(12, 10, 8, alpha))
    return Image.alpha_composite(base, overlay)


def draw_rounded_panel(canvas: Image.Image, style: dict[str, Any]) -> None:
    if int(style["panel_width"]) <= 0 or int(style["panel_height"]) <= 0:
        return
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    draw.rounded_rectangle(
        (
            int(style["panel_x"]),
            int(style["panel_y"]),
            int(style["panel_x"]) + int(style["panel_width"]),
            int(style["panel_y"]) + int(style["panel_height"]),
        ),
        radius=32,
        fill=parse_rgba(str(style["panel_fill"])),
    )
    canvas.alpha_composite(overlay)


def measure_text(font: ImageFont.FreeTypeFont | ImageFont.ImageFont, text: str) -> tuple[int, int]:
    dummy = Image.new("RGBA", (10, 10), (0, 0, 0, 0))
    draw = ImageDraw.Draw(dummy)
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    return right - left, bottom - top


def anchored_x(x: int, anchor: str, text_width: int) -> int:
    if anchor == "middle":
        return int(x - (text_width / 2))
    return x


def draw_brand_label(canvas: Image.Image, brand_name: str, x: int, y: int, anchor: str) -> None:
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    label = normalize_copy(brand_name).upper()
    font = ui_font(BRAND_FONT_SIZE)
    label_width, label_height = measure_text(font, label)
    padding_x = 18
    padding_y = 12
    text_x = anchored_x(x, anchor, label_width)
    rect_x = text_x - padding_x
    rect_y = y - label_height
    rect_width = label_width + padding_x * 2
    rect_height = label_height + padding_y * 2
    draw.rounded_rectangle(
        (rect_x, rect_y - 10, rect_x + rect_width, rect_y - 10 + rect_height),
        radius=23,
        fill=(255, 255, 255, 148),
    )
    draw.text((text_x, y - label_height), label, font=font, fill=(24, 21, 18, 224))
    canvas.alpha_composite(overlay)


def draw_kicker(canvas: Image.Image, text: str, x: int, y: int, accent: tuple[int, int, int, int]) -> None:
    draw = ImageDraw.Draw(canvas)
    font = ui_font(18)
    label = normalize_copy(text).upper()
    draw.text((x, y), label, font=font, fill=accent)
    width, _ = measure_text(font, label)
    draw.line((x, y + 28, x + width + 18, y + 28), fill=accent, width=3)


def draw_cta_chip(canvas: Image.Image, text: str, x: int, y: int, accent: tuple[int, int, int, int]) -> None:
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    font = ui_font(19)
    label = normalize_copy(text)
    width, height = measure_text(font, label)
    chip_width = width + 34
    chip_height = height + 20
    draw.rounded_rectangle((x, y - height, x + chip_width, y - height + chip_height), radius=18, fill=accent)
    draw.text((x + 17, y - height + 8), label, font=font, fill=(255, 255, 255, 255))
    canvas.alpha_composite(overlay)


def draw_multiline_text(
    canvas: Image.Image,
    *,
    lines: list[str],
    x: int,
    y: int,
    anchor: str,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
    line_height: int,
    stroke_fill: tuple[int, int, int, int] | None = None,
    stroke_width: int = 0,
) -> None:
    draw = ImageDraw.Draw(canvas)
    for index, line in enumerate(lines):
        line_width, line_height_px = measure_text(font, line)
        draw_x = anchored_x(x, anchor, line_width)
        draw_y = y + index * line_height - line_height_px
        draw.text(
            (draw_x, draw_y),
            line,
            font=font,
            fill=fill,
            stroke_width=stroke_width,
            stroke_fill=stroke_fill,
        )


def rounded_mask(size: tuple[int, int], radius: int) -> Image.Image:
    mask = Image.new("L", size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, size[0], size[1]), radius=radius, fill=255)
    return mask


def validate_layout(
    *,
    headline_lines: list[str],
    subheadline_lines: list[str],
    template: dict[str, Any],
) -> tuple[int, list[str]]:
    score = 100
    warnings: list[str] = []

    if len(headline_lines) > int(template["headline_max_lines"]):
        score -= 35
        warnings.append("headline overflow")
    if len(subheadline_lines) > int(template["subheadline_max_lines"]):
        score -= 25
        warnings.append("subheadline overflow")
    longest_headline = max((len(line) for line in headline_lines), default=0)
    if longest_headline > 34:
        score -= 10
        warnings.append("headline lines are too wide")
    if not headline_lines:
        score -= 40
        warnings.append("headline missing")
    return score, warnings


def build_pin_image(
    *,
    base_image: Image.Image,
    brand_name: str,
    variant: dict[str, Any],
    article_payload: dict[str, Any],
    design_system: dict[str, Any],
) -> tuple[Image.Image, dict[str, Any]]:
    topic_style_key = str(variant.get("topic_style") or classify_topic_style(article_payload)).strip() or "editorial"
    topic_style = dict(design_system.get("topic_styles", {}).get(topic_style_key, {}))
    template_name, template = resolve_template(
        str(variant.get("style_name", "bottom-title")),
        template_family=str(variant.get("template_family", "")),
        design_system=design_system,
    )

    headline = normalize_copy(str(variant.get("display_headline") or variant.get("title") or ""))
    subheadline = normalize_copy(str(variant.get("display_subheadline") or variant.get("description") or ""))
    kicker = normalize_copy(str(variant.get("display_kicker") or topic_style.get("kicker_prefix") or ""))
    cta_label = normalize_copy(str(variant.get("cta_label") or design_system.get("brand", {}).get("cta_text") or ""))

    title_lines = wrap_copy(
        truncate_text(headline, int(template["headline_max_chars"])),
        line_length=max(16, int(template["headline_max_chars"] // max(1, int(template["headline_max_lines"])))),
        max_lines=int(template["headline_max_lines"]),
    )
    subheadline_lines = wrap_copy(
        truncate_text(subheadline, 140),
        line_length=42,
        max_lines=int(template["subheadline_max_lines"]),
    )
    quality_score, quality_warnings = validate_layout(
        headline_lines=title_lines,
        subheadline_lines=subheadline_lines,
        template=template,
    )

    canvas = Image.new("RGBA", (PIN_WIDTH, PIN_HEIGHT), parse_rgba(design_system["brand"]["background"]))
    hero = crop_to_pin_size(base_image.copy(), mode=str(template.get("image_mode", "full_bleed")))
    hero.putalpha(rounded_mask(hero.size, PIN_BORDER_RADIUS))
    canvas.alpha_composite(hero)
    canvas = apply_gradient_overlay(
        canvas,
        str(template.get("overlay_mode", "bottom_fade")),
        overlay_strength=float(topic_style.get("overlay_strength", 0.22)),
    )

    panel = template.get("panel", {})
    panel_template = {
        "panel_x": int(panel.get("x", 0)),
        "panel_y": int(panel.get("y", 0)),
        "panel_width": int(panel.get("width", 0)),
        "panel_height": int(panel.get("height", 0)),
        "panel_fill": str(topic_style.get("panel", design_system["brand"]["paper"])),
    }
    draw_rounded_panel(canvas, panel_template)

    headline_box = template["headline_box"]
    subheadline_box = template["subheadline_box"]
    accent_color = parse_rgba(str(topic_style.get("accent", design_system["brand"]["accent"])))
    ink = parse_rgba(design_system["brand"]["ink"])
    muted_ink = parse_rgba(design_system["brand"]["muted_ink"])
    headline_fill = ink if template_name != "minimal_frame" else (255, 255, 255, 255)
    subheadline_fill = muted_ink if template_name != "minimal_frame" else (255, 255, 255, 230)

    draw_brand_label(canvas, brand_name=brand_name, x=int(headline_box["x"]), y=int(headline_box["y"]) - 72, anchor="start")
    if kicker:
        draw_kicker(canvas, kicker, int(headline_box["x"]), int(headline_box["y"]) - 20, accent_color)

    draw_multiline_text(
        canvas,
        lines=title_lines,
        x=int(headline_box["x"]),
        y=int(headline_box["y"]) + 126,
        anchor="start",
        font=title_font(int(template["headline_font_size"])),
        fill=headline_fill,
        line_height=int(template["headline_line_height"]),
        stroke_fill=(0, 0, 0, 48) if template_name == "minimal_frame" else None,
        stroke_width=2 if template_name == "minimal_frame" else 0,
    )

    if subheadline_lines:
        draw_multiline_text(
            canvas,
            lines=subheadline_lines,
            x=int(subheadline_box["x"]),
            y=int(subheadline_box["y"]) + 34,
            anchor="start",
            font=ui_font(int(template["subheadline_font_size"])),
            fill=subheadline_fill,
            line_height=int(template["subheadline_line_height"]),
        )

    if bool(template.get("cta_enabled")) and cta_label:
        draw_cta_chip(
            canvas,
            cta_label,
            int(template.get("cta_x", subheadline_box["x"])),
            int(template.get("cta_y", subheadline_box["y"])) ,
            accent_color,
        )

    diagnostics = {
        "template_family": template_name,
        "topic_style": topic_style_key,
        "quality_score": quality_score,
        "quality_warnings": quality_warnings,
    }
    return canvas.convert("RGB"), diagnostics


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
    design_system = load_pinterest_design_system()

    hero_image_path = normalize_image_path(str(payload["hero_image_path"]).strip())
    base_image = load_base_image(hero_image_path)
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
        style_name = str(variant.get("style_name", "bottom-title")).strip() or "bottom-title"
        variant_type = str(variant.get("variant_type", "")).strip() or f"variant_{index}"
        if not str(variant.get("display_headline") or variant.get("title") or "").strip():
            raise ValueError(f"Variant {index} is missing a usable headline.")
        if not str(variant.get("display_subheadline") or variant.get("description") or "").strip():
            raise ValueError(f"Variant {index} is missing a usable subheadline.")

        image: Image.Image | None = None
        diagnostics: dict[str, Any] = {}
        template_preferences = design_system.get("variant_rules", {}).get(variant_type, {}).get("template_preferences", [])
        template_candidates = [str(variant.get("template_family", "")).strip()] + [name for name in template_preferences if name]
        seen_templates: set[str] = set()
        for template_name in template_candidates:
            if not template_name or template_name in seen_templates:
                continue
            seen_templates.add(template_name)
            candidate_variant = dict(variant)
            candidate_variant["template_family"] = template_name
            candidate_image, candidate_diagnostics = build_pin_image(
                base_image=base_image,
                brand_name=brand_name,
                variant=candidate_variant,
                article_payload=payload,
                design_system=design_system,
            )
            image = candidate_image
            diagnostics = candidate_diagnostics
            if candidate_diagnostics["quality_score"] >= QUALITY_SCORE_MINIMUM:
                variant["template_family"] = template_name
                break

        if image is None:
            raise ValueError(f"Variant {index} could not render a valid pin image.")

        variant["render_diagnostics"] = diagnostics
        output_path = output_dir / f"pin-{index}.png"
        image.save(output_path, format="PNG", optimize=True)
        print(
            f"[pinterest] generated {style_name} / {variant.get('template_family', 'unknown')} pin at {output_path} "
            f"(quality={diagnostics.get('quality_score', 0)})"
        )
        saved_paths.append(output_path)

    pinterest_metadata_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
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
