from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageColor, ImageDraw, ImageFont

from pinterest_pin_design import build_pin_copy_variant, classify_topic_style, load_design_system

PIN_WIDTH = 1000
PIN_HEIGHT = 1500
PIN_BORDER_RADIUS = 32
DEFAULT_QUALITY_SCORE_MINIMUM = 82
HEADLINE_HARD_MAX_LENGTH = 116
SUBHEADLINE_HARD_MAX_LENGTH = 150
TEXT_COLUMN_MIN_WIDTH = 540
TEXT_COLUMN_MAX_WIDTH = 680
LONG_TEXT_COLUMN_MAX_WIDTH = 720


@dataclass
class TextLayout:
    text: str
    lines: list[str]
    font_size: int
    line_height: int
    width: int
    height: int
    truncated: bool
    box: dict[str, int]
    balance_score: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate branded Pinterest pin assets from Pinterest metadata."
    )
    parser.add_argument("pinterest_metadata_path", type=str, help="Path to Pinterest metadata JSON.")
    parser.add_argument(
        "--variant-key",
        type=str,
        default="",
        help="Optional single variant key to render, such as pin-1.",
    )
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


def resolve_variant_base_image(payload: dict[str, Any], variant: dict[str, Any]) -> Path:
    image_reference = normalize_copy(variant.get("image_reference") or variant.get("image_focus") or "hero").lower()
    if image_reference == "hero":
        return normalize_image_path(str(payload["hero_image_path"]).strip())

    section_paths = payload.get("section_image_paths") or []
    if isinstance(section_paths, list) and image_reference.startswith("section_"):
        try:
            section_index = int(image_reference.split("_", 1)[1]) - 1
        except (IndexError, ValueError):
            section_index = -1
        if 0 <= section_index < len(section_paths):
            section_path = normalize_copy(section_paths[section_index])
            if section_path:
                resolved = normalize_image_path(section_path)
                if resolved.exists():
                    return resolved

    return normalize_image_path(str(payload["hero_image_path"]).strip())


def normalize_copy(text: str) -> str:
    if text is None:
        return ""
    cleaned = re.sub(r"\s+", " ", str(text).strip())
    if cleaned.lower() in {"none", "null", "nan"}:
        return ""
    return cleaned


def humanize_identifier(value: Any) -> str:
    cleaned = normalize_copy(str(value or "").replace("_", " ").replace("-", " "))
    return cleaned.title()


def infer_pin_topic_phrase(article_payload: dict[str, Any]) -> str:
    cluster = humanize_identifier(article_payload.get("cluster_id"))
    subtopic = humanize_identifier(article_payload.get("subtopic_id"))
    if cluster and subtopic and subtopic.lower() not in cluster.lower():
        return f"{cluster} {subtopic}".strip()
    if cluster:
        return cluster
    title = normalize_copy(article_payload.get("article_title") or "")
    title = re.sub(r"^(how to choose|how to style|best|a guide to)\s+", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*:\s*.*$", "", title).strip()
    return title or "Home Decor"


def build_legacy_render_copy(article_payload: dict[str, Any], variant: dict[str, Any]) -> tuple[str, str]:
    topic = infer_pin_topic_phrase(article_payload)
    room = humanize_identifier(article_payload.get("cluster_id")).split()[-1] if article_payload.get("cluster_id") else "space"
    variant_type = normalize_copy(variant.get("variant_type")).lower()

    if variant_type == "practical_tips":
        headline = f"How to Choose {topic}"
        subheadline = f"Simple criteria for a calmer, more functional {room.lower()}."
    elif variant_type == "product_led":
        headline = f"{topic} Worth Comparing"
        subheadline = "Start with pieces that balance function, scale, and warmth."
    elif variant_type == "styling_angle":
        headline = f"Style {topic} Without Clutter"
        subheadline = "Use finish, texture, and restraint for a more polished room."
    else:
        headline = f"Smart {topic} Ideas"
        subheadline = f"Clean choices for a calmer, more functional {room.lower()}."

    return truncate_text(headline, 64)[0], truncate_text(subheadline, 110)[0]


def resolve_render_copy(article_payload: dict[str, Any], variant: dict[str, Any]) -> tuple[str, str]:
    if normalize_copy(variant.get("hook_text")):
        headline = normalize_copy(variant.get("hook_text"))
        subheadline = normalize_copy(variant.get("support_text") or variant.get("display_subheadline") or variant.get("description"))
        return headline, subheadline

    # Older Pinterest metadata may contain stale display_headline values generated before
    # the hook strategy layer. Repair those locally instead of weakening layout validation.
    if not normalize_copy(variant.get("template_id")):
        return build_legacy_render_copy(article_payload, variant)

    headline = normalize_copy(variant.get("title") or variant.get("display_headline"))
    subheadline = normalize_copy(variant.get("support_text") or variant.get("display_subheadline") or variant.get("description"))
    return headline, subheadline


def truncate_text(text: str, max_length: int) -> tuple[str, bool]:
    cleaned = normalize_copy(text)
    if len(cleaned) <= max_length:
        return cleaned, False
    shortened = cleaned[: max_length - 3].rsplit(" ", 1)[0].strip()
    return ((shortened or cleaned[: max_length - 3]).rstrip(" ,;:-") + "..."), True


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


def measure_text(font: ImageFont.FreeTypeFont | ImageFont.ImageFont, text: str) -> tuple[int, int]:
    dummy = Image.new("RGBA", (10, 10), (0, 0, 0, 0))
    draw = ImageDraw.Draw(dummy)
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    return right - left, bottom - top


def measure_text_bbox(
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    text: str,
) -> tuple[int, int, int, int]:
    dummy = Image.new("RGBA", (10, 10), (0, 0, 0, 0))
    draw = ImageDraw.Draw(dummy)
    return draw.textbbox((0, 0), text, font=font)


def anchored_x(x: int, anchor: str, text_width: int) -> int:
    if anchor == "middle":
        return int(x - (text_width / 2))
    return x


def crop_to_pin_size(image: Image.Image, mode: str = "full_bleed") -> Image.Image:
    source_ratio = image.width / image.height
    target_ratio = PIN_WIDTH / PIN_HEIGHT
    if source_ratio > target_ratio:
        new_width = int(image.height * target_ratio)
        left_ratio = {"left_focus": 0.25, "right_weighted": 0.75}.get(mode, 0.5)
        left = max(0, int((image.width - new_width) * left_ratio))
        image = image.crop((left, 0, left + new_width, image.height))
    else:
        new_height = int(image.width / target_ratio)
        top_ratio = {"top_crop": 0.18, "bottom_showcase": 0.78}.get(mode, 0.5)
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
    else:
        for y in range(PIN_HEIGHT):
            alpha = int(max_alpha * 0.2 + (y / PIN_HEIGHT) * max_alpha * 0.8)
            draw.line([(0, y), (PIN_WIDTH, y)], fill=(12, 10, 8, alpha))
    return Image.alpha_composite(base, overlay)


def rounded_mask(size: tuple[int, int], radius: int) -> Image.Image:
    mask = Image.new("L", size, 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, size[0], size[1]), radius=radius, fill=255)
    return mask


def draw_rounded_panel(canvas: Image.Image, panel: dict[str, int | str]) -> None:
    width = int(panel.get("width", 0))
    height = int(panel.get("height", 0))
    if width <= 0 or height <= 0:
        return
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    draw.rounded_rectangle(
        (
            int(panel.get("x", 0)),
            int(panel.get("y", 0)),
            int(panel.get("x", 0)) + width,
            int(panel.get("y", 0)) + height,
        ),
        radius=int(panel.get("radius", 28)),
        fill=parse_rgba(str(panel.get("fill", "rgba(255,255,255,0.9)"))),
    )
    canvas.alpha_composite(overlay)


def build_content_box(
    container: dict[str, int],
    *,
    padding_x: int,
    padding_y: int,
    safe_margin: int,
) -> dict[str, int]:
    x = max(safe_margin, int(container["x"]) + padding_x)
    y = max(safe_margin, int(container["y"]) + padding_y)
    width = min(
        PIN_WIDTH - safe_margin - x,
        int(container["width"]) - padding_x * 2,
    )
    height = min(
        PIN_HEIGHT - safe_margin - y,
        int(container["height"]) - padding_y * 2,
    )
    return {"x": x, "y": y, "width": max(0, width), "height": max(0, height)}


def wrap_words_to_width(
    text: str,
    *,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    max_width: int,
    max_lines: int,
) -> tuple[list[str], bool]:
    words = normalize_copy(text).split()
    if not words:
        return [], False

    lines: list[str] = []
    current = ""
    truncated = False
    for word in words:
        candidate = f"{current} {word}".strip()
        width, _ = measure_text(font, candidate)
        if not current or width <= max_width:
            current = candidate
            continue
        lines.append(current)
        current = word
        if len(lines) >= max_lines:
            truncated = True
            break

    if not truncated and current:
        lines.append(current)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        truncated = True

    if truncated and lines:
        last = lines[-1]
        while last:
            candidate = (last.rstrip(" ,;:-") + "...").strip()
            width, _ = measure_text(font, candidate)
            if width <= max_width:
                lines[-1] = candidate
                break
            if " " not in last:
                lines[-1] = candidate[: max(1, len(candidate) - 1)]
                break
            last = last.rsplit(" ", 1)[0]
    return [line for line in lines if line.strip()], truncated


def rebalance_lines(
    lines: list[str],
    *,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    max_width: int,
) -> list[str]:
    if len(lines) < 2:
        return lines
    balanced = list(lines)
    improved = True
    while improved:
        improved = False
        for index in range(len(balanced) - 1):
            current_words = balanced[index].split()
            next_words = balanced[index + 1].split()
            if len(current_words) <= 1 or not next_words:
                continue
            candidate_current = " ".join(current_words[:-1])
            candidate_next = " ".join([current_words[-1], *next_words])
            current_width, _ = measure_text(font, candidate_current)
            next_width, _ = measure_text(font, candidate_next)
            if current_width > max_width or next_width > max_width:
                continue
            old_delta = abs(measure_text(font, balanced[index])[0] - measure_text(font, balanced[index + 1])[0])
            new_delta = abs(current_width - next_width)
            if new_delta + 12 < old_delta:
                balanced[index] = candidate_current
                balanced[index + 1] = candidate_next
                improved = True
    return balanced


def compute_line_balance_score(
    lines: list[str],
    *,
    font: ImageFont.FreeTypeFont | ImageFont.ImageFont,
    max_width: int,
) -> float:
    if not lines:
        return 0.0
    widths = [measure_text(font, line)[0] for line in lines]
    if len(widths) == 1:
        return 1.0
    width_ratios = [width / max_width for width in widths if max_width > 0]
    avg_ratio = sum(width_ratios) / len(width_ratios)
    variance = sum(abs(ratio - avg_ratio) for ratio in width_ratios) / len(width_ratios)
    shortest = min(widths)
    longest = max(widths)
    shortest_ratio = shortest / longest if longest else 1.0
    return max(0.0, 1.0 - min(1.0, variance * 1.8) - max(0.0, (0.52 - shortest_ratio) * 0.8))


def fit_text_layout(
    *,
    text: str,
    font_loader,
    box: dict[str, int],
    max_font_size: int,
    min_font_size: int,
    max_lines: int,
    line_height_ratio: float,
    min_line_height: int,
    hard_max_length: int,
) -> TextLayout | None:
    cropped_text, truncated_from_length = truncate_text(text, hard_max_length)
    best_layout: TextLayout | None = None
    best_score = -1.0
    for font_size in range(max_font_size, min_font_size - 1, -2):
        font = font_loader(font_size)
        lines, truncated_from_wrap = wrap_words_to_width(
            cropped_text,
            font=font,
            max_width=box["width"],
            max_lines=max_lines,
        )
        if not lines:
            continue
        lines = rebalance_lines(lines, font=font, max_width=box["width"])
        line_height = max(min_line_height, int(font_size * line_height_ratio))
        max_line_width = max(measure_text(font, line)[0] for line in lines)
        total_height = line_height * len(lines)
        if max_line_width <= box["width"] and total_height <= box["height"]:
            balance_score = compute_line_balance_score(lines, font=font, max_width=box["width"])
            fill_ratio = total_height / box["height"] if box["height"] else 1.0
            comfort_penalty = max(0.0, fill_ratio - 0.78) * 1.5
            score = (font_size / max_font_size) * 0.45 + balance_score * 0.4 + (1.0 - comfort_penalty) * 0.15
            layout = TextLayout(
                text=cropped_text,
                lines=lines,
                font_size=font_size,
                line_height=line_height,
                width=max_line_width,
                height=total_height,
                truncated=truncated_from_length or truncated_from_wrap,
                box=box,
                balance_score=balance_score,
            )
            if score > best_score:
                best_layout = layout
                best_score = score
    return best_layout


def pick_density_bucket(headline: str, subheadline: str) -> str:
    headline_length = len(normalize_copy(headline))
    subheadline_length = len(normalize_copy(subheadline))
    combined = headline_length + subheadline_length
    if headline_length >= 68 or combined >= 170:
        return "long"
    if headline_length <= 38 and combined <= 108:
        return "short"
    return "medium"


def select_template_candidates(
    *,
    variant: dict[str, Any],
    design_system: dict[str, Any],
    density: str,
) -> list[str]:
    templates = design_system.get("template_families", {})
    preferred = []
    if str(variant.get("template_family", "")).strip():
        preferred.append(str(variant["template_family"]).strip())
    variant_type = str(variant.get("variant_type", "")).strip()
    preferred.extend(design_system.get("variant_rules", {}).get(variant_type, {}).get("template_preferences", []))

    ordered: list[str] = []
    seen: set[str] = set()
    for name in preferred:
        if name in templates and name not in seen:
            ordered.append(name)
            seen.add(name)

    density_names = [
        name for name, template in templates.items()
        if str(template.get("density_profile", "medium")) == density and name not in seen
    ]
    for name in density_names:
        ordered.append(name)
        seen.add(name)

    for name in templates:
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def choose_composition_class(
    *,
    template_name: str,
    density: str,
    design_system: dict[str, Any],
) -> str:
    compositions = design_system.get("composition_classes", {})
    if density == "long":
        if template_name in {"minimal_frame_long", "editorial_split_long", "utility_stack_long"}:
            return "long_title_premium"
        return "dense_title_safe"
    if template_name in {"minimal_frame", "minimal_frame_long"}:
        return "image_forward"
    if density == "short":
        return "minimal_copy"
    return "spacious_editorial"


def render_text_block(
    canvas: Image.Image,
    *,
    layout: TextLayout,
    font_loader,
    fill: tuple[int, int, int, int],
    stroke_fill: tuple[int, int, int, int] | None = None,
    stroke_width: int = 0,
) -> None:
    draw = ImageDraw.Draw(canvas)
    font = font_loader(layout.font_size)
    baseline_y = layout.box["y"]
    for index, line in enumerate(layout.lines):
        _, top, _, _ = measure_text_bbox(font, line)
        draw_y = baseline_y + index * layout.line_height - top
        draw.text(
            (layout.box["x"], draw_y),
            line,
            font=font,
            fill=fill,
            stroke_width=stroke_width,
            stroke_fill=stroke_fill,
        )


def draw_brand_label(canvas: Image.Image, brand_name: str, *, x: int, y: int, font_size: int) -> int:
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    label = normalize_copy(brand_name).upper()
    font = ui_font(font_size)
    label_width, label_height = measure_text(font, label)
    padding_x = 18
    padding_y = 12
    rect_x = x - padding_x
    rect_y = y
    rect_width = label_width + padding_x * 2
    rect_height = label_height + padding_y * 2
    draw.rounded_rectangle(
        (rect_x, rect_y, rect_x + rect_width, rect_y + rect_height),
        radius=22,
        fill=(255, 255, 255, 150),
    )
    draw.text((x, rect_y + padding_y - 2), label, font=font, fill=(24, 21, 18, 230))
    canvas.alpha_composite(overlay)
    return rect_height


def measure_brand_label_height(brand_name: str, *, font_size: int) -> int:
    label = normalize_copy(brand_name).upper()
    font = ui_font(font_size)
    _, label_height = measure_text(font, label)
    padding_y = 12
    return label_height + padding_y * 2


def draw_kicker(canvas: Image.Image, text: str, *, x: int, y: int, font_size: int, accent: tuple[int, int, int, int]) -> int:
    draw = ImageDraw.Draw(canvas)
    font = ui_font(font_size)
    label = normalize_copy(text).upper()
    draw.text((x, y), label, font=font, fill=accent)
    width, height = measure_text(font, label)
    draw.line((x, y + height + 8, x + width + 14, y + height + 8), fill=accent, width=3)
    return height + 12


def measure_kicker_height(text: str, *, font_size: int) -> int:
    font = ui_font(font_size)
    label = normalize_copy(text).upper()
    _, height = measure_text(font, label)
    return height + 12


def draw_cta_chip(
    canvas: Image.Image,
    text: str,
    *,
    x: int,
    y: int,
    font_size: int,
    accent: tuple[int, int, int, int],
    padding_x: int,
    padding_y: int,
) -> tuple[int, int]:
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    font = ui_font(font_size)
    label = normalize_copy(text)
    left, top, right, bottom = measure_text_bbox(font, label)
    width = right - left
    height = bottom - top
    chip_width = width + padding_x * 2
    chip_height = height + padding_y * 2
    draw.rounded_rectangle((x, y, x + chip_width, y + chip_height), radius=18, fill=accent)
    text_x = x + padding_x - left
    text_y = y + ((chip_height - height) / 2) - top
    draw.text((text_x, text_y), label, font=font, fill=(255, 255, 255, 255))
    canvas.alpha_composite(overlay)
    return chip_width, chip_height


def measure_cta_chip(
    text: str,
    *,
    font_size: int,
    padding_x: int,
    padding_y: int,
) -> tuple[int, int]:
    font = ui_font(font_size)
    label = normalize_copy(text)
    left, top, right, bottom = measure_text_bbox(font, label)
    width = right - left
    height = bottom - top
    return width + padding_x * 2, height + padding_y * 2


def validate_layout(
    *,
    headline_layout: TextLayout | None,
    subheadline_layout: TextLayout | None,
    content_box: dict[str, int],
    used_height: int,
    show_cta: bool,
    cta_height: int,
    diagnostics: dict[str, Any],
    design_system: dict[str, Any],
) -> tuple[int, list[str]]:
    rules = design_system.get("layout_rules", {})
    validation = rules.get("validation", {})
    score = 100
    warnings: list[str] = []

    if headline_layout is None:
        score -= 60
        warnings.append("headline could not fit")
    else:
        if headline_layout.width > headline_layout.box["width"] or headline_layout.height > headline_layout.box["height"]:
            score -= 40
            warnings.append("headline overflow")
        if headline_layout.font_size < int(validation.get("min_headline_font_size", 48)):
            score -= 14
            warnings.append("headline font fell below premium threshold")
        if headline_layout.truncated:
            score -= 28
            warnings.append("headline truncated")
        if headline_layout.balance_score < 0.62:
            score -= 18
            warnings.append("headline line breaks are unbalanced")
        title_density = headline_layout.height / content_box["height"] if content_box["height"] else 1.0
        if title_density > 0.52:
            score -= 10
            warnings.append("headline block is too dominant for the card")

    if subheadline_layout is None and diagnostics.get("subheadline_requested"):
        score -= 18
        warnings.append("subheadline dropped")
    elif subheadline_layout is not None:
        if subheadline_layout.width > subheadline_layout.box["width"] or subheadline_layout.height > subheadline_layout.box["height"]:
            score -= 25
            warnings.append("subheadline overflow")
        if subheadline_layout.font_size < int(validation.get("min_subheadline_font_size", 18)):
            score -= 10
            warnings.append("subheadline font too small")
        if subheadline_layout.truncated:
            score -= 5
            warnings.append("subheadline truncated")
        if headline_layout is not None:
            ratio = subheadline_layout.font_size / max(1, headline_layout.font_size)
            if ratio < 0.33:
                score -= 12
                warnings.append("subtitle hierarchy is too weak against the headline")

    if used_height > content_box["height"]:
        score -= 40
        warnings.append("content overflowed vertical safe area")
    unused_height = max(0, content_box["height"] - used_height)
    if unused_height > int(validation.get("max_unused_vertical_space", 250)):
        score -= 14
        warnings.append("layout left excessive dead space")
    fill_ratio = used_height / content_box["height"] if content_box["height"] else 1.0
    if fill_ratio < 0.34:
        score -= 14
        warnings.append("content stack is too sparse for the card height")
    elif fill_ratio < 0.42:
        score -= 8
        warnings.append("content stack feels underfilled")

    if show_cta and cta_height <= 0:
        score -= 12
        warnings.append("cta missing")
    if diagnostics.get("composition_class") == "dense_title_safe" and diagnostics.get("show_brand_label"):
        score -= 8
        warnings.append("top label crowds a dense title layout")
    return max(0, score), warnings


def build_pin_image(
    *,
    base_image: Image.Image,
    brand_name: str,
    variant: dict[str, Any],
    article_payload: dict[str, Any],
    design_system: dict[str, Any],
    template_name: str,
) -> tuple[Image.Image, dict[str, Any]]:
    topic_style_key = str(variant.get("topic_style") or classify_topic_style(article_payload)).strip() or "editorial"
    topic_style = dict(design_system.get("topic_styles", {}).get(topic_style_key, {}))
    template = dict(design_system.get("template_families", {}).get(template_name, {}))
    if not template:
        raise ValueError(f"Unknown template family: {template_name}")

    layout_rules = design_system.get("layout_rules", {})
    spacing = layout_rules.get("spacing", {})
    kicker_rules = layout_rules.get("kicker", {})
    brand_rules = layout_rules.get("brand_label", {})
    cta_rules = layout_rules.get("cta", {})
    canvas_rules = design_system.get("canvas", {})
    safe_margin = int(canvas_rules.get("safe_margin", 68))

    variant_type = str(variant.get("variant_type", "")).strip()
    generated_copy = build_pin_copy_variant(article_payload, variant_type=variant_type)
    has_pin_specific_copy = bool(variant.get("display_headline")) and bool(variant.get("display_subheadline"))
    resolved_headline, resolved_subheadline = resolve_render_copy(article_payload, variant)

    headline_raw, _ = truncate_text(
        str(
            resolved_headline
            or generated_copy.get("headline")
            or ""
        ),
        HEADLINE_HARD_MAX_LENGTH,
    )
    subheadline_raw, _ = truncate_text(
        str(
            resolved_subheadline
            or generated_copy.get("subheadline")
            or ""
        ),
        SUBHEADLINE_HARD_MAX_LENGTH,
    )
    headline = normalize_copy(headline_raw)
    subheadline = normalize_copy(subheadline_raw)
    kicker_source = variant.get("display_kicker") if has_pin_specific_copy else generated_copy.get("kicker")
    kicker = normalize_copy(str(kicker_source or ""))
    cta_source = variant.get("cta_label") if has_pin_specific_copy else generated_copy.get("cta_label")
    cta_label = normalize_copy(str(cta_source or design_system.get("brand", {}).get("cta_text") or ""))

    density = pick_density_bucket(headline, subheadline)
    composition_class = choose_composition_class(
        template_name=template_name,
        density=density,
        design_system=design_system,
    )
    composition = dict(design_system.get("composition_classes", {}).get(composition_class, {}))
    canvas = Image.new("RGBA", (PIN_WIDTH, PIN_HEIGHT), parse_rgba(design_system["brand"]["background"]))
    hero = crop_to_pin_size(base_image.copy(), mode=str(template.get("image_mode", "full_bleed")))
    hero.putalpha(rounded_mask(hero.size, PIN_BORDER_RADIUS))
    canvas.alpha_composite(hero)
    canvas = apply_gradient_overlay(
        canvas,
        str(template.get("overlay_mode", "bottom_fade")),
        overlay_strength=float(topic_style.get("overlay_strength", 0.22)),
    )

    panel = dict(template.get("panel", {}))
    panel["fill"] = str(topic_style.get("panel", design_system["brand"]["paper"]))

    if int(panel.get("width", 0)) > 0 and int(panel.get("height", 0)) > 0:
        max_panel = {"x": int(panel["x"]), "y": int(panel["y"]), "width": int(panel["width"]), "height": int(panel["height"])}
        content_box = build_content_box(
            max_panel,
            padding_x=int(spacing.get("panel_inner_padding_x", 28)),
            padding_y=int(spacing.get("panel_inner_padding_y", 26)),
            safe_margin=safe_margin,
        )
    else:
        max_panel = None
        headline_box = template["headline_box"]
        subheadline_box = template["subheadline_box"]
        min_x = min(int(headline_box["x"]), int(subheadline_box["x"]))
        min_y = max(safe_margin, min(int(headline_box["y"]), int(subheadline_box["y"])) - 110)
        max_width = max(int(headline_box["width"]), int(subheadline_box["width"]))
        content_box = {
            "x": min_x,
            "y": min_y,
            "width": min(PIN_WIDTH - safe_margin - min_x, max_width),
            "height": PIN_HEIGHT - safe_margin - min_y,
        }
    max_text_column_width = LONG_TEXT_COLUMN_MAX_WIDTH if density == "long" else TEXT_COLUMN_MAX_WIDTH
    content_box["width"] = max(TEXT_COLUMN_MIN_WIDTH, min(max_text_column_width, content_box["width"]))

    accent_color = parse_rgba(str(topic_style.get("accent", design_system["brand"]["accent"])))
    ink = parse_rgba(design_system["brand"]["ink"])
    muted_ink = parse_rgba(design_system["brand"]["muted_ink"])
    template_has_panel = int(panel.get("width", 0)) > 0 and int(panel.get("height", 0)) > 0
    headline_fill = ink if template_has_panel or template_name not in {"minimal_frame", "minimal_frame_long"} else (255, 255, 255, 255)
    subheadline_fill = muted_ink if template_has_panel or template_name not in {"minimal_frame", "minimal_frame_long"} else (255, 255, 255, 230)

    current_y = content_box["y"]
    current_y += int(content_box["height"] * float(composition.get("start_offset_ratio", 0.0)))
    show_brand_label = bool(composition.get("show_brand_label", True))
    show_kicker = bool(composition.get("show_kicker", True))
    brand_height = 0
    if show_brand_label:
        brand_height = measure_brand_label_height(
            brand_name,
            font_size=int(brand_rules.get("font_size", 22)),
        )
        current_y += brand_height + int(composition.get("brand_gap_after", brand_rules.get("gap_after", 30)))

    if kicker and show_kicker:
        kicker_height = measure_kicker_height(
            kicker,
            font_size=int(kicker_rules.get("font_size", 18)),
        )
        current_y += kicker_height + int(composition.get("kicker_gap_after", kicker_rules.get("gap_after", 28)))

    remaining_height = max(0, content_box["height"] - (current_y - content_box["y"]))
    headline_box = {"x": content_box["x"], "y": current_y, "width": content_box["width"], "height": remaining_height}
    headline_layout = fit_text_layout(
        text=headline,
        font_loader=title_font,
        box=headline_box,
        max_font_size=max(42, int(int(template.get("headline_font_size", 72)) * float(composition.get("headline_scale", 1.0)))),
        min_font_size=max(
            int(template.get("headline_min_font_size", 48)),
            int(layout_rules.get("headline", {}).get("min_font_size", 46)),
        ),
        max_lines=min(
            int(template.get("headline_max_lines", 4)),
            int(layout_rules.get("headline", {}).get("max_lines", 5)),
        ),
        line_height_ratio=float(layout_rules.get("headline", {}).get("line_height_ratio", 1.08)),
        min_line_height=int(layout_rules.get("headline", {}).get("min_line_height", 52)),
        hard_max_length=HEADLINE_HARD_MAX_LENGTH,
    )

    if headline_layout is not None:
        current_y = headline_layout.box["y"] + headline_layout.height + int(composition.get("headline_to_subheadline", spacing.get("headline_to_subheadline", 22)))

    cta_enabled = bool(cta_label)
    cta_reserved_height = 0
    if cta_enabled and cta_label:
        _, cta_measured_height = measure_cta_chip(
            cta_label,
            font_size=int(cta_rules.get("font_size", 19)),
            padding_x=int(cta_rules.get("padding_x", 18)),
            padding_y=int(cta_rules.get("padding_y", 10)),
        )
        cta_reserved_height = cta_measured_height + int(spacing.get("subheadline_to_cta", 26))

    subheadline_layout: TextLayout | None = None
    if subheadline:
        subheadline_box = {
            "x": content_box["x"],
            "y": current_y,
            "width": content_box["width"],
            "height": max(0, content_box["y"] + content_box["height"] - current_y - cta_reserved_height),
        }
        subheadline_layout = fit_text_layout(
            text=subheadline,
            font_loader=ui_font,
            box=subheadline_box,
            max_font_size=max(18, int(int(template.get("subheadline_font_size", 24)) * float(composition.get("subtitle_scale", 1.0)))),
            min_font_size=max(
                int(template.get("subheadline_min_font_size", 18)),
                int(layout_rules.get("subheadline", {}).get("min_font_size", 18)),
            ),
            max_lines=min(
                int(template.get("subheadline_max_lines", 3)),
                int(layout_rules.get("subheadline", {}).get("max_lines", 4)),
            ),
            line_height_ratio=float(layout_rules.get("subheadline", {}).get("line_height_ratio", 1.3)),
            min_line_height=int(layout_rules.get("subheadline", {}).get("min_line_height", 24)),
            hard_max_length=SUBHEADLINE_HARD_MAX_LENGTH,
        )
        if subheadline_layout is not None:
            current_y = subheadline_layout.box["y"] + subheadline_layout.height + int(composition.get("subheadline_to_cta", spacing.get("subheadline_to_cta", 26)))

    cta_height = 0
    cta_width = 0
    if cta_enabled and cta_label:
        cta_width, cta_height = measure_cta_chip(
            cta_label,
            font_size=int(cta_rules.get("font_size", 19)),
            padding_x=int(cta_rules.get("padding_x", 18)),
            padding_y=int(cta_rules.get("padding_y", 10)),
        )
        current_y += cta_height

    used_height = current_y - content_box["y"]

    if max_panel is not None:
        padding_x = int(spacing.get("panel_inner_padding_x", 28))
        padding_y = int(spacing.get("panel_inner_padding_y", 26))
        rendered_widths = [
            layout.width for layout in (headline_layout, subheadline_layout) if layout is not None
        ]
        if cta_width:
            rendered_widths.append(cta_width)
        content_width = max(rendered_widths) if rendered_widths else content_box["width"]
        desired_content_width = max(TEXT_COLUMN_MIN_WIDTH, min(max_text_column_width, content_width + 12))
        min_panel_width = min(max_panel["width"], TEXT_COLUMN_MIN_WIDTH + padding_x * 2)
        max_panel_width = min(max_panel["width"], max_text_column_width + padding_x * 2)
        panel["width"] = max(min_panel_width, min(max_panel_width, desired_content_width + padding_x * 2))
        panel["height"] = max(
            min(max_panel["height"], 260),
            min(max_panel["height"], used_height + padding_y * 2 + 12),
        )
        panel["x"] = max_panel["x"]
        panel["y"] = max_panel["y"]
        draw_rounded_panel(canvas, panel)

    if show_brand_label:
        brand_height = draw_brand_label(
            canvas,
            brand_name=brand_name,
            x=content_box["x"],
            y=content_box["y"] + int(content_box["height"] * float(composition.get("start_offset_ratio", 0.0))),
            font_size=int(brand_rules.get("font_size", 22)),
        )

    if kicker and show_kicker:
        kicker_y = content_box["y"] + int(content_box["height"] * float(composition.get("start_offset_ratio", 0.0))) + (
            brand_height + int(composition.get("brand_gap_after", brand_rules.get("gap_after", 30))) if show_brand_label else 0
        )
        draw_kicker(
            canvas,
            kicker,
            x=content_box["x"],
            y=kicker_y,
            font_size=int(kicker_rules.get("font_size", 18)),
            accent=accent_color,
        )

    if headline_layout is not None:
        render_text_block(
            canvas,
            layout=headline_layout,
            font_loader=title_font,
            fill=headline_fill,
            stroke_fill=(0, 0, 0, 48) if template_name in {"minimal_frame", "minimal_frame_long"} and not template_has_panel else None,
            stroke_width=2 if template_name in {"minimal_frame", "minimal_frame_long"} and not template_has_panel else 0,
        )

    if subheadline_layout is not None:
        render_text_block(
            canvas,
            layout=subheadline_layout,
            font_loader=ui_font,
            fill=subheadline_fill,
        )

    if cta_enabled and cta_label:
        draw_cta_chip(
            canvas,
            cta_label,
            x=content_box["x"],
            y=content_box["y"] + used_height - cta_height,
            font_size=int(cta_rules.get("font_size", 19)),
            accent=accent_color,
            padding_x=int(cta_rules.get("padding_x", 18)),
            padding_y=int(cta_rules.get("padding_y", 10)),
        )
    diagnostics = {
        "template_family": template_name,
        "composition_class": composition_class,
        "topic_style": topic_style_key,
        "content_density": density,
        "headline_font_size": headline_layout.font_size if headline_layout else None,
        "subheadline_font_size": subheadline_layout.font_size if subheadline_layout else None,
        "headline_truncated": bool(headline_layout.truncated) if headline_layout else False,
        "subheadline_truncated": bool(subheadline_layout.truncated) if subheadline_layout else False,
        "subheadline_requested": bool(subheadline),
        "show_brand_label": show_brand_label,
        "used_height": used_height,
        "content_box_height": content_box["height"],
    }
    quality_score, quality_warnings = validate_layout(
        headline_layout=headline_layout,
        subheadline_layout=subheadline_layout,
        content_box=content_box,
        used_height=used_height,
        show_cta=cta_enabled and bool(cta_label),
        cta_height=cta_height,
        diagnostics=diagnostics,
        design_system=design_system,
    )
    diagnostics["quality_score"] = quality_score
    diagnostics["quality_warnings"] = quality_warnings
    return canvas.convert("RGB"), diagnostics


def load_brand_name(project_root: Path) -> str:
    config_path = project_root / "_config.yml"
    for line in config_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("title:"):
            return stripped.split(":", 1)[1].strip().strip('"')
    return "The Livin' Edit"


def generate_pin_assets(pinterest_metadata_path: Path, variant_key: str = "") -> list[Path]:
    project_root = Path(__file__).resolve().parents[2]
    payload = load_json(pinterest_metadata_path)
    design_system = load_pinterest_design_system()
    quality_minimum = int(design_system.get("layout_rules", {}).get("validation", {}).get("min_quality_score", DEFAULT_QUALITY_SCORE_MINIMUM))

    slug = str(payload["article_slug"]).strip()
    brand_name = load_brand_name(project_root)

    output_dir = project_root / "assets" / "pins" / slug
    output_dir.mkdir(parents=True, exist_ok=True)

    saved_paths: list[Path] = []
    variants = payload.get("variants", [])
    if not isinstance(variants, list) or not variants:
        raise ValueError("Pinterest metadata must contain a non-empty variants list.")
    selected_variant_key = normalize_copy(variant_key).lower()
    if len(variants) < 4 and not selected_variant_key:
        raise ValueError("Pinterest metadata must contain at least 4 variants.")

    for index, variant in enumerate(variants, start=1):
        current_variant_key = normalize_copy(variant.get("variant_key") or f"pin-{index}").lower()
        if selected_variant_key and current_variant_key != selected_variant_key:
            continue

        style_name = str(variant.get("style_name", "bottom-title")).strip() or "bottom-title"
        variant_type = str(variant.get("variant_type", "")).strip() or f"variant_{index}"
        headline, subheadline = resolve_render_copy(payload, variant)
        if not headline:
            raise ValueError(f"Variant {index} is missing a usable headline.")

        variant["hook_text"] = headline
        variant["support_text"] = subheadline

        image_reference = variant.get("image_reference") or variant.get("image_focus") or "hero"
        base_image_path = resolve_variant_base_image(payload, variant)
        base_image = load_base_image(base_image_path)

        print(
            f"[pinterest] rendering variant {index}: "
            f"hook='{headline}' | template='{variant.get('template_id') or variant.get('template_family')}' "
            f"| image='{image_reference}' -> '{base_image_path.name}'"
        )

        image: Image.Image | None = None
        diagnostics: dict[str, Any] = {}
        best_score = -1
        density = pick_density_bucket(headline, subheadline)
        candidate_templates = select_template_candidates(
            variant=variant,
            design_system=design_system,
            density=density,
        )

        for template_name in candidate_templates:
            candidate_image, candidate_diagnostics = build_pin_image(
                base_image=base_image,
                brand_name=brand_name,
                variant=variant,
                article_payload=payload,
                design_system=design_system,
                template_name=template_name,
            )
            candidate_score = int(candidate_diagnostics.get("quality_score", 0))
            if candidate_score > best_score:
                best_score = candidate_score
                image = candidate_image
                diagnostics = candidate_diagnostics
                variant["template_family"] = template_name
            if candidate_score >= quality_minimum:
                image = candidate_image
                diagnostics = candidate_diagnostics
                variant["template_family"] = template_name
                break

        if image is None:
            raise ValueError(f"Variant {index} could not render a valid pin image.")

        if int(diagnostics.get("quality_score", 0)) < quality_minimum:
            raise ValueError(
                f"Variant {index} failed layout validation ({diagnostics.get('quality_score', 0)}): "
                f"{', '.join(diagnostics.get('quality_warnings', []))}"
            )
        if bool(diagnostics.get("headline_truncated")) and variant_type in {"trend_overview", "practical_tips"}:
            raise ValueError(
                f"Variant {index} rejected because the headline still truncates in a primary layout."
            )

        variant["render_diagnostics"] = diagnostics
        output_path = output_dir / f"pin-{index}.png"
        image.save(output_path, format="PNG", optimize=True)
        print(
            f"[pinterest] generated {style_name} / {variant.get('template_family', 'unknown')} pin at {output_path} "
            f"(quality={diagnostics.get('quality_score', 0)})"
        )
        saved_paths.append(output_path)

    if selected_variant_key and not saved_paths:
        raise ValueError(f"No Pinterest variant found for variant key: {variant_key}")

    pinterest_metadata_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return saved_paths


def main() -> int:
    args = parse_args()
    try:
        saved_paths = generate_pin_assets(Path(args.pinterest_metadata_path), variant_key=args.variant_key)
        for path in saved_paths:
            print(path)
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
