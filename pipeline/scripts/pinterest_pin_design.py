from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

DESIGN_SYSTEM_PATH = Path(__file__).resolve().parents[1] / 'data' / 'pinterest_pin_design_system.json'


def load_design_system(path: Path = DESIGN_SYSTEM_PATH) -> dict[str, Any]:
    raw = path.read_text(encoding='utf-8-sig')
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f'Pinterest design system must be a JSON object: {path}')
    return data


def normalize_text(value: Any) -> str:
    normalized = str(value or '').strip()
    return re.sub(r'\s+', ' ', normalized)


STOP_WORDS = {
    'the', 'a', 'an', 'for', 'your', 'with', 'that', 'this', 'guide', 'ideas', 'idea',
    'tips', 'tip', 'best', 'how', 'style', 'styling', 'decor', 'to', 'of'
}

HEADLINE_FILLER_PATTERNS = [
    r'^\s*the\s+',
    r'\s*:\s*a\s+practical guide.*$',
    r'\s*:\s*a\s+clear guide.*$',
    r'\s*:\s*practical tips.*$',
]


def simplify_topic_phrase(text: str) -> str:
    cleaned = normalize_text(text)
    cleaned = re.sub(r'^[Tt]he\s+', '', cleaned)
    cleaned = re.sub(r'\s*:\s*.*$', '', cleaned)
    cleaned = cleaned.strip(' .')
    return cleaned


def shorten_pin_headline(text: str) -> str:
    cleaned = normalize_text(text)
    if not cleaned:
        return cleaned
    for pattern in HEADLINE_FILLER_PATTERNS:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s*:\s*.*$', '', cleaned)
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip(' .:')
    return cleaned or normalize_text(text)


def tighten_pin_subheadline(text: str) -> str:
    cleaned = normalize_text(text)
    if not cleaned:
        return cleaned
    parts = re.split(r'(?<=[.!?])\s+', cleaned)
    first_sentence = parts[0].strip() if parts else cleaned
    if len(first_sentence) <= 120:
        return first_sentence
    shortened = first_sentence[:117].rsplit(' ', 1)[0].strip()
    return (shortened or first_sentence[:117]).rstrip(' ,;:-') + '...'


ROOM_HINTS = ['living room', 'bedroom', 'bathroom', 'kitchen', 'entryway', 'balcony', 'patio', 'nursery']


def extract_room_phrase(*values: Any) -> str:
    haystack = ' '.join(normalize_text(value).lower() for value in values if value)
    for room in ROOM_HINTS:
        if room in haystack:
            return room
    return 'your space'


def classify_topic_style(article_metadata: dict[str, Any]) -> str:
    intent_id = str(article_metadata.get('intent_id') or '').strip().lower()
    angle_id = str(article_metadata.get('angle_id') or '').strip().lower()
    if intent_id in {'comparison', 'decision_making'} or angle_id == 'best_options':
        return 'decision'
    if intent_id in {'implementation', 'problem_solving'} or angle_id in {'how_to', 'mistakes'}:
        return 'tutorial'
    if intent_id == 'inspiration' or angle_id == 'ideas':
        return 'editorial'
    return 'insight'


def build_pin_copy_variant(article_metadata: dict[str, Any], *, variant_type: str) -> dict[str, str]:
    title = normalize_text(article_metadata.get('title', ''))
    primary_keyword = normalize_text(article_metadata.get('primary_keyword', '') or title)
    meta_description = normalize_text(article_metadata.get('meta_description', ''))
    cluster_name = normalize_text(article_metadata.get('cluster_name', '') or article_metadata.get('subtopic_name', ''))
    room_phrase = extract_room_phrase(title, primary_keyword, cluster_name)
    topic_phrase = simplify_topic_phrase(primary_keyword or title)
    room_label = simplify_topic_phrase(cluster_name or topic_phrase)
    if room_phrase == 'your space' and room_label:
        room_phrase = room_label
    elif room_phrase != 'your space' and room_label and room_phrase in room_label.lower():
        room_phrase = room_label
    style = classify_topic_style(article_metadata)

    if variant_type == 'practical_tips':
        headline = f'What Actually Works in a {room_phrase.title()}' if room_phrase != 'your space' else f'What Actually Works for {topic_phrase}'
        subheadline = f'Useful design moves to make {topic_phrase.lower()} feel calmer, clearer, and more pulled together.'
        kicker = 'Practical guide'
        cta = 'Use these ideas'
    elif variant_type == 'product_led':
        headline = f'Before You Buy for a {room_phrase.title()}' if room_phrase != 'your space' else f'What to Look for in {topic_phrase}'
        subheadline = f'A decision-friendly breakdown of the materials, shapes, and details worth prioritizing.'
        kicker = 'Worth comparing'
        cta = 'Compare the options'
    elif variant_type == 'styling_angle':
        headline = f'The Detail That Changes {room_phrase.title()}' if room_phrase != 'your space' else f'The Styling Shift That Changes the Room'
        subheadline = f'An editorial take on how {topic_phrase.lower()} can feel more elevated without getting overdone.'
        kicker = 'Editorial angle'
        cta = 'See the styling move'
    else:
        headline = simplify_topic_phrase(title) or simplify_topic_phrase(primary_keyword)
        subheadline = meta_description or f'A clearer, more useful take on {topic_phrase.lower()} with strong, realistic ideas.'
        kicker = 'Save this guide'
        cta = 'Read the full article'

    if style == 'decision' and variant_type == 'trend_overview':
        headline = f'Where to Start with a {room_phrase.title()}' if room_phrase != 'your space' else f'The Best Starting Point for {topic_phrase}'
        subheadline = f'If you want stronger choices without guesswork, start with these grounded selection principles.'
    elif style == 'tutorial' and variant_type == 'trend_overview':
        headline = f'How to Get a {room_phrase.title()} Right' if room_phrase != 'your space' else f'How to Get {topic_phrase} Right'
        subheadline = f'Simple, high-impact guidance that makes {topic_phrase.lower()} easier to apply in real life.'

    return {
        'headline': shorten_pin_headline(headline),
        'subheadline': tighten_pin_subheadline(subheadline),
        'kicker': kicker,
        'cta_label': cta,
        'topic_style': style,
        'topic_phrase': topic_phrase,
        'room_phrase': room_phrase,
    }


def classify_copy_density(headline: str, subheadline: str) -> str:
    headline_length = len(normalize_text(headline))
    subheadline_length = len(normalize_text(subheadline))
    combined = headline_length + subheadline_length
    if headline_length >= 66 or combined >= 170:
        return 'long'
    if headline_length <= 38 and combined <= 110:
        return 'short'
    return 'medium'


def select_template_family(
    *,
    article_metadata: dict[str, Any],
    variant_type: str,
    duplicate_index: int = 0,
    design_system: dict[str, Any] | None = None,
) -> str:
    system = design_system or load_design_system()
    variant_rules = system.get('variant_rules', {})
    templates = system.get('template_families', {})
    rule = variant_rules.get(variant_type, {})
    candidates = [name for name in rule.get('template_preferences', []) if name in templates]
    if not candidates:
        candidates = list(templates.keys())
    copy_variant = build_pin_copy_variant(article_metadata, variant_type=variant_type)
    density = classify_copy_density(copy_variant['headline'], copy_variant['subheadline'])
    density_matches = [
        name for name in candidates
        if str(templates.get(name, {}).get('density_profile', 'medium')) == density
    ]
    ordered = density_matches or candidates
    return ordered[duplicate_index % len(ordered)]
