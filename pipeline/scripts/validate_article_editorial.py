from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
DEFAULT_REPORT_PATH = Path(__file__).resolve().parents[1] / "reports" / "article_editorial_validation_report.json"

STOPWORDS = {
    "a",
    "an",
    "and",
    "for",
    "from",
    "how",
    "in",
    "into",
    "of",
    "on",
    "the",
    "to",
    "with",
    "your",
}

GENERIC_INTRO_PATTERNS = [
    r"^when it comes to\b",
    r"^if you(?:'re| are) looking to\b",
    r"^whether you(?:'re| are)\b",
    r"^there (?:is|are) few\b",
    r"^there(?:'s| is) no denying\b",
    r"^creating a\b.*\bspace\b",
    r"^the .* trend\b",
    r"^a well designed\b",
    r"^in today(?:'s)? homes\b",
]

GENERIC_HEADING_PATTERNS = [
    r"^(add|bring|use|mix|layer|choose|style|keep)\s+(texture|color|contrast|decor|details|pieces)$",
    r"^(set the tone|pull it together|finish the look|keep it cohesive)$",
    r"^(make it work|make it feel balanced|create harmony)$",
]

GENERIC_HEADING_TERMS = {
    "balance",
    "color",
    "contrast",
    "decor",
    "details",
    "elements",
    "look",
    "pieces",
    "space",
    "style",
    "styling",
    "texture",
    "touches",
}

SPECIFICITY_TERMS = {
    "armchair",
    "art",
    "beige",
    "blackout",
    "boucle",
    "brass",
    "cabinet",
    "chair",
    "console",
    "cotton",
    "curtain",
    "drape",
    "fabric",
    "floor",
    "hardware",
    "lamp",
    "layout",
    "linen",
    "marble",
    "matte",
    "oak",
    "palette",
    "rod",
    "rug",
    "sconce",
    "sectional",
    "shade",
    "shelf",
    "sheer",
    "sofa",
    "stone",
    "texture",
    "upholstery",
    "velvet",
    "window",
    "wood",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate editorial quality for an article package.")
    parser.add_argument("--package-path", type=str, required=True, help="Path to the article package JSON file.")
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--report-path", type=str, default=str(DEFAULT_REPORT_PATH))
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def normalize_text(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("_", " ").replace("-", " ")
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def tokenize(value: Any) -> set[str]:
    return {token for token in normalize_text(value).split() if token and token not in STOPWORDS}


def jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def fallback_slug(article_package: dict[str, Any]) -> str:
    slug = str(article_package.get("slug") or "").strip()
    if slug:
        return slug
    return normalize_text(article_package.get("title") or "").replace(" ", "-") or "decor-article"


def fallback_cluster(article_package: dict[str, Any]) -> str:
    return str(article_package.get("cluster_id") or article_package.get("topical_cluster") or article_package.get("trend_cluster") or "uncategorized").strip() or "uncategorized"


def fallback_subtopic(article_package: dict[str, Any]) -> str:
    return str(article_package.get("subtopic_id") or "").strip() or "legacy_unspecified"


def fallback_angle(article_package: dict[str, Any]) -> str:
    return str(article_package.get("angle_id") or "").strip() or "legacy_unspecified"


def fallback_primary_keyword(article_package: dict[str, Any]) -> str:
    primary_keyword = str(article_package.get("primary_keyword") or "").strip()
    if primary_keyword:
        return primary_keyword
    keywords = article_package.get("keywords", [])
    if isinstance(keywords, list):
        for keyword in keywords:
            cleaned = str(keyword).strip()
            if cleaned:
                return cleaned
    return str(article_package.get("title") or article_package.get("slug") or "").strip()


def extract_intro(article_markdown: str) -> str:
    before_first_h2 = re.split(r"(?m)^##\s+", article_markdown, maxsplit=1)[0]
    return before_first_h2.strip()


def extract_h2_headings(article_markdown: str) -> list[str]:
    return [match.group(1).strip() for match in re.finditer(r"(?m)^##\s+(.+)$", article_markdown)]


def normalize_heading_signature(heading: str) -> str:
    tokens = [token for token in normalize_text(heading).split() if token and token not in STOPWORDS]
    return " ".join(tokens[:4])


def extract_heading_stems(headings: list[str]) -> list[str]:
    stems: list[str] = []
    for heading in headings:
        words = normalize_text(heading).split()
        stems.append(words[0] if words else "")
    return [stem for stem in stems if stem]


def build_concept_tokens(article_package: dict[str, Any]) -> set[str]:
    parts = [
        article_package.get("title", ""),
        fallback_primary_keyword(article_package),
        article_package.get("cluster_id", ""),
        article_package.get("subtopic_id", ""),
        article_package.get("subtopic_name", ""),
    ]
    secondary_keywords = article_package.get("secondary_keywords", [])
    if isinstance(secondary_keywords, list):
        parts.extend(secondary_keywords[:3])
    return tokenize(" ".join(str(part) for part in parts))


def evaluate_generic_intro(article_package: dict[str, Any]) -> tuple[list[str], list[str], dict[str, Any]]:
    warnings: list[str] = []
    errors: list[str] = []
    article_markdown = str(article_package.get("article_markdown") or "")
    intro = extract_intro(article_markdown)
    intro_tokens = tokenize(intro)
    concept_tokens = build_concept_tokens(article_package)
    intro_word_count = len(intro.split())
    concept_overlap = len(intro_tokens & concept_tokens)
    first_sentence = re.split(r"(?<=[.!?])\s+", intro, maxsplit=1)[0].strip()
    matched_patterns = [
        pattern for pattern in GENERIC_INTRO_PATTERNS
        if re.search(pattern, normalize_text(first_sentence))
    ]

    if intro_word_count < 70:
        warnings.append("Introduction is short and may not establish a strong editorial premise.")

    if concept_overlap <= 1:
        warnings.append("Introduction barely anchors the actual article concept.")

    if matched_patterns:
        warnings.append("Introduction opens with a broad interchangeable framing pattern.")

    if intro_word_count < 55 and concept_overlap == 0:
        errors.append("Introduction is too generic to carry the article concept.")

    if matched_patterns and concept_overlap == 0:
        errors.append("Introduction reads like a reusable decor opener instead of a specific article lead.")

    return warnings, errors, {
        "intro_word_count": intro_word_count,
        "concept_overlap": concept_overlap,
        "first_sentence": first_sentence,
    }


def evaluate_heading_quality(article_package: dict[str, Any]) -> tuple[list[str], list[str], dict[str, Any]]:
    warnings: list[str] = []
    errors: list[str] = []
    headings = [
        heading for heading in extract_h2_headings(str(article_package.get("article_markdown") or ""))
        if normalize_text(heading) not in {"faq", "frequently asked questions"}
    ]
    concept_tokens = build_concept_tokens(article_package)
    vague_count = 0
    low_specificity_count = 0
    normalized_signatures = [normalize_heading_signature(heading) for heading in headings]
    repeated_signature_count = len(normalized_signatures) - len(set(signature for signature in normalized_signatures if signature))
    repeated_stem_count = 0
    heading_stems = extract_heading_stems(headings)
    for stem in set(heading_stems):
        if heading_stems.count(stem) >= 3:
            repeated_stem_count += 1

    for heading in headings:
        normalized_heading = normalize_text(heading)
        heading_tokens = tokenize(heading)
        overlap = len(heading_tokens & concept_tokens)
        generic_terms = sum(1 for token in heading_tokens if token in GENERIC_HEADING_TERMS)
        if any(re.search(pattern, normalized_heading) for pattern in GENERIC_HEADING_PATTERNS):
            vague_count += 1
        elif len(heading_tokens) <= 3 and generic_terms >= 1:
            vague_count += 1

        if overlap == 0 or (overlap == 1 and generic_terms >= max(1, len(heading_tokens) - 1)):
            low_specificity_count += 1

    if vague_count >= 1:
        warnings.append("Some section headings are vague and could fit many decor articles.")
    if low_specificity_count >= 2:
        warnings.append("Several section headings do not signal the topic strongly enough.")
    if repeated_signature_count >= 1 or repeated_stem_count >= 1:
        warnings.append("Section headings rely on repetitive formulas instead of distinct editorial moves.")

    if vague_count >= 3:
        errors.append("Too many section headings are bland or overly generic.")
    if low_specificity_count >= 4:
        errors.append("Section headings are too unspecific to support a strong editorial frame.")

    return warnings, errors, {
        "heading_count": len(headings),
        "vague_heading_count": vague_count,
        "low_specificity_heading_count": low_specificity_count,
        "repeated_signature_count": repeated_signature_count,
        "repeated_stem_count": repeated_stem_count,
        "headings": headings,
    }


def evaluate_specificity(article_package: dict[str, Any]) -> tuple[list[str], list[str], dict[str, Any]]:
    warnings: list[str] = []
    errors: list[str] = []
    article_markdown = str(article_package.get("article_markdown") or "")
    body_tokens = tokenize(article_markdown)
    concept_tokens = build_concept_tokens(article_package)
    concept_hits = len(body_tokens & concept_tokens)
    specificity_hits = len(body_tokens & SPECIFICITY_TERMS)
    secondary_keywords = article_package.get("secondary_keywords", [])
    normalized_body = normalize_text(article_markdown)
    secondary_hits = 0
    if isinstance(secondary_keywords, list):
        secondary_hits = sum(
            1 for keyword in secondary_keywords
            if keyword and normalize_text(keyword) and normalize_text(keyword) in normalized_body
        )

    if concept_hits < 4:
        warnings.append("Article body is light on topic-specific framing.")
    if specificity_hits < 6:
        warnings.append("Article could use more concrete decor detail such as materials, furnishings, or room-specific guidance.")
    if secondary_keywords and secondary_hits == 0:
        warnings.append("Supporting keyword ideas are barely reflected in the body.")

    if concept_hits < 2 and specificity_hits < 4:
        errors.append("Article feels too broad and under-specific for the chosen concept.")

    return warnings, errors, {
        "concept_token_hits": concept_hits,
        "specificity_term_hits": specificity_hits,
        "secondary_keyword_hits": secondary_hits,
    }


def evaluate_distinctiveness(
    article_package: dict[str, Any],
    existing_index_data: dict[str, Any],
) -> tuple[list[str], list[str], dict[str, Any]]:
    warnings: list[str] = []
    errors: list[str] = []
    cluster_id = fallback_cluster(article_package)
    subtopic_id = fallback_subtopic(article_package)
    angle_id = fallback_angle(article_package)
    title_tokens = tokenize(article_package.get("title") or "")
    primary_tokens = tokenize(fallback_primary_keyword(article_package))
    intro_tokens = tokenize(extract_intro(str(article_package.get("article_markdown") or "")))
    heading_signatures = {
        normalize_heading_signature(heading)
        for heading in extract_h2_headings(str(article_package.get("article_markdown") or ""))
        if normalize_heading_signature(heading)
    }

    max_similarity = 0.0
    nearest_slug = ""
    strongest_heading_overlap = 0

    for article in existing_index_data.get("articles", []):
        if not isinstance(article, dict):
            continue
        if str(article.get("cluster_id") or "") != cluster_id:
            continue

        existing_subtopic_id = str(article.get("subtopic_id") or "") or "legacy_unspecified"
        existing_angle_id = str(article.get("angle_id") or "") or "legacy_unspecified"
        same_subtopic = existing_subtopic_id == subtopic_id and subtopic_id != "legacy_unspecified"
        same_angle = existing_angle_id == angle_id and angle_id != "legacy_unspecified"

        title_similarity = jaccard_similarity(title_tokens, tokenize(article.get("article_title") or ""))
        primary_similarity = jaccard_similarity(primary_tokens, tokenize(article.get("primary_keyword") or ""))
        excerpt_similarity = jaccard_similarity(intro_tokens, tokenize(article.get("excerpt") or ""))
        similarity = max(title_similarity, primary_similarity, excerpt_similarity)

        if similarity > max_similarity:
            max_similarity = similarity
            nearest_slug = str(article.get("article_slug") or "")

        existing_signatures = {
            normalize_heading_signature(str(value))
            for value in [article.get("article_title"), article.get("primary_keyword"), article.get("excerpt")]
            if normalize_heading_signature(str(value))
        }
        heading_overlap = len(heading_signatures & existing_signatures)
        strongest_heading_overlap = max(strongest_heading_overlap, heading_overlap)

        if same_subtopic and same_angle and similarity >= 0.75:
            errors.append(
                f"Article framing is too close to existing article '{nearest_slug or article.get('article_slug', '')}' in the same subtopic and angle."
            )
        elif same_subtopic and similarity >= 0.62:
            warnings.append(
                f"Article framing is quite close to existing cluster coverage in '{article.get('article_slug', '')}'."
            )
        elif same_angle and title_similarity >= 0.58 and primary_similarity >= 0.5:
            warnings.append(
                f"Article angle/title combination overlaps heavily with '{article.get('article_slug', '')}'."
            )

    if strongest_heading_overlap >= 3:
        warnings.append("The article's section framing is very close to nearby cluster coverage.")

    return sorted(dict.fromkeys(warnings)), sorted(dict.fromkeys(errors)), {
        "nearest_article_slug": nearest_slug,
        "max_similarity": round(max_similarity, 3),
        "strongest_heading_overlap": strongest_heading_overlap,
    }


def validate_article_editorial(
    article_package: dict[str, Any],
    existing_index_data: dict[str, Any],
) -> dict[str, Any]:
    warnings: list[str] = []
    errors: list[str] = []

    intro_warnings, intro_errors, intro_checks = evaluate_generic_intro(article_package)
    heading_warnings, heading_errors, heading_checks = evaluate_heading_quality(article_package)
    specificity_warnings, specificity_errors, specificity_checks = evaluate_specificity(article_package)
    distinctiveness_warnings, distinctiveness_errors, distinctiveness_checks = evaluate_distinctiveness(
        article_package,
        existing_index_data,
    )

    warnings.extend(intro_warnings)
    warnings.extend(heading_warnings)
    warnings.extend(specificity_warnings)
    warnings.extend(distinctiveness_warnings)

    errors.extend(intro_errors)
    errors.extend(heading_errors)
    errors.extend(specificity_errors)
    errors.extend(distinctiveness_errors)

    deduped_warnings = sorted(dict.fromkeys(warnings))
    deduped_errors = sorted(dict.fromkeys(errors))
    status = "fail" if deduped_errors else ("warning" if deduped_warnings else "pass")

    return {
        "article_slug": fallback_slug(article_package),
        "cluster_id": fallback_cluster(article_package),
        "subtopic_id": fallback_subtopic(article_package),
        "angle_id": fallback_angle(article_package),
        "primary_keyword": fallback_primary_keyword(article_package),
        "validation_status": status,
        "warnings": deduped_warnings,
        "errors": deduped_errors,
        "checks": {
            "generic_intro": intro_checks,
            "heading_quality": heading_checks,
            "specificity": specificity_checks,
            "distinctiveness": distinctiveness_checks,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def write_validation_report(report_path: Path, result: dict[str, Any]) -> Path:
    current = load_json(report_path, {"generated_at": "", "articles": []})
    articles = current.get("articles", []) if isinstance(current, dict) else []
    if not isinstance(articles, list):
        articles = []

    updated_articles = [
        article for article in articles
        if isinstance(article, dict) and article.get("article_slug") != result.get("article_slug")
    ]
    updated_articles.append(result)
    updated_articles.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "articles": updated_articles,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path


def main() -> int:
    args = parse_args()
    package = load_json(Path(args.package_path), {})
    if isinstance(package, dict) and isinstance(package.get("package"), dict):
        package = package["package"]
    result = validate_article_editorial(
        article_package=package,
        existing_index_data=load_json(Path(args.cluster_index_path), {"articles": []}),
    )
    write_validation_report(Path(args.report_path), result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["validation_status"] != "fail" else 1


if __name__ == "__main__":
    raise SystemExit(main())
