from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
DEFAULT_CLUSTER_REPORT_PATH = Path(__file__).resolve().parents[1] / "data" / "keyword_cluster_report.json"
DEFAULT_TREND_HISTORY_PATH = Path(__file__).resolve().parents[1] / "data" / "trend_history.json"
DEFAULT_REPORT_PATH = Path(__file__).resolve().parents[1] / "reports" / "article_seo_validation_report.json"
GENERIC_TITLE_TERMS = {
    "decor",
    "ideas",
    "interiors",
    "rooms",
    "style",
    "styling",
    "guide",
    "trend",
    "trends",
    "tips",
    "home",
    "modern",
    "warm",
    "beautiful",
    "love",
}
STOPWORDS = {
    "a",
    "an",
    "and",
    "for",
    "how",
    "in",
    "of",
    "on",
    "the",
    "to",
    "with",
    "your",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate SEO duplication and content-quality risks for an article package.")
    parser.add_argument("--package-path", type=str, required=True, help="Path to the article package JSON file.")
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--cluster-report-path", type=str, default=str(DEFAULT_CLUSTER_REPORT_PATH))
    parser.add_argument("--trend-history-path", type=str, default=str(DEFAULT_TREND_HISTORY_PATH))
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


def count_phrase_occurrences(text: str, phrase: str) -> int:
    normalized_text = normalize_text(text)
    normalized_phrase = normalize_text(phrase)
    if not normalized_phrase:
        return 0
    return normalized_text.count(normalized_phrase)


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
    if isinstance(keywords, str) and keywords.strip():
        return keywords.strip().split(",")[0].strip()

    return str(article_package.get("title") or article_package.get("slug") or "").strip()


def fallback_cluster(article_package: dict[str, Any]) -> str:
    return str(article_package.get("topical_cluster") or article_package.get("trend_cluster") or "uncategorized").strip() or "uncategorized"


def fallback_slug(article_package: dict[str, Any]) -> str:
    slug = str(article_package.get("slug") or "").strip()
    if slug:
        return slug
    return normalize_text(article_package.get("title") or "").replace(" ", "-") or "decor-article"


def extract_headings(article_markdown: str) -> list[str]:
    return [match.group(1).strip() for match in re.finditer(r"(?m)^##\s+(.+)$", article_markdown)]


def detect_angle(*values: str) -> str:
    text = normalize_text(" ".join(values))
    if text.startswith("how to") or " how to " in f" {text} ":
        return "how_to"
    if any(token in text for token in {"mistake", "mistakes", "avoid", "fix", "fixes"}):
        return "mistakes"
    if any(token in text for token in {"ideas", "inspiration"}):
        return "ideas"
    if any(token in text for token in {"guide", "mastering", "complete"}):
        return "guide"
    if "trend" in text:
        return "trend"
    return "styling_advice"


def build_existing_angle_maps(index_data: dict[str, Any], trend_history: dict[str, Any]) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, int]]]:
    article_counts: dict[str, dict[str, int]] = {}
    for article in index_data.get("articles", []):
        if not isinstance(article, dict):
            continue
        cluster = str(article.get("cluster_name") or "uncategorized").strip()
        angle = detect_angle(
            str(article.get("article_title") or ""),
            str(article.get("primary_keyword") or ""),
            str(article.get("excerpt") or ""),
        )
        article_counts.setdefault(cluster, {})[angle] = article_counts.setdefault(cluster, {}).get(angle, 0) + 1

    history_counts: dict[str, dict[str, int]] = {}
    for entry in trend_history.get("entries", []):
        if not isinstance(entry, dict):
            continue
        cluster = str(entry.get("trend_cluster") or "uncategorized").strip()
        angle = detect_angle(str(entry.get("trend_keyword") or ""))
        history_counts.setdefault(cluster, {})[angle] = history_counts.setdefault(cluster, {}).get(angle, 0) + 1

    return article_counts, history_counts


def evaluate_cannibalization(
    article_package: dict[str, Any],
    index_data: dict[str, Any],
) -> tuple[list[str], list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []
    duplicates: list[str] = []

    new_slug = fallback_slug(article_package)
    new_title = str(article_package.get("title") or "")
    new_primary = fallback_primary_keyword(article_package)
    new_cluster = fallback_cluster(article_package)
    new_angle = detect_angle(new_title, new_primary)
    new_slug_tokens = tokenize(new_slug)
    new_title_tokens = tokenize(new_title)
    new_primary_tokens = tokenize(new_primary)

    for article in index_data.get("articles", []):
        if not isinstance(article, dict):
            continue
        existing_slug = str(article.get("article_slug") or "").strip()
        existing_title = str(article.get("article_title") or "")
        existing_primary = str(article.get("primary_keyword") or "")
        existing_cluster = str(article.get("cluster_name") or "uncategorized")
        existing_angle = detect_angle(existing_title, existing_primary, str(article.get("excerpt") or ""))

        slug_similarity = jaccard_similarity(new_slug_tokens, tokenize(existing_slug))
        title_similarity = jaccard_similarity(new_title_tokens, tokenize(existing_title))
        primary_similarity = jaccard_similarity(new_primary_tokens, tokenize(existing_primary))

        if new_slug and existing_slug and normalize_text(new_slug) == normalize_text(existing_slug):
            errors.append(f"Slug duplicates existing article '{existing_slug}'.")
            duplicates.append(existing_slug)
            continue

        if slug_similarity >= 0.9:
            errors.append(f"Slug is extremely close to existing article '{existing_slug}'.")
            duplicates.append(existing_slug)
            continue

        if new_primary and existing_primary and normalize_text(new_primary) == normalize_text(existing_primary) and new_angle == existing_angle:
            errors.append(f"Primary keyword and intent duplicate existing article '{existing_slug}'.")
            duplicates.append(existing_slug)
            continue

        if new_cluster == existing_cluster and title_similarity >= 0.82 and primary_similarity >= 0.7:
            errors.append(f"Title and keyword intent strongly overlap with existing cluster article '{existing_slug}'.")
            duplicates.append(existing_slug)
            continue

        if primary_similarity >= 0.72 or (new_cluster == existing_cluster and title_similarity >= 0.68):
            warnings.append(f"Possible cannibalization risk against '{existing_slug}'.")
            duplicates.append(existing_slug)

    return warnings, errors, sorted(dict.fromkeys(duplicates))


def evaluate_cluster_duplication(article_package: dict[str, Any], index_data: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    new_cluster = fallback_cluster(article_package)
    if not new_cluster or new_cluster == "uncategorized":
        return warnings

    new_title_tokens = tokenize(article_package.get("title") or "")
    new_heading_tokens = tokenize(" ".join(extract_headings(str(article_package.get("article_markdown") or ""))))

    for article in index_data.get("articles", []):
        if not isinstance(article, dict) or str(article.get("cluster_name") or "") != new_cluster:
            continue

        title_similarity = jaccard_similarity(new_title_tokens, tokenize(article.get("article_title") or ""))
        excerpt_similarity = jaccard_similarity(new_heading_tokens, tokenize(article.get("excerpt") or ""))
        if title_similarity >= 0.55 or excerpt_similarity >= 0.45:
            warnings.append(
                f"Cluster overlap is high with '{article.get('article_slug', '')}', so the angle may add limited new value."
            )

    return sorted(dict.fromkeys(warnings))


def evaluate_title_quality(article_package: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    title = str(article_package.get("title") or "").strip()
    primary_keyword = str(article_package.get("primary_keyword") or "").strip()
    title_tokens = tokenize(title)
    primary_tokens = tokenize(primary_keyword)

    if len(title) < 35:
        warnings.append("Title is short and may lack search specificity.")
    if len(title_tokens) < 4:
        warnings.append("Title is very brief and may be too vague for search intent.")
    if primary_tokens and jaccard_similarity(title_tokens, primary_tokens) < 0.35:
        warnings.append("Title does not reflect the primary keyword strongly enough.")

    specific_terms = [token for token in title_tokens if token not in GENERIC_TITLE_TERMS]
    if len(specific_terms) < 2:
        warnings.append("Title pattern looks generic compared to the intended keyword cluster.")

    weak_patterns = [
        r"decor ideas youll love",
        r"warm interiors",
        r"beautiful spaces",
    ]
    normalized_title = normalize_text(title)
    if any(re.search(pattern, normalized_title) for pattern in weak_patterns):
        warnings.append("Title matches a weak generic SEO pattern.")

    return sorted(dict.fromkeys(warnings))


def evaluate_semantic_coverage(article_package: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    article_markdown = str(article_package.get("article_markdown") or "")
    headings = extract_headings(article_markdown)
    primary_keyword = str(article_package.get("primary_keyword") or "").strip()
    secondary_keywords = [str(item).strip() for item in article_package.get("secondary_keywords", []) if str(item).strip()]
    cluster_keywords = [str(item).strip() for item in article_package.get("cluster_keywords", []) if str(item).strip()]

    if count_phrase_occurrences(article_markdown, primary_keyword) < 2:
        warnings.append("Primary keyword appears very lightly in the article body.")

    secondary_present = sum(1 for keyword in secondary_keywords if count_phrase_occurrences(article_markdown, keyword) >= 1)
    if secondary_keywords and secondary_present == 0:
        warnings.append("None of the secondary keywords appear in the article body.")

    heading_text = " ".join(headings)
    cluster_token_hits = len(tokenize(heading_text) & tokenize(" ".join(cluster_keywords)))
    if cluster_keywords and cluster_token_hits < 2:
        warnings.append("Headings do not reflect the topic cluster strongly enough.")

    if headings and all(jaccard_similarity(tokenize(heading), tokenize(primary_keyword)) < 0.2 for heading in headings[:5]):
        warnings.append("Main section headings may be too generic for the chosen keyword.")

    return sorted(dict.fromkeys(warnings))


def evaluate_repetitive_angles(
    article_package: dict[str, Any],
    index_data: dict[str, Any],
    trend_history: dict[str, Any],
) -> list[str]:
    warnings: list[str] = []
    cluster = fallback_cluster(article_package)
    angle = detect_angle(
        str(article_package.get("title") or ""),
        fallback_primary_keyword(article_package),
        str(article_package.get("article_markdown") or ""),
    )
    article_counts, history_counts = build_existing_angle_maps(index_data, trend_history)
    total_existing = article_counts.get(cluster, {}).get(angle, 0) + history_counts.get(cluster, {}).get(angle, 0)

    if total_existing >= 2:
        warnings.append(
            f"The '{angle}' angle is already used frequently in the '{cluster}' cluster."
        )

    return warnings


def validate_article_seo(
    article_package: dict[str, Any],
    existing_index_data: dict[str, Any],
    cluster_report_data: dict[str, Any] | None = None,
    trend_history_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del cluster_report_data
    trend_history_data = trend_history_data or {"entries": []}

    warnings: list[str] = []
    errors: list[str] = []

    cannibalization_warnings, cannibalization_errors, duplicates = evaluate_cannibalization(article_package, existing_index_data)
    warnings.extend(cannibalization_warnings)
    errors.extend(cannibalization_errors)
    warnings.extend(evaluate_cluster_duplication(article_package, existing_index_data))
    warnings.extend(evaluate_title_quality(article_package))
    warnings.extend(evaluate_semantic_coverage(article_package))
    warnings.extend(evaluate_repetitive_angles(article_package, existing_index_data, trend_history_data))

    deduped_warnings = sorted(dict.fromkeys(warnings))
    deduped_errors = sorted(dict.fromkeys(errors))
    status = "fail" if deduped_errors else ("warning" if deduped_warnings else "pass")

    return {
        "article_slug": fallback_slug(article_package),
        "cluster": fallback_cluster(article_package),
        "primary_keyword": fallback_primary_keyword(article_package),
        "validation_status": status,
        "warnings": deduped_warnings,
        "errors": deduped_errors,
        "duplicate_candidate_slugs": duplicates,
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
    result = validate_article_seo(
        article_package=package,
        existing_index_data=load_json(Path(args.cluster_index_path), {"articles": []}),
        cluster_report_data=load_json(Path(args.cluster_report_path), {"clusters": []}),
        trend_history_data=load_json(Path(args.trend_history_path), {"entries": []}),
    )
    write_validation_report(Path(args.report_path), result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["validation_status"] != "fail" else 1


if __name__ == "__main__":
    raise SystemExit(main())
