from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "clusters"
MIN_CLUSTER_ARTICLES = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate static Jekyll cluster pillar pages from the article-cluster index."
    )
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--min-articles", type=int, default=MIN_CLUSTER_ARTICLES)
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9\s-]", " ", str(text or "")).strip().lower()
    slug = re.sub(r"[\s_-]+", "-", cleaned).strip("-")
    return slug or "cluster"


def yaml_escape(text: str) -> str:
    return str(text or "").replace("\\", "\\\\").replace('"', '\\"')


def normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        key = cleaned.lower()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
    return result


def titleize_cluster_name(cluster_name: str) -> str:
    words = [word for word in re.split(r"\s+", cluster_name.strip()) if word]
    return " ".join(word.capitalize() for word in words) or "Cluster"


def build_cluster_intro(
    *,
    cluster_name: str,
    article_count: int,
    primary_keywords: list[str],
    search_intents: list[str],
) -> str:
    title_case_name = titleize_cluster_name(cluster_name)
    keyword_phrase = ", ".join(primary_keywords[:3]) if primary_keywords else cluster_name
    intent_phrase = ", ".join(search_intents[:3]) if search_intents else "ideas and styling guidance"

    paragraphs = [
        (
            f"{title_case_name} is a useful decorating topic because it combines inspiration with practical decision-making. "
            f"Readers usually arrive here looking for a clear style direction, help solving a room problem, or confidence about how to make the look feel intentional at home."
        ),
        (
            f"This pillar page brings together the site's coverage around {cluster_name}, with article angles shaped by keyword themes such as {keyword_phrase}. "
            f"That mix helps the page support both broader search intent and more specific decorating questions without feeling repetitive."
        ),
        (
            f"There are currently {article_count} related article{'s' if article_count != 1 else ''} in this cluster, covering {intent_phrase}. "
            f"Use the guide below to move from overview pieces into more focused styling advice, problem-solving content, and room-specific ideas."
        ),
    ]
    return "\n\n".join(paragraphs)


def build_collection_summary(cluster_name: str, article_count: int) -> str:
    return (
        f"This article collection keeps every {cluster_name} guide in one place so readers can compare ideas, "
        f"follow related angles, and move deeper into the topic as the cluster grows. "
        f"There are currently {article_count} linked article{'s' if article_count != 1 else ''} below."
    )


def article_sort_key(article: dict[str, Any]) -> tuple[str, str]:
    return (
        str(article.get("publish_date") or ""),
        str(article.get("article_slug") or ""),
    )


def build_frontmatter(
    *,
    cluster_name: str,
    cluster_slug: str,
    cluster_intro: str,
    description: str,
    article_count: int,
    primary_keywords: list[str],
    generated_at: str,
) -> str:
    keyword_values = ", ".join(f'"{yaml_escape(keyword)}"' for keyword in primary_keywords[:8])
    return (
        "---\n"
        "layout: cluster\n"
        f'title: "{yaml_escape(titleize_cluster_name(cluster_name))}"\n'
        f'permalink: "/clusters/{cluster_slug}/"\n'
        f'description: "{yaml_escape(description)}"\n'
        f'cluster_name: "{yaml_escape(cluster_name)}"\n'
        f'cluster_slug: "{yaml_escape(cluster_slug)}"\n'
        f"cluster_article_count: {article_count}\n"
        f"cluster_primary_keywords: [{keyword_values}]\n"
        f'cluster_generated_at: "{yaml_escape(generated_at)}"\n'
        f'cluster_intro: "{yaml_escape(cluster_intro)}"\n'
        "generated_cluster_page: true\n"
        "pillar_page: true\n"
        "---\n\n"
    )


def build_article_section(article: dict[str, Any]) -> str:
    title = str(article.get("article_title") or article.get("article_slug") or "").strip()
    permalink = str(article.get("permalink") or "#").strip() or "#"
    publish_date = str(article.get("publish_date") or "").strip()
    primary_keyword = str(article.get("primary_keyword") or "").strip()
    excerpt = str(article.get("excerpt") or "").strip()

    lines = [f"### [{title}]({permalink})"]
    meta_parts = [part for part in [publish_date, primary_keyword] if part]
    if meta_parts:
        lines.append(f"*{' | '.join(meta_parts)}*")
    if excerpt:
        lines.append(excerpt)
    lines.append(f"[Read the full article]({permalink})")
    return "\n".join(lines)


def build_body(cluster_name: str, articles: list[dict[str, Any]]) -> str:
    sections = [
        "## Related Articles",
        "",
        build_collection_summary(cluster_name, len(articles)),
        "",
    ]
    for article in articles:
        sections.append(build_article_section(article))
        sections.append("")
    return "\n".join(sections).rstrip() + "\n"


def build_pillar_pages(
    *,
    cluster_index_path: Path,
    output_dir: Path,
    min_articles: int,
) -> dict[str, Any]:
    index_payload = load_json(cluster_index_path, default={})
    articles = index_payload.get("articles", []) if isinstance(index_payload, dict) else []

    cluster_map: dict[str, list[dict[str, Any]]] = {}
    for article in articles:
        if not isinstance(article, dict):
            continue
        cluster_name = str(article.get("cluster_name") or "").strip()
        if not cluster_name or cluster_name == "uncategorized":
            continue
        cluster_map.setdefault(cluster_name, []).append(article)

    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    eligible_slugs: set[str] = set()
    generated_paths: list[Path] = []

    for cluster_name, cluster_articles in sorted(cluster_map.items()):
        if len(cluster_articles) < max(1, min_articles):
            continue

        cluster_slug = slugify(cluster_name)
        eligible_slugs.add(cluster_slug)
        articles_sorted = sorted(cluster_articles, key=article_sort_key, reverse=True)
        primary_keywords = dedupe_preserving_order(
            [
                str(article.get("primary_keyword") or "").strip()
                for article in articles_sorted
            ]
        )
        secondary_keywords = dedupe_preserving_order(
            [
                keyword
                for article in articles_sorted
                for keyword in normalize_string_list(article.get("secondary_keywords"))
            ]
        )
        search_intents = dedupe_preserving_order(
            [
                str(article.get("search_intent") or "").strip()
                for article in articles_sorted
            ]
        )
        keyword_reference = primary_keywords or secondary_keywords or [cluster_name]
        cluster_intro = build_cluster_intro(
            cluster_name=cluster_name,
            article_count=len(articles_sorted),
            primary_keywords=keyword_reference,
            search_intents=search_intents,
        )
        description = (
            f"Explore {cluster_name} guides, styling ideas, and related articles from The Livin' Edit."
        )
        frontmatter = build_frontmatter(
            cluster_name=cluster_name,
            cluster_slug=cluster_slug,
            cluster_intro=cluster_intro,
            description=description,
            article_count=len(articles_sorted),
            primary_keywords=keyword_reference,
            generated_at=generated_at,
        )
        body = build_body(cluster_name, articles_sorted)
        page_path = output_dir / f"{cluster_slug}.md"
        page_path.write_text(f"{frontmatter}{body}", encoding="utf-8")
        generated_paths.append(page_path)

    removed_paths: list[Path] = []
    for existing_path in output_dir.glob("*.md"):
        if existing_path.stem in eligible_slugs:
            continue
        raw_text = existing_path.read_text(encoding="utf-8-sig")
        if "generated_cluster_page: true" not in raw_text:
            continue
        existing_path.unlink()
        removed_paths.append(existing_path)

    return {
        "generated_count": len(generated_paths),
        "removed_count": len(removed_paths),
        "generated_paths": generated_paths,
        "removed_paths": removed_paths,
        "output_dir": output_dir,
        "min_articles": min_articles,
    }


def main() -> int:
    args = parse_args()
    try:
        result = build_pillar_pages(
            cluster_index_path=Path(args.cluster_index_path),
            output_dir=Path(args.output_dir),
            min_articles=args.min_articles,
        )
        print(result["output_dir"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
