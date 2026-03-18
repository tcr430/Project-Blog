from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from content_architecture import load_content_clusters
from topic_clusters import normalize_text

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


def unique_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for article in articles:
        slug = str(article.get("article_slug") or "").strip()
        if slug and slug in seen:
            continue
        if slug:
            seen.add(slug)
        result.append(article)
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


def score_featured_article(article: dict[str, Any], cluster_name: str) -> tuple[int, str, str]:
    angle_id = normalize_identifier(article.get("angle_id"))
    title = str(article.get("article_title") or "").strip()
    primary_keyword = str(article.get("primary_keyword") or "").strip()
    score = 0

    angle_boosts = {
        "ideas": 4,
        "how_to": 3,
        "best_options": 2,
        "mistakes": 1,
    }
    score += angle_boosts.get(angle_id, 0)

    cluster_tokens = set(normalize_text(cluster_name).split())
    keyword_tokens = set(normalize_text(primary_keyword).split())
    title_tokens = set(normalize_text(title).split())
    score += len(cluster_tokens & keyword_tokens) * 2
    score += len(cluster_tokens & title_tokens)

    if angle_id == "ideas" and "guide" in normalize_text(title):
        score += 1

    return score, str(article.get("publish_date") or ""), title.lower()


def choose_featured_article(articles: list[dict[str, Any]], cluster_name: str) -> dict[str, Any] | None:
    if not articles:
        return None
    return max(articles, key=lambda article: score_featured_article(article, cluster_name))


def build_featured_article_block(article: dict[str, Any], cluster_name: str) -> str:
    title = str(article.get("article_title") or article.get("article_slug") or "").strip()
    permalink = str(article.get("permalink") or "#").strip() or "#"
    excerpt = str(article.get("excerpt") or "").strip()
    angle_id = normalize_identifier(article.get("angle_id")) or "guide"
    subtopic_name = str(article.get("subtopic_name") or "").strip()
    guidance = (
        f"If you are new to {cluster_name}, this is the clearest entry point because it gives the broadest read on the topic "
        f"before readers branch into narrower styling questions."
    )
    if angle_id == "how_to":
        guidance = (
            f"This is the best entry point for readers who want a practical path into {cluster_name} rather than pure inspiration."
        )
    elif angle_id == "best_options":
        guidance = (
            f"This is a strong starting point for readers comparing options and trying to make confident product or styling decisions."
        )

    meta_parts = [part for part in [subtopic_name, angle_id.replace("_", " ")] if part]
    meta_line = " | ".join(meta_parts)
    lines = [
        "## Start Here",
        "",
        '<div class="cluster-hub-card cluster-featured-card">',
        '<p class="cluster-hub-eyebrow">Featured Entry Point</p>',
        f'<h3><a href="{permalink}">{title}</a></h3>',
    ]
    if meta_line:
        lines.append(f'<p class="cluster-hub-meta">{meta_line}</p>')
    if excerpt:
        lines.append(f"<p>{excerpt}</p>")
    lines.append(f"<p>{guidance}</p>")
    lines.append(f'<p><a class="cluster-hub-link" href="{permalink}">Start with this article</a></p>')
    lines.append("</div>")
    return "\n".join(lines)


def build_subtopic_summary(
    *,
    cluster_name: str,
    subtopic_name: str,
    description: str,
    article_count: int,
) -> str:
    if description:
        return (
            f"{description} In this cluster, the {subtopic_name.lower()} section currently gathers "
            f"{article_count} related article{'s' if article_count != 1 else ''} so readers can move from inspiration "
            f"to more practical decisions without losing the thread of the topic."
        )
    return (
        f"This section focuses on {subtopic_name.lower()} within {cluster_name}. "
        f"It currently includes {article_count} related article{'s' if article_count != 1 else ''} that help readers "
        f"go deeper into this part of the cluster."
    )


def build_subtopic_guidance_note(subtopic_name: str, article_count: int) -> str:
    if article_count <= 1:
        return (
            f"Use this section to get oriented in {subtopic_name.lower()} first, then follow the reading path below to expand into adjacent decisions."
        )
    return (
        f"Readers usually do best here by starting with the broadest article, then moving into the more specific follow-up pieces in the same section."
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


def choose_related_clusters(
    *,
    cluster_name: str,
    cluster_config: dict[str, Any] | None,
    clusters_by_id: dict[str, dict[str, Any]],
    articles_by_cluster_name: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    current_cluster_id = str((cluster_config or {}).get("cluster_id") or "").strip()
    current_domain_id = str((cluster_config or {}).get("domain_id") or "").strip()
    current_tokens = set(normalize_text(cluster_name).split())

    explicit_related_ids = [
        str(item).strip()
        for item in (cluster_config or {}).get("related_clusters", [])
        if str(item).strip()
    ]
    for related_id in explicit_related_ids:
        related_cluster = clusters_by_id.get(related_id)
        if not related_cluster:
            continue
        related_name = str(related_cluster.get("cluster_name") or "").strip()
        related_articles = articles_by_cluster_name.get(related_name, [])
        if not related_name:
            continue
        candidates.append(
            {
                "cluster_id": related_id,
                "cluster_name": related_name,
                "description": str(related_cluster.get("description") or "").strip(),
                "article_count": len(related_articles),
                "score": 100 + len(related_articles),
            }
        )

    for related_id, related_cluster in clusters_by_id.items():
        if related_id == current_cluster_id:
            continue
        related_name = str(related_cluster.get("cluster_name") or "").strip()
        if not related_name:
            continue
        related_tokens = set(normalize_text(related_name).split())
        overlap = len(current_tokens & related_tokens)
        same_domain = str(related_cluster.get("domain_id") or "").strip() == current_domain_id and current_domain_id
        if not same_domain and overlap == 0:
            continue
        related_articles = articles_by_cluster_name.get(related_name, [])
        score = overlap * 10 + (4 if same_domain else 0) + min(len(related_articles), 3)
        candidates.append(
            {
                "cluster_id": related_id,
                "cluster_name": related_name,
                "description": str(related_cluster.get("description") or "").strip(),
                "article_count": len(related_articles),
                "score": score,
            }
        )

    deduped: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in sorted(candidates, key=lambda value: (-value["score"], value["cluster_name"])):
        cluster_id = str(item.get("cluster_id") or "").strip()
        if cluster_id in seen_ids:
            continue
        seen_ids.add(cluster_id)
        deduped.append(item)
        if len(deduped) >= 3:
            break
    return deduped


def build_related_clusters_block(related_clusters: list[dict[str, Any]]) -> str:
    if not related_clusters:
        return ""
    lines = [
        "## Related Clusters",
        "",
        "These adjacent hubs help readers keep moving through the topic without losing the larger decorating context.",
        "",
        '<div class="cluster-hub-grid">',
    ]
    for cluster in related_clusters:
        cluster_name = str(cluster.get("cluster_name") or "").strip()
        description = str(cluster.get("description") or "").strip()
        article_count = int(cluster.get("article_count") or 0)
        permalink = f"/clusters/{slugify(cluster_name)}/"
        lines.extend(
            [
                '<div class="cluster-hub-card">',
                f'<h3><a href="{permalink}">{titleize_cluster_name(cluster_name)}</a></h3>',
                f'<p>{description or f"Explore related coverage around {cluster_name}."}</p>',
                f'<p class="cluster-hub-meta">{article_count} article{"s" if article_count != 1 else ""}</p>',
                f'<p><a class="cluster-hub-link" href="{permalink}">Open this cluster</a></p>',
                "</div>",
            ]
        )
    lines.extend(["</div>"])
    return "\n".join(lines)


def build_next_reading_path(
    *,
    featured_article: dict[str, Any] | None,
    articles: list[dict[str, Any]],
    related_clusters: list[dict[str, Any]],
) -> str:
    steps: list[str] = []
    used_slugs: set[str] = set()
    if featured_article:
        title = str(featured_article.get("article_title") or "").strip()
        permalink = str(featured_article.get("permalink") or "#").strip() or "#"
        used_slugs.add(str(featured_article.get("article_slug") or "").strip())
        steps.append(f"Start with [{title}]({permalink}) for the broadest orientation.")

    for article in articles:
        slug = str(article.get("article_slug") or "").strip()
        if slug in used_slugs:
            continue
        title = str(article.get("article_title") or "").strip()
        permalink = str(article.get("permalink") or "#").strip() or "#"
        subtopic_name = str(article.get("subtopic_name") or "").strip()
        steps.append(
            f"Then read [{title}]({permalink}) to go deeper into {subtopic_name.lower() if subtopic_name else 'a more specific angle'}."
        )
        used_slugs.add(slug)
        if len(steps) >= 2:
            break

    for cluster in related_clusters:
        cluster_name = str(cluster.get("cluster_name") or "").strip()
        permalink = f"/clusters/{slugify(cluster_name)}/"
        steps.append(
            f"After that, move into [{titleize_cluster_name(cluster_name)}]({permalink}) to keep the topic expanding naturally."
        )
        if len(steps) >= 3:
            break

    if not steps:
        return ""

    lines = [
        "## Read Next",
        "",
        "A simple path through this topic so readers can build confidence one step at a time:",
        "",
    ]
    for index, step in enumerate(steps, start=1):
        lines.append(f"{index}. {step}")
    return "\n".join(lines)


def normalize_identifier(value: Any) -> str:
    cleaned = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return re.sub(r"_+", "_", cleaned).strip("_")


def build_cluster_lookup() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    clusters = load_content_clusters()
    by_id: dict[str, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for cluster in clusters:
        cluster_id = str(cluster.get("cluster_id") or "").strip()
        cluster_name = normalize_text(cluster.get("cluster_name") or "")
        if cluster_id:
            by_id[cluster_id] = cluster
        if cluster_name:
            by_name[cluster_name] = cluster
    return by_id, by_name


def resolve_cluster_config(
    cluster_name: str,
    articles: list[dict[str, Any]],
    clusters_by_id: dict[str, dict[str, Any]],
    clusters_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for article in articles:
        cluster_id = normalize_identifier(article.get("cluster_id"))
        if cluster_id and cluster_id in clusters_by_id:
            return clusters_by_id[cluster_id]
    return clusters_by_name.get(normalize_text(cluster_name))


def group_articles_by_subtopic(
    *,
    cluster_config: dict[str, Any] | None,
    articles: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not cluster_config:
        return [], articles

    subtopics = cluster_config.get("subtopics", [])
    grouped_sections: list[dict[str, Any]] = []
    used_slugs: set[str] = set()

    for subtopic in subtopics:
        subtopic_id = str(subtopic.get("subtopic_id") or "").strip()
        if not subtopic_id:
            continue
        matching_articles = [
            article
            for article in articles
            if normalize_identifier(article.get("subtopic_id")) == subtopic_id
        ]
        if not matching_articles:
            continue
        matching_articles = sorted(matching_articles, key=article_sort_key, reverse=True)
        for article in matching_articles:
            used_slugs.add(str(article.get("article_slug") or ""))
        grouped_sections.append(
            {
                "subtopic_id": subtopic_id,
                "subtopic_name": str(subtopic.get("subtopic_name") or "").strip() or "Subtopic",
                "description": str(subtopic.get("description") or "").strip(),
                "articles": matching_articles,
            }
        )

    ungrouped_articles = [
        article
        for article in articles
        if str(article.get("article_slug") or "") not in used_slugs
    ]
    return grouped_sections, ungrouped_articles


def build_body(
    cluster_name: str,
    articles: list[dict[str, Any]],
    cluster_config: dict[str, Any] | None,
    related_clusters: list[dict[str, Any]],
) -> str:
    featured_article = choose_featured_article(articles, cluster_name)
    sections = [
        build_featured_article_block(featured_article, cluster_name) if featured_article else "",
        "",
        "## Cluster Guide",
        "",
        build_collection_summary(cluster_name, len(articles)),
        "",
    ]

    grouped_sections, ungrouped_articles = group_articles_by_subtopic(
        cluster_config=cluster_config,
        articles=articles,
    )

    if grouped_sections:
        for section in grouped_sections:
            sections.append(f"## {section['subtopic_name']}")
            sections.append("")
            sections.append(
                build_subtopic_summary(
                    cluster_name=cluster_name,
                    subtopic_name=str(section["subtopic_name"]),
                    description=str(section["description"]),
                    article_count=len(section["articles"]),
                )
            )
            sections.append("")
            sections.append(build_subtopic_guidance_note(str(section["subtopic_name"]), len(section["articles"])))
            sections.append("")
            for article in section["articles"]:
                sections.append(build_article_section(article))
                sections.append("")

    if ungrouped_articles:
        heading = "## More in This Cluster" if grouped_sections else "## Related Articles"
        sections.append(heading)
        sections.append("")
        if grouped_sections:
            sections.append(
                "These articles still support the cluster even though they are not yet mapped to a clear subtopic section. "
                "They remain useful entry points while the coverage grows."
            )
            sections.append("")
        for article in ungrouped_articles:
            sections.append(build_article_section(article))
            sections.append("")

    related_block = build_related_clusters_block(related_clusters)
    if related_block:
        sections.append(related_block)
        sections.append("")

    reading_path = build_next_reading_path(
        featured_article=featured_article,
        articles=articles,
        related_clusters=related_clusters,
    )
    if reading_path:
        sections.append(reading_path)
        sections.append("")

    return "\n".join(item for item in sections if item is not None).rstrip() + "\n"


def build_pillar_pages(
    *,
    cluster_index_path: Path,
    output_dir: Path,
    min_articles: int,
) -> dict[str, Any]:
    index_payload = load_json(cluster_index_path, default={})
    articles = index_payload.get("articles", []) if isinstance(index_payload, dict) else []
    clusters_by_id, clusters_by_name = build_cluster_lookup()

    cluster_map: dict[str, list[dict[str, Any]]] = {}
    for article in articles:
        if not isinstance(article, dict):
            continue
        cluster_name = str(article.get("cluster_name") or "").strip()
        if not cluster_name or cluster_name == "uncategorized":
            continue
        cluster_map.setdefault(cluster_name, []).append(article)

    cluster_map = {name: unique_articles(items) for name, items in cluster_map.items()}

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
        cluster_config = resolve_cluster_config(
            cluster_name=cluster_name,
            articles=articles_sorted,
            clusters_by_id=clusters_by_id,
            clusters_by_name=clusters_by_name,
        )
        related_clusters = choose_related_clusters(
            cluster_name=cluster_name,
            cluster_config=cluster_config,
            clusters_by_id=clusters_by_id,
            articles_by_cluster_name=cluster_map,
        )
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
        if cluster_config and str(cluster_config.get("description") or "").strip():
            description = str(cluster_config.get("description") or "").strip()
        frontmatter = build_frontmatter(
            cluster_name=cluster_name,
            cluster_slug=cluster_slug,
            cluster_intro=cluster_intro,
            description=description,
            article_count=len(articles_sorted),
            primary_keywords=keyword_reference,
            generated_at=generated_at,
        )
        body = build_body(cluster_name, articles_sorted, cluster_config, related_clusters)
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
