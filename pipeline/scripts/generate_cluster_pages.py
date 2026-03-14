from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
DEFAULT_CLUSTER_REPORT_PATH = Path(__file__).resolve().parents[1] / "data" / "keyword_cluster_report.json"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "clusters"
MIN_CLUSTER_ARTICLES = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Jekyll cluster hub pages from article-cluster metadata."
    )
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--cluster-report-path", type=str, default=str(DEFAULT_CLUSTER_REPORT_PATH))
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


def titleize_cluster_name(cluster_name: str) -> str:
    words = [word for word in re.split(r"\s+", cluster_name.strip()) if word]
    return " ".join(word.capitalize() for word in words) or "Cluster"


def normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def build_cluster_intro(
    *,
    cluster_name: str,
    primary_keywords: list[str],
    article_count: int,
    status: str,
) -> str:
    title_case_name = titleize_cluster_name(cluster_name)
    keyword_phrase = ", ".join(primary_keywords[:3]) if primary_keywords else cluster_name
    status_phrase_map = {
        "strong": "This is one of the strongest topical areas on the site right now, with enough coverage to support both discovery and deeper follow-up reading.",
        "growing": "This cluster is actively growing, which makes it a useful place to explore the core topic plus the practical styling questions that sit around it.",
        "underdeveloped": "This cluster is still early, but the existing articles already outline the most useful entry points for readers exploring the topic.",
    }
    status_phrase = status_phrase_map.get(status, status_phrase_map["growing"])

    paragraphs = [
        (
            f"{title_case_name} is a practical decorating topic because it sits at the intersection of inspiration and decision-making. "
            f"People searching this theme are usually trying to solve a real room problem, refine a style direction, or understand how a trend works in everyday spaces. "
            f"This cluster brings those questions together in one place so readers can move from broad ideas to more specific decorating guidance without having to jump around the site."
        ),
        (
            f"The current coverage focuses on search-led angles such as {keyword_phrase}. "
            f"That means the articles in this hub are designed to answer different versions of the same decorating intent: what the look is, how to style it well, which mistakes to avoid, and how to make it work in a real room. "
            f"Instead of treating the topic as a one-off trend, the goal is to build a stronger editorial pathway around the full cluster."
        ),
        (
            f"There are currently {article_count} article{'s' if article_count != 1 else ''} in this cluster. "
            f"{status_phrase} "
            f"As more articles are added, this page will continue to act as a stable hub for related ideas, supporting keywords, and internal paths between closely connected posts."
        ),
        (
            f"If you are exploring {cluster_name}, start with the article list below and work from the broadest idea-led pieces into the more specific styling or problem-solving guides. "
            f"That reading path tends to mirror how people actually search: first for inspiration, then for clarity, and finally for confident decorating decisions."
        ),
    ]
    return "\n\n".join(paragraphs)


def build_cluster_page_frontmatter(
    *,
    cluster_name: str,
    cluster_slug: str,
    cluster_intro: str,
    article_count: int,
    primary_keywords: list[str],
    generated_at: str,
    description: str,
) -> str:
    primary_keyword_values = ", ".join(f'"{yaml_escape(keyword)}"' for keyword in primary_keywords[:6])
    return (
        "---\n"
        "layout: cluster\n"
        f'title: "{yaml_escape(titleize_cluster_name(cluster_name))}"\n'
        f'permalink: "/clusters/{cluster_slug}/"\n'
        f'description: "{yaml_escape(description)}"\n'
        f'cluster_name: "{yaml_escape(cluster_name)}"\n'
        f'cluster_slug: "{yaml_escape(cluster_slug)}"\n'
        f"cluster_article_count: {article_count}\n"
        f"cluster_primary_keywords: [{primary_keyword_values}]\n"
        f'cluster_generated_at: "{yaml_escape(generated_at)}"\n'
        f'cluster_intro: "{yaml_escape(cluster_intro)}"\n'
        "generated_cluster_page: true\n"
        "---\n\n"
    )


def build_cluster_page_body(articles: list[dict[str, Any]]) -> str:
    lines = [
        "## Articles in this cluster",
        "",
    ]
    for article in articles:
        title = str(article.get("article_title") or article.get("article_slug") or "").strip()
        permalink = str(article.get("permalink") or "#").strip() or "#"
        publish_date = str(article.get("publish_date") or "").strip()
        primary_keyword = str(article.get("primary_keyword") or "").strip()
        excerpt = str(article.get("excerpt") or "").strip()

        meta_parts = [part for part in [publish_date, primary_keyword] if part]
        lines.append(f"### [{title}]({permalink})")
        if meta_parts:
            lines.append(f"*{' | '.join(meta_parts)}*")
        if excerpt:
            lines.append(excerpt)
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def article_sort_key(article: dict[str, Any]) -> tuple[str, str]:
    return (
        str(article.get("publish_date") or ""),
        str(article.get("article_slug") or ""),
    )


def build_cluster_pages(
    *,
    cluster_index_path: Path,
    cluster_report_path: Path,
    output_dir: Path,
    min_articles: int,
) -> dict[str, Any]:
    index_payload = load_json(cluster_index_path, default={})
    report_payload = load_json(cluster_report_path, default={})

    articles = index_payload.get("articles", []) if isinstance(index_payload, dict) else []
    clusters = report_payload.get("clusters", []) if isinstance(report_payload, dict) else []

    cluster_map: dict[str, list[dict[str, Any]]] = {}
    for article in articles:
        if not isinstance(article, dict):
            continue
        cluster_name = str(article.get("cluster_name") or "").strip()
        if not cluster_name or cluster_name == "uncategorized":
            continue
        cluster_map.setdefault(cluster_name, []).append(article)

    cluster_report_map: dict[str, dict[str, Any]] = {}
    for cluster_row in clusters:
        if not isinstance(cluster_row, dict):
            continue
        cluster_name = str(cluster_row.get("cluster_name") or "").strip()
        if cluster_name:
            cluster_report_map[cluster_name] = cluster_row

    output_dir.mkdir(parents=True, exist_ok=True)
    eligible_slugs: set[str] = set()
    generated_paths: list[Path] = []
    generated_at = datetime.now(timezone.utc).isoformat()

    for cluster_name, cluster_articles in sorted(cluster_map.items()):
        if len(cluster_articles) < max(1, min_articles):
            continue

        cluster_slug = slugify(cluster_name)
        eligible_slugs.add(cluster_slug)
        cluster_articles_sorted = sorted(cluster_articles, key=article_sort_key, reverse=True)
        report_row = cluster_report_map.get(cluster_name, {})
        primary_keywords = normalize_string_list(report_row.get("primary_keywords_used"))
        if not primary_keywords:
            primary_keywords = [
                str(item.get("primary_keyword") or "").strip()
                for item in cluster_articles_sorted
                if str(item.get("primary_keyword") or "").strip()
            ]
        status = str(report_row.get("status") or "growing").strip() or "growing"
        cluster_intro = build_cluster_intro(
            cluster_name=cluster_name,
            primary_keywords=primary_keywords,
            article_count=len(cluster_articles_sorted),
            status=status,
        )
        description = f"Explore {cluster_name} articles, styling guides, and related decor ideas from The Livin' Edit."
        frontmatter = build_cluster_page_frontmatter(
            cluster_name=cluster_name,
            cluster_slug=cluster_slug,
            cluster_intro=cluster_intro,
            article_count=len(cluster_articles_sorted),
            primary_keywords=primary_keywords,
            generated_at=generated_at,
            description=description,
        )
        body = build_cluster_page_body(cluster_articles_sorted)
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
        result = build_cluster_pages(
            cluster_index_path=Path(args.cluster_index_path),
            cluster_report_path=Path(args.cluster_report_path),
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
