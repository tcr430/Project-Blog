from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from topic_clusters import TopicCandidate, build_manual_topic_candidate, load_default_topic_clusters, normalize_text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_METADATA_DIR = PROJECT_ROOT / "_data" / "article_metadata"
DEFAULT_POSTS_DIR = PROJECT_ROOT / "_posts"
INSERT_BEFORE_KEYS = ("featured", "estimated_reading_time", "image")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill keyword and cluster metadata for older published articles."
    )
    parser.add_argument("--metadata-dir", type=str, default=str(DEFAULT_METADATA_DIR))
    parser.add_argument("--posts-dir", type=str, default=str(DEFAULT_POSTS_DIR))
    return parser.parse_args()


def split_frontmatter(markdown_content: str) -> tuple[str, str]:
    if not markdown_content.startswith("---\n"):
        return "", markdown_content

    parts = markdown_content.split("\n---\n", 1)
    if len(parts) != 2:
        return "", markdown_content

    return parts[0][4:], parts[1]


def load_json(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8-sig").strip()
    payload = json.loads(raw) if raw else {}
    return payload if isinstance(payload, dict) else {}


def normalize_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


def tokenize(value: str) -> set[str]:
    return {token for token in re.split(r"\W+", normalize_text(value)) if token}


def score_cluster(cluster_name: str, keywords: list[str], context: str, context_tokens: set[str]) -> tuple[int, str]:
    best_keyword = keywords[0] if keywords else cluster_name
    best_score = 0

    cluster_tokens = tokenize(cluster_name)
    cluster_overlap = len(cluster_tokens & context_tokens)
    if cluster_name and cluster_name in context:
        best_score += 6
    best_score += cluster_overlap * 2

    for keyword in keywords:
        keyword_tokens = tokenize(keyword)
        keyword_score = len(keyword_tokens & context_tokens)
        if keyword and keyword in context:
            keyword_score += 6
        if keyword_score > best_score:
            best_score = keyword_score
            best_keyword = keyword

    return best_score, best_keyword


def infer_topic_candidate(metadata: dict[str, Any], frontmatter_raw: str) -> TopicCandidate:
    slug = str(metadata.get("slug") or "").strip()
    title = str(metadata.get("title") or "").strip()
    tags = normalize_list(metadata.get("keywords") or metadata.get("tags"))
    categories = normalize_list(metadata.get("categories"))

    if not tags:
        tags = extract_frontmatter_list(frontmatter_raw, "tags")
    if not categories:
        categories = extract_frontmatter_list(frontmatter_raw, "categories")

    context_parts = [slug, title, " ".join(tags), " ".join(categories)]
    context = " ".join(normalize_text(item) for item in context_parts if item).strip()
    context_tokens = tokenize(context)

    clusters = load_default_topic_clusters()
    best_cluster = None
    best_score = -1
    best_keyword = ""
    for cluster in clusters:
        score, keyword = score_cluster(cluster["cluster_name"], cluster["keywords"], context, context_tokens)
        if score > best_score:
            best_cluster = cluster
            best_score = score
            best_keyword = keyword

    if best_cluster is None or best_score < 3:
        return build_manual_topic_candidate(title or slug)

    secondary_keywords = [keyword for keyword in best_cluster["keywords"] if keyword != best_keyword][:4]
    return {
        "trend_cluster": best_cluster["cluster_name"],
        "trend_keyword": best_keyword,
        "primary_keyword": best_keyword,
        "secondary_keywords": secondary_keywords,
        "cluster_keywords": best_cluster["keywords"],
        "search_intent": "ideas" if "ideas" in best_keyword else ("how_to" if best_keyword.startswith("how to ") else "styling_advice"),
        "season": best_cluster.get("season", ""),
        "holiday": best_cluster.get("holiday", ""),
        "source": "cluster_backfill",
    }


def extract_frontmatter_list(frontmatter_raw: str, key: str) -> list[str]:
    pattern = re.compile(rf"^{re.escape(key)}:\s*\[(.*?)\]\s*$", re.MULTILINE)
    match = pattern.search(frontmatter_raw)
    if not match:
        return []
    payload = match.group(1).strip()
    if not payload:
        return []
    items = []
    for part in payload.split(","):
        cleaned = part.strip().strip('"').strip("'")
        if cleaned:
            items.append(cleaned)
    return items


def yaml_quote(value: str) -> str:
    escaped = str(value).replace('"', '\\"')
    return f'"{escaped}"'


def yaml_inline_list(values: list[str]) -> str:
    rendered = ", ".join(yaml_quote(value) for value in values)
    return f"[{rendered}]"


def set_or_insert_frontmatter_line(frontmatter_raw: str, key: str, value_repr: str) -> str:
    lines = frontmatter_raw.splitlines()
    prefix = f"{key}:"
    replacement = f"{key}: {value_repr}"

    for index, line in enumerate(lines):
        if line.startswith(prefix):
            lines[index] = replacement
            return "\n".join(lines)

    insert_at = len(lines)
    for index, line in enumerate(lines):
        if any(line.startswith(f"{candidate}:") for candidate in INSERT_BEFORE_KEYS):
            insert_at = index
            break

    lines.insert(insert_at, replacement)
    return "\n".join(lines)


def update_post_frontmatter(post_path: Path, topic_candidate: TopicCandidate) -> None:
    content = post_path.read_text(encoding="utf-8")
    frontmatter_raw, body = split_frontmatter(content)
    if not frontmatter_raw:
        return

    updated_frontmatter = frontmatter_raw
    updated_frontmatter = set_or_insert_frontmatter_line(
        updated_frontmatter,
        "primary_keyword",
        yaml_quote(topic_candidate["primary_keyword"]),
    )
    updated_frontmatter = set_or_insert_frontmatter_line(
        updated_frontmatter,
        "secondary_keywords",
        yaml_inline_list(topic_candidate["secondary_keywords"]),
    )
    updated_frontmatter = set_or_insert_frontmatter_line(
        updated_frontmatter,
        "topical_cluster",
        yaml_quote(topic_candidate["trend_cluster"]),
    )
    updated_frontmatter = set_or_insert_frontmatter_line(
        updated_frontmatter,
        "cluster_keywords",
        yaml_inline_list(topic_candidate["cluster_keywords"]),
    )
    updated_frontmatter = set_or_insert_frontmatter_line(
        updated_frontmatter,
        "search_intent",
        yaml_quote(topic_candidate["search_intent"]),
    )

    post_path.write_text(f"---\n{updated_frontmatter}\n---\n{body.lstrip()}", encoding="utf-8")


def update_metadata(metadata_path: Path, metadata: dict[str, Any], topic_candidate: TopicCandidate) -> None:
    metadata["primary_keyword"] = topic_candidate["primary_keyword"]
    metadata["secondary_keywords"] = topic_candidate["secondary_keywords"]
    metadata["topical_cluster"] = topic_candidate["trend_cluster"]
    metadata["cluster_keywords"] = topic_candidate["cluster_keywords"]
    metadata["search_intent"] = topic_candidate["search_intent"]
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")


def backfill_cluster_metadata(metadata_dir: Path, posts_dir: Path) -> dict[str, Any]:
    updated: list[dict[str, str]] = []
    for metadata_path in sorted(metadata_dir.glob("*.json")):
        metadata = load_json(metadata_path)
        slug = str(metadata.get("slug") or metadata_path.stem).strip()
        post_matches = sorted(posts_dir.glob(f"*-{slug}.md"))
        if not post_matches:
            continue

        post_path = post_matches[0]
        content = post_path.read_text(encoding="utf-8")
        frontmatter_raw, _ = split_frontmatter(content)
        topic_candidate = infer_topic_candidate(metadata, frontmatter_raw)
        update_metadata(metadata_path, metadata, topic_candidate)
        update_post_frontmatter(post_path, topic_candidate)
        updated.append(
            {
                "slug": slug,
                "cluster": topic_candidate["trend_cluster"],
                "primary_keyword": topic_candidate["primary_keyword"],
            }
        )

    return {"updated_count": len(updated), "updated_articles": updated}


def main() -> int:
    args = parse_args()
    result = backfill_cluster_metadata(Path(args.metadata_dir), Path(args.posts_dir))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
