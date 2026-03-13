from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


SECTION_COUNT = 5
PINTEREST_ITEM_COUNT = 5
AUTHOR_IDS = ["elena_hart", "sophie_bennett", "marco_alvarez"]
AUTHOR_NAME_MAP = {
    "elena_hart": "Elena Hart",
    "sophie_bennett": "Sophie Bennett",
    "marco_alvarez": "Marco Alvarez",
}
CATEGORY_RULES = [
    ("Mistakes & Fixes", ["mistake", "fix", "avoid"]),
    ("Ideas", ["idea", "inspiration"]),
    ("Trends", ["trend", "trending"]),
    ("Styling Advice", ["style", "styling", "decor", "guide"]),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish a generated article package JSON as a Jekyll markdown post."
    )
    parser.add_argument(
        "package_json_path",
        type=str,
        help="Path to article package JSON file.",
    )
    return parser.parse_args()


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9\s-]", "", text).strip().lower()
    slug = re.sub(r"[\s_-]+", "-", cleaned).strip("-")
    return slug or "decor-article"


def yaml_escape(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return escaped


def normalize_tags(keywords: Any) -> list[str]:
    if isinstance(keywords, list):
        tags = [str(item).strip() for item in keywords if str(item).strip()]
    elif isinstance(keywords, str):
        tags = [item.strip() for item in keywords.split(",") if item.strip()]
    else:
        raise ValueError("keywords must be a list of strings or a comma-separated string.")

    if not tags:
        raise ValueError("keywords/tags cannot be empty.")

    return tags


def normalize_string_list(value: Any, field_name: str, expected_count: int) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list.")

    items = [str(item).strip() for item in value if str(item).strip()]
    if len(items) != expected_count:
        raise ValueError(f"{field_name} must contain exactly {expected_count} items.")

    return items


def choose_author(slug: str) -> str:
    digest = hashlib.sha256(slug.encode("utf-8")).hexdigest()
    index = int(digest, 16) % len(AUTHOR_IDS)
    return AUTHOR_IDS[index]


def build_author_name(author_id: str) -> str:
    if author_id in AUTHOR_NAME_MAP:
        return AUTHOR_NAME_MAP[author_id]

    cleaned = author_id.replace("_", " ").replace("-", " ").strip()
    return cleaned.title() if cleaned else "The Livin' Edit"


def derive_categories(title: str, tags: list[str]) -> list[str]:
    haystack = f"{title} {' '.join(tags)}".lower()
    for category, keywords in CATEGORY_RULES:
        if any(keyword in haystack for keyword in keywords):
            return [category]
    return ["Styling Advice"]


def derive_excerpt(meta_description: str, article_markdown: str) -> str:
    if meta_description.strip():
        return meta_description.strip()

    without_headings = re.sub(r"(?m)^#+\s+", "", article_markdown)
    without_images = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", without_headings)
    without_links = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", without_images)
    words = re.findall(r"\S+", without_links)
    return " ".join(words[:32]).strip()


def derive_featured(categories: list[str], tags: list[str]) -> bool:
    if categories and categories[0] == "Trends":
        return True
    return any("trend" in tag.lower() for tag in tags)


def slugify_path_part(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9\s-]", " ", text).strip().lower()
    slug = re.sub(r"[\s_-]+", "-", cleaned).strip("-")
    return slug or "posts"


def validate_article_package(data: dict[str, Any]) -> dict[str, Any]:
    required_fields = [
        "title",
        "slug",
        "meta_description",
        "keywords",
        "estimated_reading_time",
        "hero_image_prompt",
        "section_image_prompts",
        "pinterest_titles",
        "pinterest_descriptions",
        "article_markdown",
    ]
    missing = [field for field in required_fields if field not in data]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    title = str(data["title"]).strip()
    slug = slugify(str(data["slug"]).strip())
    meta_description = str(data["meta_description"]).strip()
    article_markdown = str(data["article_markdown"]).strip()
    estimated_reading_time = str(data["estimated_reading_time"]).strip()
    hero_image_prompt = str(data["hero_image_prompt"]).strip()

    tags = normalize_tags(data["keywords"])
    section_image_prompts = normalize_string_list(
        data["section_image_prompts"],
        field_name="section_image_prompts",
        expected_count=SECTION_COUNT,
    )
    pinterest_titles = normalize_string_list(
        data["pinterest_titles"],
        field_name="pinterest_titles",
        expected_count=PINTEREST_ITEM_COUNT,
    )
    pinterest_descriptions = normalize_string_list(
        data["pinterest_descriptions"],
        field_name="pinterest_descriptions",
        expected_count=PINTEREST_ITEM_COUNT,
    )

    if not title:
        raise ValueError("title cannot be empty.")
    if not slug:
        raise ValueError("slug cannot be empty.")
    if not meta_description:
        raise ValueError("meta_description cannot be empty.")
    if not article_markdown:
        raise ValueError("article_markdown cannot be empty.")
    if not estimated_reading_time:
        raise ValueError("estimated_reading_time cannot be empty.")
    if not hero_image_prompt:
        raise ValueError("hero_image_prompt cannot be empty.")

    author_id = str(data.get("author_id") or data.get("author") or choose_author(slug)).strip()
    author_name = build_author_name(author_id)
    categories_raw = data.get("categories")
    if isinstance(categories_raw, list):
        categories = [str(item).strip() for item in categories_raw if str(item).strip()]
    else:
        categories = derive_categories(title=title, tags=tags)
    excerpt = str(data.get("excerpt") or derive_excerpt(meta_description, article_markdown)).strip()
    featured = bool(data.get("featured", derive_featured(categories, tags)))

    return {
        "title": title,
        "slug": slug,
        "meta_description": meta_description,
        "keywords": tags,
        "estimated_reading_time": estimated_reading_time,
        "hero_image_prompt": hero_image_prompt,
        "section_image_prompts": section_image_prompts,
        "pinterest_titles": pinterest_titles,
        "pinterest_descriptions": pinterest_descriptions,
        "article_markdown": article_markdown,
        "author_id": author_id,
        "author_name": author_name,
        "categories": categories,
        "excerpt": excerpt,
        "featured": featured,
    }


def load_article_package(package_json_path: Path) -> dict[str, Any]:
    if not package_json_path.exists():
        raise FileNotFoundError(f"Input JSON file not found: {package_json_path}")

    raw = package_json_path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Input file is not valid JSON: {package_json_path}") from exc

    if not isinstance(data, dict):
        raise ValueError("Input JSON must be an object.")

    return validate_article_package(data)


def build_image_url(slug: str, filename: str) -> str:
    return f"/assets/img/{slug}/{filename}"


def build_public_image_src(image_url: str) -> str:
    return "{{ '" + image_url + "' | relative_url }}"


def build_hero_image_alt(title: str) -> str:
    return f"{title} interior design inspiration"


def normalize_heading_text(heading: str) -> str:
    text = re.sub(r"^#+\s+", "", heading).strip()
    return re.sub(r"\s+", " ", text)


def build_section_image_alt(article_title: str, heading_text: str, section_number: int) -> str:
    cleaned_heading = normalize_heading_text(heading_text)
    if cleaned_heading:
        return f"{article_title} - {cleaned_heading.lower()} styling detail"
    return f"{article_title} section {section_number} interior styling detail"


def strip_leading_title(article_markdown: str) -> str:
    return re.sub(r"\A#\s+.+?(?:\n{2,}|\n(?=##\s)|\Z)", "", article_markdown.strip(), count=1, flags=re.DOTALL).strip()


def strip_intro_heading(article_markdown: str) -> str:
    return re.sub(
        r"\A##\s+(?:Introduction|Intro)\s*\n+",
        "",
        article_markdown.strip(),
        count=1,
        flags=re.IGNORECASE,
    ).strip()


def split_frontmatter(markdown_content: str) -> tuple[str, str]:
    match = re.match(r"\A---\n.*?\n---\n+", markdown_content, flags=re.DOTALL)
    if not match:
        return "", markdown_content
    return match.group(0), markdown_content[match.end():]


def strip_section_image_blocks(article_markdown: str) -> str:
    cleaned = re.sub(
        r"\n*<figure class=\"article-section-image\">.*?</figure>\n*",
        "\n\n",
        article_markdown,
        flags=re.DOTALL,
    )
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def build_section_image_block(image_url: str, alt_text: str) -> str:
    return (
        '<figure class="article-section-image">\n'
        f'  <img src="{build_public_image_src(image_url)}" alt="{alt_text}" loading="lazy">\n'
        '</figure>'
    )


def is_intro_heading(heading_line: str) -> bool:
    heading = normalize_heading_text(heading_line).lower()
    return heading in {"introduction", "intro"} or heading.startswith("introduction ")


def is_conclusion_heading(heading_line: str) -> bool:
    heading = normalize_heading_text(heading_line).lower()
    return heading in {"conclusion", "final thoughts", "closing thoughts", "wrap-up"} or heading.startswith(
        "conclusion "
    )


def is_body_section_heading(heading_line: str) -> bool:
    if not heading_line.strip().startswith("## "):
        return False
    return not is_intro_heading(heading_line) and not is_conclusion_heading(heading_line)


def build_existing_section_image_specs(
    metadata: dict[str, Any],
    article_title: str,
    project_root: Path,
) -> list[dict[str, str]]:
    section_paths = metadata.get("section_image_paths", [])
    section_alts = metadata.get("section_image_alts", [])

    specs: list[dict[str, str]] = []
    for index, image_path in enumerate(section_paths, start=1):
        relative_path = str(image_path).strip()
        if not relative_path:
            continue

        filesystem_path = project_root / Path(relative_path.lstrip("/").replace("/", os.sep))
        if not filesystem_path.exists():
            print(f"[images] section image skipped because file missing: {filesystem_path}")
            continue

        alt_text = ""
        if index - 1 < len(section_alts):
            alt_text = str(section_alts[index - 1]).strip()
        if not alt_text:
            alt_text = build_section_image_alt(
                article_title=article_title,
                heading_text="",
                section_number=index,
            )

        print(f"[images] section image ready: {filesystem_path}")
        specs.append({"image_url": relative_path, "alt_text": alt_text})

    return specs


def inject_section_images(article_markdown: str, image_specs: list[dict[str, str]]) -> str:
    lines = article_markdown.splitlines()
    eligible_indices = [index for index, line in enumerate(lines) if is_body_section_heading(line)]

    offset = 0
    for spec, line_index in zip(image_specs, eligible_indices):
        heading_index = line_index + offset
        insert_at = heading_index + 1

        while insert_at < len(lines) and not lines[insert_at].strip():
            insert_at += 1

        lines[insert_at:insert_at] = [
            "",
            build_section_image_block(image_url=spec["image_url"], alt_text=spec["alt_text"]),
            "",
        ]
        print(f"[images] section image inserted: {spec['image_url']}")
        offset += 3

    return "\n".join(lines).strip() + "\n"


def sync_post_images(post_path: str | Path, metadata_path: str | Path) -> Path:
    post_path = Path(post_path)
    metadata_path = Path(metadata_path)
    project_root = Path(__file__).resolve().parents[2]

    if not post_path.exists():
        raise FileNotFoundError(f"Post markdown file not found: {post_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata JSON file not found: {metadata_path}")

    markdown_content = post_path.read_text(encoding="utf-8")
    frontmatter, body = split_frontmatter(markdown_content)
    if not body:
        raise ValueError(f"Post body is empty: {post_path}")

    metadata_raw = metadata_path.read_text(encoding="utf-8")
    metadata = json.loads(metadata_raw)

    article_title = str(metadata.get("title", "")).strip() or post_path.stem
    cleaned_body = strip_intro_heading(strip_section_image_blocks(body))
    image_specs = build_existing_section_image_specs(
        metadata=metadata,
        article_title=article_title,
        project_root=project_root,
    )

    if image_specs:
        synced_body = inject_section_images(cleaned_body, image_specs=image_specs)
    else:
        print(f"[images] no section images inserted for {post_path.name}")
        synced_body = cleaned_body.rstrip() + "\n"

    post_path.write_text(f"{frontmatter}{synced_body}", encoding="utf-8")
    return post_path


def build_frontmatter(
    title: str,
    published_at: datetime,
    description: str,
    tags: list[str],
    estimated_reading_time: str,
    hero_image_url: str,
    hero_image_alt: str,
    author_id: str,
    author_name: str,
    categories: list[str],
    excerpt: str,
    featured: bool,
    affiliate_disclosure: bool,
) -> str:
    date_value = published_at.strftime("%Y-%m-%d %H:%M:%S")
    tag_values = ", ".join(f'"{yaml_escape(tag)}"' for tag in tags)
    category_values = ", ".join(f'"{yaml_escape(category)}"' for category in categories)
    featured_value = "true" if featured else "false"
    affiliate_disclosure_value = "true" if affiliate_disclosure else "false"

    return (
        "---\n"
        "layout: post\n"
        f'title: "{yaml_escape(title)}"\n'
        f'date: "{date_value}"\n'
        f'description: "{yaml_escape(description)}"\n'
        f'excerpt: "{yaml_escape(excerpt)}"\n'
        f'author: "{yaml_escape(author_name)}"\n'
        f'author_id: "{yaml_escape(author_id)}"\n'
        f"categories: [{category_values}]\n"
        f"tags: [{tag_values}]\n"
        f"featured: {featured_value}\n"
        f'estimated_reading_time: "{yaml_escape(estimated_reading_time)}"\n'
        f'image: "{yaml_escape(hero_image_url)}"\n'
        f'image_alt: "{yaml_escape(hero_image_alt)}"\n'
        f'affiliate_disclosure: {affiliate_disclosure_value}\n'
        "---\n\n"
    )


def build_post_path(project_root: Path, slug: str, published_at: datetime) -> Path:
    date_prefix = published_at.strftime("%Y-%m-%d")
    filename = f"{date_prefix}-{slug}.md"
    return project_root / "_posts" / filename


def build_metadata_path(project_root: Path, post_path: Path) -> Path:
    metadata_dir = project_root / "_data" / "article_metadata"
    return metadata_dir / f"{post_path.stem}.json"


def build_post_relative_url(categories: list[str], published_at: datetime, slug: str) -> str:
    category_part = slugify_path_part(categories[0]) if categories else "posts"
    return f"/{category_part}/{published_at.strftime('%Y/%m/%d')}/{slug}/"


def save_article_metadata(
    package: dict[str, Any],
    post_path: Path,
    metadata_path: Path,
    published_at: datetime,
) -> Path:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    section_image_paths = [
        build_image_url(slug=package["slug"], filename=f"section-{index}.png")
        for index in range(1, SECTION_COUNT + 1)
    ]
    section_image_alts = [
        build_section_image_alt(
            article_title=package["title"],
            heading_text="",
            section_number=index,
        )
        for index in range(1, SECTION_COUNT + 1)
    ]
    post_relative_url = build_post_relative_url(
        categories=package["categories"],
        published_at=published_at,
        slug=package["slug"],
    )

    payload = {
        "title": package["title"],
        "slug": package["slug"],
        "meta_description": package["meta_description"],
        "estimated_reading_time": package["estimated_reading_time"],
        "author_id": package["author_id"],
        "author_name": package["author_name"],
        "categories": package["categories"],
        "excerpt": package["excerpt"],
        "featured": package["featured"],
        "hero_image_prompt": package["hero_image_prompt"],
        "section_image_prompts": package["section_image_prompts"],
        "hero_image_path": build_image_url(slug=package["slug"], filename="hero.png"),
        "hero_image_alt": build_hero_image_alt(package["title"]),
        "section_image_paths": section_image_paths,
        "section_image_alts": section_image_alts,
        "pinterest_titles": package["pinterest_titles"],
        "pinterest_descriptions": package["pinterest_descriptions"],
        "article_relative_url": post_relative_url,
        "post_path": str(post_path),
        "updated_at": datetime.now().isoformat(),
    }
    metadata_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata_path


def has_affiliate_links(article_markdown: str) -> bool:
    return bool(re.search(r"https?://", article_markdown))


def count_visible_affiliate_links(article_markdown: str) -> int:
    return len(re.findall(r"\[[^\]]+\]\(https?://[^)]+\)", article_markdown))


def publish_post_from_package_file(package_json_path: str | Path) -> dict[str, Path]:
    package_path = Path(package_json_path)
    project_root = Path(__file__).resolve().parents[2]

    package = load_article_package(package_path)

    published_at = datetime.now()
    output_path = build_post_path(project_root=project_root, slug=package["slug"], published_at=published_at)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    hero_image_url = build_image_url(slug=package["slug"], filename="hero.png")
    hero_image_alt = build_hero_image_alt(package["title"])

    markdown_body = strip_leading_title(package["article_markdown"])
    markdown_body = strip_intro_heading(markdown_body)
    markdown_body = strip_section_image_blocks(markdown_body)
    markdown_body = markdown_body.rstrip() + "\n"

    affiliate_disclosure = has_affiliate_links(markdown_body)
    visible_link_count = count_visible_affiliate_links(markdown_body)
    print(f"[publish] generated markdown visible affiliate links: {visible_link_count}")

    frontmatter = build_frontmatter(
        title=package["title"],
        published_at=published_at,
        description=package["meta_description"],
        tags=package["keywords"],
        estimated_reading_time=package["estimated_reading_time"],
        hero_image_url=hero_image_url,
        hero_image_alt=hero_image_alt,
        author_id=package["author_id"],
        author_name=package["author_name"],
        categories=package["categories"],
        excerpt=package["excerpt"],
        featured=package["featured"],
        affiliate_disclosure=affiliate_disclosure,
    )

    markdown_content = f"{frontmatter}{markdown_body.rstrip()}\n"
    output_path.write_text(markdown_content, encoding="utf-8")
    print(f"[publish] published markdown visible affiliate links: {count_visible_affiliate_links(markdown_content)}")

    metadata_path = build_metadata_path(project_root=project_root, post_path=output_path)
    save_article_metadata(
        package=package,
        post_path=output_path,
        metadata_path=metadata_path,
        published_at=published_at,
    )

    return {"post_path": output_path, "metadata_path": metadata_path}


def publish_post(package_json_path: str | Path) -> dict[str, Path]:
    return publish_post_from_package_file(package_json_path)


def main() -> int:
    args = parse_args()

    try:
        result = publish_post_from_package_file(args.package_json_path)
        print(result["post_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
