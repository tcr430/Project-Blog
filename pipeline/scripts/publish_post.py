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


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


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


def normalize_affiliate_products(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []

    normalized_products: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue

        title = str(item.get("title") or "").strip()
        affiliate_url = str(item.get("affiliate_url") or "").strip()
        short_reason = str(item.get("short_reason") or item.get("reason_for_recommendation") or "").strip()
        if not title or not affiliate_url:
            continue

        normalized_products.append(
            {
                "title": title,
                "affiliate_url": affiliate_url,
                "short_reason": short_reason,
            }
        )

    return normalized_products


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
        "primary_keyword",
        "secondary_keywords",
        "topical_cluster",
        "cluster_keywords",
        "search_intent",
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
    primary_keyword = str(data["primary_keyword"]).strip()
    topical_cluster = str(data["topical_cluster"]).strip()
    search_intent = str(data["search_intent"]).strip()

    tags = normalize_tags(data["keywords"])
    secondary_keywords = normalize_tags(data["secondary_keywords"])
    cluster_keywords = normalize_tags(data["cluster_keywords"])
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
    affiliate_products = normalize_affiliate_products(data.get("affiliate_products", []))

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
    if not primary_keyword:
        raise ValueError("primary_keyword cannot be empty.")
    if not topical_cluster:
        raise ValueError("topical_cluster cannot be empty.")
    if not search_intent:
        raise ValueError("search_intent cannot be empty.")

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
        "primary_keyword": primary_keyword,
        "secondary_keywords": secondary_keywords,
        "topical_cluster": topical_cluster,
        "cluster_keywords": cluster_keywords,
        "search_intent": search_intent,
        "estimated_reading_time": estimated_reading_time,
        "hero_image_prompt": hero_image_prompt,
        "section_image_prompts": section_image_prompts,
        "pinterest_titles": pinterest_titles,
        "pinterest_descriptions": pinterest_descriptions,
        "affiliate_products": affiliate_products,
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


def strip_markdown_formatting(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text)
    cleaned = re.sub(r"!\[[^\]]*\]\([^)]+\)", " ", cleaned)
    cleaned = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", cleaned)
    cleaned = cleaned.replace("**", "").replace("__", "").replace("*", "").replace("_", "")
    cleaned = cleaned.replace("`", "")
    return normalize_whitespace(cleaned)


def strip_affiliate_url_only(text: str) -> str:
    cleaned = re.sub(r"\[[^\]]+\]\s*\(https?://[^)]+\)", "", text)
    cleaned = re.sub(r"https?://\S+", "", cleaned)
    cleaned = cleaned.replace("()", "")
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip(" \t\n-,:;")


def extract_faq_items(article_markdown: str) -> list[dict[str, str]]:
    faq_match = re.search(
        r"(?ims)^##\s+(faq|frequently asked questions)\s*$\n(?P<body>.*?)(?=^##\s+|\Z)",
        article_markdown,
    )
    if not faq_match:
        return []

    faq_body = faq_match.group("body").strip()
    if not faq_body:
        return []

    question_matches = list(re.finditer(r"(?im)^###\s+(.+?)\s*$", faq_body))
    faq_items: list[dict[str, str]] = []

    for index, match in enumerate(question_matches):
        question_text = strip_markdown_formatting(match.group(1))
        start = match.end()
        end = question_matches[index + 1].start() if index + 1 < len(question_matches) else len(faq_body)
        answer_text = strip_markdown_formatting(faq_body[start:end])
        if not question_text or not answer_text:
            continue
        faq_items.append({"question": question_text, "answer": answer_text})

    return faq_items[:5]


def build_product_card_html(title: str, description: str, affiliate_url: str) -> str:
    safe_title = yaml_escape(title)
    safe_description = yaml_escape(description)
    safe_url = yaml_escape(affiliate_url)
    return (
        '<div class="product-card">\n'
        f'  <div class="product-title">{safe_title}</div>\n'
        f'  <p class="product-desc">{safe_description}</p>\n'
        f'  <a href="{safe_url}" class="product-button" target="_blank" rel="nofollow sponsored noopener">View Product</a>\n'
        "</div>"
    )


def build_product_card_description(
    product_title: str,
    product_lookup: dict[str, dict[str, str]],
    paragraph: str,
) -> str:
    product = product_lookup.get(product_title.lower())
    if product:
        reason = str(product.get("short_reason") or "").strip()
        if reason:
            return reason[0].upper() + reason[1:]

    return f"{product_title} is a practical way to bring this section's styling direction home while keeping the look cohesive."


def should_replace_whole_paragraph(paragraph: str) -> bool:
    normalized = paragraph.strip().lower()
    affiliate_cues = (
        "this [",
        "the [",
        "consider this [",
        "a polished way to bring this look home is with [",
    )
    return len(normalized) <= 280 or normalized.startswith(affiliate_cues)


def convert_affiliate_links_to_product_cards(
    article_markdown: str,
    affiliate_products: list[dict[str, str]],
) -> str:
    link_pattern = re.compile(r"\[([^\]]+)\]\s*\((https?://[^)]+)\)")
    product_lookup = {
        str(product.get("title", "")).strip().lower(): product
        for product in affiliate_products
        if str(product.get("title", "")).strip()
    }
    paragraphs = article_markdown.split("\n\n")
    converted_paragraphs: list[str] = []
    rendered_urls: set[str] = set()

    for paragraph in paragraphs:
        matches = list(link_pattern.finditer(paragraph))
        if not matches:
            converted_paragraphs.append(paragraph)
            continue

        updated_paragraph = paragraph
        appended_cards: list[str] = []
        paragraph_replaced = False

        for match in matches:
            product_title = strip_markdown_formatting(match.group(1))
            affiliate_url = match.group(2).strip()
            if affiliate_url in rendered_urls:
                updated_paragraph = updated_paragraph.replace(match.group(0), product_title, 1)
                continue

            description = build_product_card_description(product_title, product_lookup, paragraph)
            card_html = build_product_card_html(product_title, description, affiliate_url)
            rendered_urls.add(affiliate_url)

            if should_replace_whole_paragraph(paragraph) and not paragraph_replaced:
                converted_paragraphs.append(card_html)
                paragraph_replaced = True
            else:
                updated_paragraph = updated_paragraph.replace(match.group(0), product_title, 1)
                appended_cards.append(card_html)

        if paragraph_replaced:
            continue

        converted_paragraphs.append(updated_paragraph)
        converted_paragraphs.extend(appended_cards)

    return "\n\n".join(block.rstrip() for block in converted_paragraphs if block.strip()).strip() + "\n"


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
    primary_keyword: str,
    secondary_keywords: list[str],
    topical_cluster: str,
    cluster_keywords: list[str],
    search_intent: str,
    faq_items: list[dict[str, str]],
) -> str:
    date_value = published_at.strftime("%Y-%m-%d %H:%M:%S")
    tag_values = ", ".join(f'"{yaml_escape(tag)}"' for tag in tags)
    category_values = ", ".join(f'"{yaml_escape(category)}"' for category in categories)
    featured_value = "true" if featured else "false"
    affiliate_disclosure_value = "true" if affiliate_disclosure else "false"
    secondary_keyword_values = ", ".join(f'"{yaml_escape(keyword)}"' for keyword in secondary_keywords)
    cluster_keyword_values = ", ".join(f'"{yaml_escape(keyword)}"' for keyword in cluster_keywords)
    faq_lines = ""
    if faq_items:
        faq_lines = "faq_items:\n" + "".join(
            (
                f'  - question: "{yaml_escape(item["question"])}"\n'
                f'    answer: "{yaml_escape(item["answer"])}"\n'
            )
            for item in faq_items
        )

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
        f'primary_keyword: "{yaml_escape(primary_keyword)}"\n'
        f"secondary_keywords: [{secondary_keyword_values}]\n"
        f'topical_cluster: "{yaml_escape(topical_cluster)}"\n'
        f"cluster_keywords: [{cluster_keyword_values}]\n"
        f'search_intent: "{yaml_escape(search_intent)}"\n'
        f"{faq_lines}"
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
        "primary_keyword": package["primary_keyword"],
        "secondary_keywords": package["secondary_keywords"],
        "topical_cluster": package["topical_cluster"],
        "cluster_keywords": package["cluster_keywords"],
        "search_intent": package["search_intent"],
        "estimated_reading_time": package["estimated_reading_time"],
        "author_id": package["author_id"],
        "author_name": package["author_name"],
        "categories": package["categories"],
        "excerpt": package["excerpt"],
        "featured": package["featured"],
        "keywords": package["keywords"],
        "primary_keyword": package["primary_keyword"],
        "secondary_keywords": package["secondary_keywords"],
        "topical_cluster": package["topical_cluster"],
        "cluster_keywords": package["cluster_keywords"],
        "search_intent": package["search_intent"],
        "hero_image_prompt": package["hero_image_prompt"],
        "section_image_prompts": package["section_image_prompts"],
        "hero_image_path": build_image_url(slug=package["slug"], filename="hero.png"),
        "hero_image_alt": build_hero_image_alt(package["title"]),
        "section_image_paths": section_image_paths,
        "section_image_alts": section_image_alts,
        "pinterest_titles": package["pinterest_titles"],
        "pinterest_descriptions": package["pinterest_descriptions"],
        "affiliate_products": package.get("affiliate_products", []),
        "article_relative_url": post_relative_url,
        "faq_items": package.get("faq_items", []),
        "post_path": str(post_path),
        "updated_at": datetime.now().isoformat(),
    }
    metadata_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata_path


def has_affiliate_links(article_markdown: str) -> bool:
    return bool(re.search(r"https?://", article_markdown))


def count_visible_affiliate_links(article_markdown: str) -> int:
    markdown_links = len(re.findall(r"\[[^\]]+\]\s*\(https?://[^)]+\)", article_markdown))
    card_links = len(re.findall(r'class="product-button"[^>]+href="https?://', article_markdown))
    return markdown_links + card_links


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
    markdown_body = convert_affiliate_links_to_product_cards(
        article_markdown=markdown_body,
        affiliate_products=package.get("affiliate_products", []),
    )
    markdown_body = markdown_body.rstrip() + "\n"
    faq_items = extract_faq_items(markdown_body)

    affiliate_disclosure = has_affiliate_links(markdown_body)
    visible_link_count = count_visible_affiliate_links(markdown_body)
    print(f"[publish] generated markdown visible affiliate links: {visible_link_count}")

    frontmatter = build_frontmatter(
        title=package["title"],
        published_at=published_at,
        description=package["meta_description"],
        tags=package["keywords"],
        estimated_reading_time=package["estimated_reading_time"],
        primary_keyword=package["primary_keyword"],
        secondary_keywords=package["secondary_keywords"],
        topical_cluster=package["topical_cluster"],
        cluster_keywords=package["cluster_keywords"],
        search_intent=package["search_intent"],
        faq_items=faq_items,
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
        package={**package, "faq_items": faq_items},
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
