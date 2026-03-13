from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

ARTICLE_METADATA_DIR = Path(__file__).resolve().parents[2] / "_data" / "article_metadata"
NEWSLETTER_DRAFTS_DIR = Path(__file__).resolve().parents[1] / "data" / "newsletter_drafts"
MAX_FEATURED_ARTICLES = 3
META_PREFIX = "<!-- newsletter_meta: "
META_SUFFIX = " -->"


@dataclass
class ArticleRecord:
    title: str
    slug: str
    meta_description: str
    excerpt: str
    article_relative_url: str
    categories: list[str]
    featured: bool
    published_date: date


@dataclass
class NewsletterHighlight:
    title: str
    category: str
    summary: str
    link: str


@dataclass
class NewsletterDraft:
    week_label: str
    subject_line: str
    preview_text: str
    intro: str
    highlights: list[NewsletterHighlight]
    design_pick: str
    sign_off: str
    meta: dict[str, Any]
    raw_markdown: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a weekly newsletter draft from articles published during the current week."
    )
    parser.add_argument(
        "--metadata-dir",
        type=str,
        default=str(ARTICLE_METADATA_DIR),
        help="Directory containing article metadata JSON files.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(NEWSLETTER_DRAFTS_DIR),
        help="Directory where newsletter draft markdown files should be written.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return data


def title_from_slug(slug: str) -> str:
    return re.sub(r"[-_]+", " ", slug).strip().title() or "Untitled Article"


def extract_published_date(path: Path) -> date:
    match = re.match(r"(?P<date>\d{4}-\d{2}-\d{2})-", path.stem)
    if not match:
        raise ValueError(f"Could not determine published date from metadata filename: {path.name}")
    return datetime.strptime(match.group("date"), "%Y-%m-%d").date()


def normalize_article_record(path: Path) -> ArticleRecord | None:
    data = load_json(path)
    published_date = extract_published_date(path)
    slug = str(data.get("slug") or "").strip()
    if not slug:
        return None

    title = str(data.get("title") or "").strip() or title_from_slug(slug)
    meta_description = str(data.get("meta_description") or "").strip()
    excerpt = str(data.get("excerpt") or meta_description).strip() or meta_description
    article_relative_url = str(data.get("article_relative_url") or "").strip()
    categories_raw = data.get("categories")
    categories = [str(item).strip() for item in categories_raw] if isinstance(categories_raw, list) else []
    featured = bool(data.get("featured", False))

    return ArticleRecord(
        title=title,
        slug=slug,
        meta_description=meta_description,
        excerpt=excerpt,
        article_relative_url=article_relative_url,
        categories=categories,
        featured=featured,
        published_date=published_date,
    )


def current_iso_week(today: date) -> tuple[int, int]:
    iso = today.isocalendar()
    return iso.year, iso.week


def select_articles_for_week(metadata_dir: Path, today: date) -> list[ArticleRecord]:
    week_year, week_number = current_iso_week(today)
    selected: list[ArticleRecord] = []

    for path in sorted(metadata_dir.glob("*.json")):
        try:
            record = normalize_article_record(path)
        except Exception:
            continue
        if record is None:
            continue

        record_week = record.published_date.isocalendar()
        if record_week.year == week_year and record_week.week == week_number:
            selected.append(record)

    selected.sort(key=lambda item: (item.featured, item.published_date, item.title.lower()), reverse=True)
    return selected[:MAX_FEATURED_ARTICLES]


def build_subject_line(articles: list[ArticleRecord], week_number: int) -> str:
    if not articles:
        return f"Week {week_number} at The Livin' Edit"
    if len(articles) == 1:
        return f"This week at The Livin' Edit: {articles[0].title}"
    return f"This week at The Livin' Edit: {articles[0].title} and more"


def build_preview_text(articles: list[ArticleRecord]) -> str:
    if not articles:
        return "A curated round-up of this week's latest decor stories."
    article_titles = [article.title for article in articles[:2]]
    return "Featuring " + ", ".join(article_titles) + "."


def build_intro_paragraph(articles: list[ArticleRecord]) -> str:
    if not articles:
        return (
            "A quieter week in the journal, but the newsletter draft is ready so the next run can pick up "
            "fresh stories as soon as they publish."
        )
    return (
        "This week's edit brings together the most useful stories we published, with a focus on practical decor "
        "ideas, warm editorial styling, and a few sharp takeaways you can use right away at home."
    )


def build_design_pick(articles: list[ArticleRecord]) -> str:
    if not articles:
        return "A sculptural ceramic vase or textured tray would make a strong styling pick when the next article batch lands."

    lead_article = articles[0]
    category = lead_article.categories[0] if lead_article.categories else "decor"
    return (
        f"Design pick of the week: a small, tactile accent that echoes this week's {category.lower()} focus, "
        "like a brushed-metal candle holder, handmade ceramic piece, or woven tray with real texture."
    )


def build_highlight_block(article: ArticleRecord) -> str:
    link = article.article_relative_url or "#"
    category = article.categories[0] if article.categories else "Story"
    summary = article.excerpt or article.meta_description or "A fresh editorial story from this week's publication run."
    return (
        f"### {article.title}\n"
        f"*{category}*  \n"
        f"{summary}\n"
        f"Read more: {link}\n"
    )


def build_sign_off() -> str:
    return "See you next week,\n\nThe Livin' Edit"


def build_article_signature(articles: list[ArticleRecord]) -> list[str]:
    return [f"{article.published_date.isoformat()}:{article.slug}" for article in articles]


def build_newsletter_meta(articles: list[ArticleRecord], today: date) -> dict[str, Any]:
    week_year, week_number = current_iso_week(today)
    return {
        "week": f"{week_year}-W{week_number:02d}",
        "articles": build_article_signature(articles),
    }


def load_existing_meta(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None

    first_line = path.read_text(encoding="utf-8-sig").splitlines()[:1]
    if not first_line:
        return None

    line = first_line[0].strip()
    if not (line.startswith(META_PREFIX) and line.endswith(META_SUFFIX)):
        return None

    raw_payload = line[len(META_PREFIX):-len(META_SUFFIX)]
    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def build_newsletter_markdown(articles: list[ArticleRecord], today: date) -> str:
    week_year, week_number = current_iso_week(today)
    subject_line = build_subject_line(articles, week_number)
    preview_text = build_preview_text(articles)
    intro = build_intro_paragraph(articles)
    design_pick = build_design_pick(articles)
    meta = build_newsletter_meta(articles, today)

    lines = [
        f"{META_PREFIX}{json.dumps(meta, ensure_ascii=False)}{META_SUFFIX}",
        "",
        "# Weekly Newsletter Draft",
        "",
        f"- Week: {week_year}-W{week_number:02d}",
        f"- Subject Line: {subject_line}",
        f"- Preview Text: {preview_text}",
        "",
        "## Intro",
        intro,
        "",
        "## Weekly Highlights",
    ]

    if articles:
        for article in articles:
            lines.extend([build_highlight_block(article), ""])
    else:
        lines.append("No new articles were published during this week yet.")
        lines.append("")

    lines.extend(
        [
            "## Design Pick of the Week",
            design_pick,
            "",
            "## Sign-off",
            build_sign_off(),
            "",
        ]
    )

    return "\n".join(lines).strip() + "\n"


def build_output_path(output_dir: Path, today: date) -> Path:
    week_year, week_number = current_iso_week(today)
    return output_dir / f"{week_year}-week-{week_number:02d}-newsletter.md"


def split_draft_sections(markdown_body: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current_key: str | None = None

    for line in markdown_body.splitlines():
        if line.startswith("## "):
            current_key = line[3:].strip().lower()
            sections[current_key] = []
            continue
        if current_key is not None:
            sections[current_key].append(line)

    return {key: "\n".join(value).strip() for key, value in sections.items()}


def parse_highlights(section_text: str) -> list[NewsletterHighlight]:
    if not section_text or section_text.strip() == "No new articles were published during this week yet.":
        return []

    blocks = [block.strip() for block in re.split(r"(?m)^###\s+", section_text) if block.strip()]
    highlights: list[NewsletterHighlight] = []

    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if not lines:
            continue

        title = lines[0]
        category = "Story"
        summary_lines: list[str] = []
        link = "#"

        for line in lines[1:]:
            if line.startswith("*") and line.endswith("*"):
                category = line.strip("*").strip()
            elif line.lower().startswith("read more:"):
                link = line.split(":", 1)[1].strip() or "#"
            else:
                summary_lines.append(line)

        summary = " ".join(summary_lines).strip() or "A fresh editorial story from this week's publication run."
        highlights.append(
            NewsletterHighlight(
                title=title,
                category=category,
                summary=summary,
                link=link,
            )
        )

    return highlights


def parse_weekly_newsletter_draft(path: Path) -> NewsletterDraft:
    raw_markdown = path.read_text(encoding="utf-8-sig")
    lines = raw_markdown.splitlines()
    meta = load_existing_meta(path) or {}

    subject_line = ""
    preview_text = ""
    week_label = ""
    body_start = 0

    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("- Week:"):
            week_label = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- Subject Line:"):
            subject_line = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- Preview Text:"):
            preview_text = stripped.split(":", 1)[1].strip()
            body_start = index + 1
            break

    body_text = "\n".join(lines[body_start:]).strip()
    sections = split_draft_sections(body_text)
    intro = sections.get("intro", "")
    highlights = parse_highlights(sections.get("weekly highlights", ""))
    design_pick = sections.get("design pick of the week", "")
    sign_off = sections.get("sign-off", "")

    if not week_label:
        week_label = str(meta.get("week") or path.stem)
    if not subject_line:
        raise ValueError(f"Could not parse subject line from newsletter draft: {path}")
    if not preview_text:
        raise ValueError(f"Could not parse preview text from newsletter draft: {path}")

    return NewsletterDraft(
        week_label=week_label,
        subject_line=subject_line,
        preview_text=preview_text,
        intro=intro,
        highlights=highlights,
        design_pick=design_pick,
        sign_off=sign_off,
        meta=meta,
        raw_markdown=raw_markdown,
    )


def generate_weekly_newsletter_draft(
    metadata_dir: Path = ARTICLE_METADATA_DIR,
    output_dir: Path = NEWSLETTER_DRAFTS_DIR,
    today: date | None = None,
) -> Path:
    current_day = today or datetime.now().date()
    print("[newsletter] selecting weekly articles")
    articles = select_articles_for_week(metadata_dir=metadata_dir, today=current_day)
    print(f"[newsletter] selected {len(articles)} article(s) for this week's draft")

    output_path = build_output_path(output_dir=output_dir, today=current_day)
    current_meta = build_newsletter_meta(articles, current_day)
    existing_meta = load_existing_meta(output_path)
    if existing_meta == current_meta:
        print("[newsletter] no weekly changes detected; keeping existing draft")
        return output_path

    print("[newsletter] building weekly newsletter draft")
    markdown = build_newsletter_markdown(articles=articles, today=current_day)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    return output_path


def main() -> int:
    args = parse_args()
    try:
        output_path = generate_weekly_newsletter_draft(
            metadata_dir=Path(args.metadata_dir),
            output_dir=Path(args.output_dir),
        )
        print(output_path)
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
