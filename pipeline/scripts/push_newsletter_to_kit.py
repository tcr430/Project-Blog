from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any
from urllib import error, request

from dotenv import load_dotenv

from generate_weekly_newsletter import NEWSLETTER_DRAFTS_DIR, NewsletterDraft, parse_weekly_newsletter_draft

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"
KIT_API_URL = "https://api.kit.com/v4/broadcasts"


class KitConfigurationError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Push the latest weekly newsletter draft to Kit as a draft broadcast."
    )
    parser.add_argument(
        "--draft-path",
        type=str,
        default=None,
        help="Optional path to a weekly newsletter draft markdown file.",
    )
    return parser.parse_args()


def load_env() -> None:
    load_dotenv(ENV_PATH)


def latest_newsletter_draft(drafts_dir: Path = NEWSLETTER_DRAFTS_DIR) -> Path:
    draft_paths = sorted(drafts_dir.glob("*-newsletter.md"), key=lambda item: item.stat().st_mtime, reverse=True)
    if not draft_paths:
        raise FileNotFoundError(f"No newsletter drafts found in {drafts_dir}")
    return draft_paths[0]


def sidecar_path_for_draft(draft_path: Path) -> Path:
    return draft_path.with_name(f"{draft_path.stem}-kit.json")


def load_sidecar(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    return data if isinstance(data, dict) else None


def draft_signature(draft: NewsletterDraft) -> str:
    return json.dumps(draft.meta, ensure_ascii=False, sort_keys=True)


def require_kit_api_key() -> str:
    api_key = os.getenv("KIT_API_KEY", "").strip()
    if not api_key:
        raise KitConfigurationError(f"KIT_API_KEY was not found in {ENV_PATH}")
    return api_key


def optional_int_env(name: str) -> int | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    return int(raw)


def load_site_url_parts() -> tuple[str, str]:
    config_path = PROJECT_ROOT / "_config.yml"
    url = ""
    baseurl = ""

    for line in config_path.read_text(encoding="utf-8-sig").splitlines():
        stripped = line.strip()
        if stripped.startswith("url:"):
            url = stripped.split(":", 1)[1].strip().strip('"')
        elif stripped.startswith("baseurl:"):
            baseurl = stripped.split(":", 1)[1].strip().strip('"')

    return url.rstrip("/"), baseurl.rstrip("/")


def absolute_article_url(relative_url: str) -> str:
    site_url, baseurl = load_site_url_parts()
    if relative_url.startswith("http"):
        return relative_url
    normalized_relative = relative_url if relative_url.startswith("/") else f"/{relative_url}"
    return f"{site_url}{baseurl}{normalized_relative}"


def paragraph_html(text: str) -> str:
    return f"<p>{escape(text)}</p>"


def render_highlight_html(draft: NewsletterDraft) -> str:
    if not draft.highlights:
        return (
            '<div style="padding:20px 0;border-top:1px solid #e8e8e1;">'
            '<p style="margin:0;color:#6d6d69;">No new posts were published this week yet, but the next digest draft is ready to build on.</p>'
            '</div>'
        )

    blocks: list[str] = []
    for item in draft.highlights:
        link = absolute_article_url(item.link)
        block = (
            '<div style="padding:22px 0;border-top:1px solid #e8e8e1;">'
            f'<p style="margin:0 0 8px;font-family:Arial,sans-serif;font-size:12px;letter-spacing:0.12em;text-transform:uppercase;color:#8a8a83;">{escape(item.category)}</p>'
            f'<h2 style="margin:0 0 10px;font-family:Georgia,serif;font-size:26px;line-height:1.2;color:#181818;">{escape(item.title)}</h2>'
            f'<p style="margin:0 0 12px;font-size:16px;line-height:1.7;color:#5f5f5b;">{escape(item.summary)}</p>'
            f'<p style="margin:0;"><a href="{escape(link)}" style="color:#c24a28;text-decoration:none;font-weight:600;">Read the full post</a></p>'
            '</div>'
        )
        blocks.append(block)
    return "".join(blocks)


def render_newsletter_html(draft: NewsletterDraft) -> str:
    sign_off_paragraphs = [paragraph_html(item) for item in draft.sign_off.split("\n\n") if item.strip()]
    return (
        '<div style="margin:0 auto;max-width:680px;padding:36px 24px;background:#ffffff;color:#181818;">'
        '<p style="margin:0 0 10px;font-family:Arial,sans-serif;font-size:12px;letter-spacing:0.14em;text-transform:uppercase;color:#8a8a83;">The Livin&#39; Edit</p>'
        f'<h1 style="margin:0 0 18px;font-family:Georgia,serif;font-size:38px;line-height:1.1;color:#181818;">{escape(draft.subject_line)}</h1>'
        f'<p style="margin:0 0 28px;font-size:17px;line-height:1.75;color:#5f5f5b;">{escape(draft.intro)}</p>'
        '<div style="margin:0 0 28px;">'
        '<h2 style="margin:0 0 8px;font-family:Arial,sans-serif;font-size:14px;letter-spacing:0.12em;text-transform:uppercase;color:#8a8a83;">Weekly Highlights</h2>'
        f'{render_highlight_html(draft)}'
        '</div>'
        '<div style="margin:12px 0 28px;padding:20px 22px;border-radius:18px;background:#f5f5f2;">'
        '<h2 style="margin:0 0 10px;font-family:Arial,sans-serif;font-size:14px;letter-spacing:0.12em;text-transform:uppercase;color:#8a8a83;">Design Pick of the Week</h2>'
        f'<p style="margin:0;font-size:16px;line-height:1.7;color:#4f4f4b;">{escape(draft.design_pick)}</p>'
        '</div>'
        f'{"".join(sign_off_paragraphs)}'
        '</div>'
    )


def build_broadcast_payload(draft: NewsletterDraft) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "subject": draft.subject_line,
        "preview_text": draft.preview_text,
        "content": render_newsletter_html(draft),
        "description": f"Weekly newsletter draft {draft.week_label}",
        "public": False,
        "published_at": datetime.now(timezone.utc).isoformat(),
        "send_at": None,
    }

    email_address = os.getenv("KIT_EMAIL_ADDRESS", "").strip()
    if email_address:
        payload["email_address"] = email_address

    email_template_id = optional_int_env("KIT_EMAIL_TEMPLATE_ID")
    if email_template_id is not None:
        payload["email_template_id"] = email_template_id

    return payload


def create_kit_broadcast_draft(draft: NewsletterDraft) -> dict[str, Any]:
    api_key = require_kit_api_key()
    payload = build_broadcast_payload(draft)
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        KIT_API_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Kit-Api-Key": api_key,
        },
        method="POST",
    )

    with request.urlopen(req, timeout=30) as response:
        raw = response.read().decode("utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict) or not isinstance(data.get("broadcast"), dict):
        raise RuntimeError("Kit did not return a valid broadcast payload.")
    return data["broadcast"]


def push_weekly_newsletter_to_kit(draft_path: Path | None = None) -> dict[str, Any]:
    load_env()
    resolved_draft_path = draft_path or latest_newsletter_draft()
    draft = parse_weekly_newsletter_draft(resolved_draft_path)
    sidecar_path = sidecar_path_for_draft(resolved_draft_path)
    signature = draft_signature(draft)
    sidecar = load_sidecar(sidecar_path) or {}

    if sidecar.get("draft_signature") == signature and sidecar.get("kit_broadcast_id"):
        print("[kit] draft already synced to Kit; keeping existing broadcast draft")
        return {
            "status": "unchanged",
            "draft_path": resolved_draft_path,
            "sidecar_path": sidecar_path,
            "kit_broadcast_id": sidecar.get("kit_broadcast_id"),
        }

    try:
        print("[kit] creating Kit broadcast draft")
        broadcast = create_kit_broadcast_draft(draft)
    except KitConfigurationError as exc:
        print(f"[kit] not configured: {exc}")
        return {
            "status": "skipped",
            "draft_path": resolved_draft_path,
            "reason": str(exc),
        }
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Kit broadcast draft creation failed with HTTP {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Kit broadcast draft creation failed: {exc.reason}") from exc

    sidecar_payload = {
        "draft_path": str(resolved_draft_path),
        "draft_signature": signature,
        "week": draft.week_label,
        "subject": draft.subject_line,
        "preview_text": draft.preview_text,
        "kit_broadcast_id": broadcast.get("id"),
        "kit_broadcast_url": broadcast.get("url"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    sidecar_path.write_text(json.dumps(sidecar_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[kit] broadcast draft created: {broadcast.get('id')}")
    return {
        "status": "created",
        "draft_path": resolved_draft_path,
        "sidecar_path": sidecar_path,
        "kit_broadcast_id": broadcast.get("id"),
    }


def main() -> int:
    args = parse_args()
    try:
        result = push_weekly_newsletter_to_kit(
            draft_path=Path(args.draft_path) if args.draft_path else None,
        )
        if result.get("sidecar_path"):
            print(result["sidecar_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
