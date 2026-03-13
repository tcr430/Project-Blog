from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import urllib.request
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI


IMAGE_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "image_generation_cache"
HERO_MAX_IMAGE_ATTEMPTS = 2
SECTION_MAX_IMAGE_ATTEMPTS = 1
QA_MODEL = "gpt-4.1-mini"
RETRY_SUFFIX = (
    "Strict requirements: editorial interior photography, natural daylight, "
    "no text, no logos, no people, clean composition, realistic materials, "
    "balanced styling, and no heavy clutter."
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate hero and section images from article metadata prompts."
    )
    parser.add_argument(
        "metadata_json_path",
        type=str,
        help="Path to metadata JSON containing slug, hero_image_prompt, section_image_prompts.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-image-1",
        help="OpenAI image model to use (default: gpt-image-1).",
    )
    parser.add_argument(
        "--size",
        type=str,
        default="1536x1024",
        help="Image size (default: 1536x1024).",
    )
    parser.add_argument(
        "--quality",
        type=str,
        default="high",
        help="Image quality hint (default: high).",
    )
    return parser.parse_args()


def load_openai_api_key() -> str:
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    load_dotenv(env_path)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(f"OPENAI_API_KEY was not found in {env_path}")
    return api_key


def load_metadata(metadata_path: Path) -> dict[str, Any]:
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata JSON file not found: {metadata_path}")

    raw = metadata_path.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Metadata file is not valid JSON: {metadata_path}") from exc

    if not isinstance(data, dict):
        raise ValueError("Metadata JSON must be an object.")

    return data


def validate_metadata(data: dict[str, Any]) -> tuple[str, str, list[str]]:
    required = ["slug", "hero_image_prompt", "section_image_prompts"]
    missing = [field for field in required if field not in data]
    if missing:
        raise ValueError(f"Metadata is missing required fields: {', '.join(missing)}")

    slug = str(data["slug"]).strip()
    hero_prompt = str(data["hero_image_prompt"]).strip()

    section_raw = data["section_image_prompts"]
    if not isinstance(section_raw, list):
        raise ValueError("section_image_prompts must be a list.")

    section_prompts = [str(item).strip() for item in section_raw if str(item).strip()]

    if not slug:
        raise ValueError("slug cannot be empty.")
    if not hero_prompt:
        raise ValueError("hero_image_prompt cannot be empty.")
    if not section_prompts:
        raise ValueError("section_image_prompts cannot be empty.")

    return slug, hero_prompt, section_prompts


def build_image_cache_path(slug: str) -> Path:
    return IMAGE_CACHE_DIR / f"{slug}.json"


def load_cache(cache_path: Path) -> dict[str, Any]:
    if not cache_path.exists():
        return {}
    try:
        raw = cache_path.read_text(encoding="utf-8-sig").strip()
        if not raw:
            return {}
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def save_cache(cache_path: Path, payload: dict[str, Any]) -> Path:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return cache_path


def build_prompt_cache_key(prompt: str, model: str, size: str, quality: str, run_qa: bool) -> str:
    payload = {
        "prompt": prompt,
        "model": model,
        "size": size,
        "quality": quality,
        "run_qa": run_qa,
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def resolve_section_quality(requested_quality: str) -> str:
    normalized = requested_quality.strip().lower()
    if normalized == "high":
        return "medium"
    return requested_quality


def build_generation_prompt(base_prompt: str, attempt: int) -> str:
    if attempt <= 1:
        return base_prompt
    return f"{base_prompt}\n\n{RETRY_SUFFIX}"


def strip_code_fences(text: str) -> str:
    match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL)
    return match.group(1).strip() if match else text.strip()


def generate_image_bytes(
    client: OpenAI,
    prompt: str,
    model: str,
    size: str,
    quality: str,
) -> bytes:
    response = client.images.generate(
        model=model,
        prompt=prompt,
        size=size,
        quality=quality,
    )

    if not response.data:
        raise RuntimeError("Image API returned no image data.")

    image_item = response.data[0]

    b64_data = getattr(image_item, "b64_json", None)
    if b64_data:
        return base64.b64decode(b64_data)

    image_url = getattr(image_item, "url", None)
    if image_url:
        with urllib.request.urlopen(image_url) as response_stream:
            return response_stream.read()

    raise RuntimeError("Image API response did not include b64_json or url.")


def run_image_qa(client: OpenAI, image_bytes: bytes, intended_prompt: str) -> tuple[bool, str]:
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:image/png;base64,{image_b64}"

    qa_instruction = (
        "Evaluate this image for a decor blog. Return JSON only with fields: "
        "pass (boolean), relevance (high|medium|low), usable (boolean), has_text (boolean), "
        "has_logo (boolean), has_people (boolean), heavy_clutter (boolean), reason (string). "
        "Fail if image is not relevant to the intended prompt, not usable for decor blog, "
        "or contains obvious text/logo/people/heavy clutter."
    )

    response = client.responses.create(
        model=QA_MODEL,
        input=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": f"Intended prompt:\n{intended_prompt}\n\n{qa_instruction}",
                    },
                    {
                        "type": "input_image",
                        "image_url": data_url,
                    },
                ],
            }
        ],
    )

    raw_text = (response.output_text or "").strip()
    if not raw_text:
        raise RuntimeError("QA model returned an empty response.")

    json_text = strip_code_fences(raw_text)
    try:
        qa_payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("QA model did not return valid JSON.") from exc

    if not isinstance(qa_payload, dict):
        raise RuntimeError("QA response must be a JSON object.")

    relevance = str(qa_payload.get("relevance", "")).strip().lower()
    usable = bool(qa_payload.get("usable", False))
    has_text = bool(qa_payload.get("has_text", False))
    has_logo = bool(qa_payload.get("has_logo", False))
    has_people = bool(qa_payload.get("has_people", False))
    heavy_clutter = bool(qa_payload.get("heavy_clutter", False))
    reason = str(qa_payload.get("reason", "")).strip() or "No reason provided."

    rule_pass = (
        relevance in {"high", "medium"}
        and usable
        and not has_text
        and not has_logo
        and not has_people
        and not heavy_clutter
    )

    return rule_pass, reason


def save_image(image_bytes: bytes, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(image_bytes)
    return output_path


def generate_one_image_with_policy(
    client: OpenAI,
    label: str,
    base_prompt: str,
    output_path: Path,
    model: str,
    size: str,
    quality: str,
    max_attempts: int,
    run_qa: bool,
    cache_data: dict[str, Any],
    cache_slot: str,
) -> tuple[Path, dict[str, Any]]:
    cache_key = build_prompt_cache_key(
        prompt=base_prompt,
        model=model,
        size=size,
        quality=quality,
        run_qa=run_qa,
    )
    cache_entry = cache_data.get(cache_slot)
    if (
        isinstance(cache_entry, dict)
        and cache_entry.get("cache_key") == cache_key
        and output_path.exists()
    ):
        print(f"[images] cache hit: {label}")
        return output_path, {
            "label": label,
            "cache_hit": True,
            "generated": False,
            "generation_calls": 0,
            "qa_calls": 0,
            "quality": quality,
            "qa_enabled": run_qa,
        }

    last_image_bytes: bytes | None = None
    generation_calls = 0
    qa_calls = 0

    for attempt in range(1, max_attempts + 1):
        prompt = build_generation_prompt(base_prompt=base_prompt, attempt=attempt)
        print(f"Generating image: {label} (attempt {attempt}/{max_attempts})")

        try:
            generation_calls += 1
            image_bytes = generate_image_bytes(
                client=client,
                prompt=prompt,
                model=model,
                size=size,
                quality=quality,
            )
        except Exception as exc:
            print(f"QA result: failed - generation error: {exc}")
            if attempt < max_attempts:
                print(f"Retrying image: {label}")
            continue

        last_image_bytes = image_bytes

        if not run_qa:
            saved_path = save_image(image_bytes=image_bytes, output_path=output_path)
            cache_data[cache_slot] = {
                "cache_key": cache_key,
                "output_path": str(saved_path),
            }
            print(f"Accepted image: {saved_path}")
            return saved_path, {
                "label": label,
                "cache_hit": False,
                "generated": True,
                "generation_calls": generation_calls,
                "qa_calls": 0,
                "quality": quality,
                "qa_enabled": False,
            }

        try:
            qa_calls += 1
            qa_passed, qa_reason = run_image_qa(
                client=client,
                image_bytes=image_bytes,
                intended_prompt=prompt,
            )
            qa_status = "passed" if qa_passed else "failed"
            print(f"QA result: {qa_status} - {qa_reason}")
        except Exception as exc:
            qa_passed = False
            print(f"QA result: failed - qa error: {exc}")

        if qa_passed:
            saved_path = save_image(image_bytes=image_bytes, output_path=output_path)
            cache_data[cache_slot] = {
                "cache_key": cache_key,
                "output_path": str(saved_path),
            }
            print(f"Accepted image: {saved_path}")
            return saved_path, {
                "label": label,
                "cache_hit": False,
                "generated": True,
                "generation_calls": generation_calls,
                "qa_calls": qa_calls,
                "quality": quality,
                "qa_enabled": True,
            }

        if attempt < max_attempts:
            print(f"Retrying image: {label}")

    if last_image_bytes is None:
        raise RuntimeError(f"Image generation failed for {label}; no image produced.")

    saved_path = save_image(image_bytes=last_image_bytes, output_path=output_path)
    print(
        f"Warning: {label} failed QA after {max_attempts} attempts. "
        f"Keeping final image: {saved_path}"
    )
    cache_data[cache_slot] = {
        "cache_key": cache_key,
        "output_path": str(saved_path),
    }
    return saved_path, {
        "label": label,
        "cache_hit": False,
        "generated": True,
        "generation_calls": generation_calls,
        "qa_calls": qa_calls,
        "quality": quality,
        "qa_enabled": run_qa,
    }


def generate_and_save_images_with_report(
    metadata_path: Path,
    model: str,
    size: str,
    quality: str,
) -> tuple[list[Path], dict[str, Any]]:
    metadata = load_metadata(metadata_path)
    slug, hero_prompt, section_prompts = validate_metadata(metadata)

    api_key = load_openai_api_key()
    client = OpenAI(api_key=api_key)

    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "assets" / "img" / slug
    cache_path = build_image_cache_path(slug)
    cache_data = load_cache(cache_path)

    saved_paths: list[Path] = []
    image_details: list[dict[str, Any]] = []
    print(f"[images] hero policy: model={model}, quality={quality}, qa=on")

    hero_path, hero_detail = generate_one_image_with_policy(
        client=client,
        label="hero",
        base_prompt=hero_prompt,
        output_path=output_dir / "hero.png",
        model=model,
        size=size,
        quality=quality,
        max_attempts=HERO_MAX_IMAGE_ATTEMPTS,
        run_qa=True,
        cache_data=cache_data,
        cache_slot="hero",
    )
    saved_paths.append(hero_path)
    image_details.append(hero_detail)

    section_quality = resolve_section_quality(quality)
    print(f"[images] section policy: model={model}, quality={section_quality}, qa=off")

    for index, prompt in enumerate(section_prompts, start=1):
        section_path, section_detail = generate_one_image_with_policy(
            client=client,
            label=f"section-{index}",
            base_prompt=prompt,
            output_path=output_dir / f"section-{index}.png",
            model=model,
            size=size,
            quality=section_quality,
            max_attempts=SECTION_MAX_IMAGE_ATTEMPTS,
            run_qa=False,
            cache_data=cache_data,
            cache_slot=f"section-{index}",
        )
        saved_paths.append(section_path)
        image_details.append(section_detail)

    save_cache(cache_path, cache_data)

    report = {
        "cache_path": str(cache_path),
        "model": model,
        "size": size,
        "hero_quality": quality,
        "section_quality": section_quality,
        "generated_images": sum(1 for item in image_details if item["generated"]),
        "cache_hits": sum(1 for item in image_details if item["cache_hit"]),
        "generation_calls": sum(item["generation_calls"] for item in image_details),
        "qa_calls": sum(item["qa_calls"] for item in image_details),
        "details": image_details,
    }

    return saved_paths, report


def generate_and_save_images(
    metadata_path: Path,
    model: str,
    size: str,
    quality: str,
) -> list[Path]:
    saved_paths, _ = generate_and_save_images_with_report(
        metadata_path=metadata_path,
        model=model,
        size=size,
        quality=quality,
    )
    return saved_paths


def main() -> int:
    args = parse_args()

    try:
        metadata_path = Path(args.metadata_json_path)
        saved_paths = generate_and_save_images(
            metadata_path=metadata_path,
            model=args.model,
            size=args.size,
            quality=args.quality,
        )

        for path in saved_paths:
            print(path)

        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

