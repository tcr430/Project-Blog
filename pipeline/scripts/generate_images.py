from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI
from PIL import Image

from article_image_providers import invoke_image_provider, provider_is_configured, parse_image_size


IMAGE_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "image_generation_cache"
HERO_MAX_IMAGE_ATTEMPTS = 2
SECTION_MAX_IMAGE_ATTEMPTS = 1
QA_MODEL = "gpt-4.1-mini"
DEFAULT_PRIMARY_PROVIDER = "flux"
DEFAULT_FALLBACK_PROVIDER = "openai"
DEFAULT_FLUX_MODEL = "flux-2-max"
DEFAULT_OPENAI_MODEL = "gpt-image-1"
DEFAULT_FLUX_TIMEOUT_SECONDS = 180.0
DEFAULT_FLUX_POLL_INTERVAL_SECONDS = 1.2
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
        "--provider",
        type=str,
        default=None,
        choices=["flux", "openai"],
        help="Primary image provider override (default: IMAGE_PROVIDER env or flux).",
    )
    parser.add_argument(
        "--fallback-provider",
        type=str,
        default=None,
        choices=["flux", "openai", "none"],
        help="Fallback image provider override (default: IMAGE_FALLBACK_PROVIDER env or openai).",
    )
    parser.add_argument(
        "--disable-fallback",
        action="store_true",
        help="Disable automatic provider fallback.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Primary provider model override (defaults to provider-specific model).",
    )
    parser.add_argument(
        "--fallback-model",
        type=str,
        default=None,
        help="Fallback provider model override (defaults to provider-specific fallback model).",
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
    parser.add_argument(
        "--flux-timeout-seconds",
        type=float,
        default=None,
        help="FLUX polling timeout in seconds (default: FLUX_TIMEOUT_SECONDS env or 180).",
    )
    parser.add_argument(
        "--flux-poll-interval-seconds",
        type=float,
        default=None,
        help="FLUX poll interval in seconds (default: FLUX_POLL_INTERVAL_SECONDS env or 1.2).",
    )
    return parser.parse_args()


def load_environment(project_root: Path) -> None:
    env_path = project_root / ".env"
    load_dotenv(env_path)


def load_openai_api_key(required: bool = True) -> str | None:
    project_root = Path(__file__).resolve().parents[2]
    load_environment(project_root)

    api_key = os.getenv("OPENAI_API_KEY")
    if required and not api_key:
        env_path = project_root / ".env"
        raise RuntimeError(f"OPENAI_API_KEY was not found in {env_path}")
    return api_key


def load_flux_api_key(required: bool = False) -> str | None:
    project_root = Path(__file__).resolve().parents[2]
    load_environment(project_root)
    api_key = os.getenv("BFL_API_KEY") or os.getenv("FLUX_API_KEY")
    if required and not api_key:
        env_path = project_root / ".env"
        raise RuntimeError(f"BFL_API_KEY was not found in {env_path}")
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


def validate_metadata(data: dict[str, Any]) -> tuple[str, str, list[str], dict[str, Any]]:
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

    diagnostics = data.get("image_prompt_diagnostics", {})
    if not isinstance(diagnostics, dict):
        diagnostics = {}

    return slug, hero_prompt, section_prompts, diagnostics


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


def build_prompt_cache_key(
    prompt: str,
    provider: str,
    model: str,
    size: str,
    quality: str,
    run_qa: bool,
) -> str:
    payload = {
        "prompt": prompt,
        "provider": provider,
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


def validate_image_bytes(image_bytes: bytes, *, expected_size: str) -> tuple[bool, str, dict[str, Any]]:
    if not image_bytes:
        return False, "provider returned empty image bytes", {}

    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            image.load()
            actual_width, actual_height = image.size
            actual_format = str(image.format or "").upper()
    except Exception as exc:
        return False, f"image bytes are not readable: {exc}", {}

    expected_width, expected_height = parse_image_size(expected_size)
    actual_ratio = actual_width / actual_height
    expected_ratio = expected_width / expected_height
    if abs(actual_ratio - expected_ratio) > 0.06:
        return False, "image aspect ratio is outside acceptable range", {
            "width": actual_width,
            "height": actual_height,
            "format": actual_format,
        }

    return True, "image passed basic validation", {
        "width": actual_width,
        "height": actual_height,
        "format": actual_format,
    }


def parse_bool_env(name: str, default: bool) -> bool:
    value = str(os.getenv(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def build_provider_settings(
    *,
    provider_override: str | None,
    fallback_provider_override: str | None,
    disable_fallback: bool,
    model_override: str | None,
    fallback_model_override: str | None,
    flux_timeout_override: float | None,
    flux_poll_interval_override: float | None,
) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[2]
    load_environment(project_root)

    primary_provider = str(provider_override or os.getenv("IMAGE_PROVIDER") or DEFAULT_PRIMARY_PROVIDER).strip().lower()
    fallback_provider = str(
        fallback_provider_override or os.getenv("IMAGE_FALLBACK_PROVIDER") or DEFAULT_FALLBACK_PROVIDER
    ).strip().lower()
    if fallback_provider == "none":
        fallback_provider = ""

    allow_fallback = not disable_fallback
    if not disable_fallback:
        allow_fallback = parse_bool_env("IMAGE_ALLOW_FALLBACK", True)

    flux_model = str(os.getenv("FLUX_IMAGE_MODEL") or DEFAULT_FLUX_MODEL).strip()
    openai_model = str(os.getenv("OPENAI_IMAGE_MODEL") or DEFAULT_OPENAI_MODEL).strip()
    if model_override:
        if primary_provider == "flux":
            flux_model = str(model_override).strip()
        elif primary_provider == "openai":
            openai_model = str(model_override).strip()

    settings = {
        "primary_provider": primary_provider,
        "fallback_provider": fallback_provider,
        "allow_fallback": allow_fallback,
        "flux_model": flux_model,
        "openai_model": openai_model,
        "fallback_model": str(
            fallback_model_override or os.getenv("IMAGE_FALLBACK_MODEL") or ""
        ).strip(),
        "flux_timeout_seconds": float(
            flux_timeout_override
            if flux_timeout_override is not None
            else os.getenv("FLUX_TIMEOUT_SECONDS") or DEFAULT_FLUX_TIMEOUT_SECONDS
        ),
        "flux_poll_interval_seconds": float(
            flux_poll_interval_override
            if flux_poll_interval_override is not None
            else os.getenv("FLUX_POLL_INTERVAL_SECONDS") or DEFAULT_FLUX_POLL_INTERVAL_SECONDS
        ),
    }
    return settings


def resolve_provider_model(provider: str, settings: dict[str, Any], *, fallback: bool = False) -> str:
    normalized = provider.strip().lower()
    explicit_fallback_model = str(settings.get("fallback_model") or "").strip()
    if fallback and explicit_fallback_model:
        return explicit_fallback_model
    if normalized == "flux":
        return str(settings.get("flux_model") or DEFAULT_FLUX_MODEL)
    if normalized == "openai":
        return str(settings.get("openai_model") or DEFAULT_OPENAI_MODEL)
    raise ValueError(f"Unsupported image provider: {provider}")


def generate_one_image_with_policy(
    label: str,
    base_prompt: str,
    output_path: Path,
    size: str,
    quality: str,
    max_attempts: int,
    run_qa: bool,
    cache_data: dict[str, Any],
    cache_slot: str,
    provider_settings: dict[str, Any],
    qa_client: OpenAI | None,
    flux_api_key: str | None,
    openai_api_key: str | None,
) -> tuple[Path, dict[str, Any]]:
    primary_provider = str(provider_settings["primary_provider"])
    primary_model = resolve_provider_model(primary_provider, provider_settings, fallback=False)
    cache_key = build_prompt_cache_key(
        prompt=base_prompt,
        provider=primary_provider,
        model=primary_model,
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
            "provider": cache_entry.get("provider", primary_provider) if isinstance(cache_entry, dict) else primary_provider,
            "provider_model": cache_entry.get("provider_model", primary_model) if isinstance(cache_entry, dict) else primary_model,
            "fallback_used": False,
        }

    last_image_bytes: bytes | None = None
    last_validation_reason = ""
    last_provider_name = primary_provider
    last_provider_model = primary_model
    provider_events: list[dict[str, Any]] = []
    generation_calls = 0
    qa_calls = 0

    for attempt in range(1, max_attempts + 1):
        prompt = build_generation_prompt(base_prompt=base_prompt, attempt=attempt)
        print(f"[images] generating {label} (attempt {attempt}/{max_attempts})")

        providers_to_try = [primary_provider]
        fallback_provider = str(provider_settings.get("fallback_provider") or "").strip().lower()
        if (
            provider_settings.get("allow_fallback")
            and fallback_provider
            and fallback_provider != primary_provider
        ):
            providers_to_try.append(fallback_provider)

        image_bytes: bytes | None = None
        provider_diagnostics: dict[str, Any] = {}
        for provider_name in providers_to_try:
            provider_model = resolve_provider_model(
                provider_name,
                provider_settings,
                fallback=provider_name == fallback_provider,
            )
            print(f"[images] provider attempt for {label}: provider={provider_name}, model={provider_model}")
            try:
                generation_calls += 1
                candidate_bytes, candidate_diagnostics = invoke_image_provider(
                    provider=provider_name,
                    prompt=prompt,
                    size=size,
                    quality=quality,
                    flux_api_key=flux_api_key,
                    flux_model=str(provider_settings["flux_model"]),
                    flux_timeout_seconds=float(provider_settings["flux_timeout_seconds"]),
                    flux_poll_interval_seconds=float(provider_settings["flux_poll_interval_seconds"]),
                    openai_api_key=openai_api_key,
                    openai_model=str(provider_settings["openai_model"]),
                )
                validation_ok, validation_reason, image_info = validate_image_bytes(
                    candidate_bytes,
                    expected_size=size,
                )
                provider_event = {
                    "provider": provider_name,
                    "model": provider_model,
                    "attempt": attempt,
                    "validation_passed": validation_ok,
                    "validation_reason": validation_reason,
                }
                provider_event.update(candidate_diagnostics)
                provider_event.update(image_info)
                provider_events.append(provider_event)

                if not validation_ok:
                    print(f"[images] provider rejected for {label}: {provider_name} - {validation_reason}")
                    last_validation_reason = validation_reason
                    last_provider_name = provider_name
                    last_provider_model = provider_model
                    continue

                image_bytes = candidate_bytes
                provider_diagnostics = provider_event
                last_validation_reason = validation_reason
                last_provider_name = provider_name
                last_provider_model = provider_model
                if provider_name != primary_provider:
                    print(
                        f"[images] fallback succeeded for {label}: "
                        f"{primary_provider} -> {provider_name} ({validation_reason})"
                    )
                break
            except Exception as exc:
                failure_reason = str(exc).strip() or "unknown provider error"
                print(f"[images] provider failed for {label}: {provider_name} - {failure_reason}")
                provider_events.append(
                    {
                        "provider": provider_name,
                        "model": provider_model,
                        "attempt": attempt,
                        "error": failure_reason,
                    }
                )
                last_validation_reason = failure_reason
                last_provider_name = provider_name
                last_provider_model = provider_model
                continue

        if image_bytes is None:
            if attempt < max_attempts:
                print(f"[images] retrying {label} after provider failures")
                continue
            break

        last_image_bytes = image_bytes

        if not run_qa:
            saved_path = save_image(image_bytes=image_bytes, output_path=output_path)
            cache_data[cache_slot] = {
                "cache_key": cache_key,
                "output_path": str(saved_path),
                "provider": last_provider_name,
                "provider_model": last_provider_model,
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
                "provider": last_provider_name,
                "provider_model": last_provider_model,
                "fallback_used": last_provider_name != primary_provider,
                "provider_events": provider_events,
                "validation_reason": last_validation_reason,
            }

        if qa_client is None:
            qa_passed = True
            qa_reason = "Skipped model QA because OPENAI_API_KEY is unavailable."
            print(f"[images] qa skipped for {label}: {qa_reason}")
        else:
            try:
                qa_calls += 1
                qa_passed, qa_reason = run_image_qa(
                    client=qa_client,
                    image_bytes=image_bytes,
                    intended_prompt=prompt,
                )
                qa_status = "passed" if qa_passed else "failed"
                print(f"QA result: {qa_status} - {qa_reason}")
            except Exception as exc:
                qa_passed = False
                qa_reason = f"qa error: {exc}"
                print(f"QA result: failed - {qa_reason}")

        if qa_passed:
            saved_path = save_image(image_bytes=image_bytes, output_path=output_path)
            cache_data[cache_slot] = {
                "cache_key": cache_key,
                "output_path": str(saved_path),
                "provider": last_provider_name,
                "provider_model": last_provider_model,
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
                "provider": last_provider_name,
                "provider_model": last_provider_model,
                "fallback_used": last_provider_name != primary_provider,
                "provider_events": provider_events,
                "validation_reason": last_validation_reason,
                "qa_reason": qa_reason,
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
        "provider": last_provider_name,
        "provider_model": last_provider_model,
    }
    return saved_path, {
        "label": label,
        "cache_hit": False,
        "generated": True,
        "generation_calls": generation_calls,
        "qa_calls": qa_calls,
        "quality": quality,
        "qa_enabled": run_qa,
        "provider": last_provider_name,
        "provider_model": last_provider_model,
        "fallback_used": last_provider_name != primary_provider,
        "provider_events": provider_events,
        "validation_reason": last_validation_reason,
    }


def generate_and_save_images_with_report(
    metadata_path: Path,
    model: str | None,
    size: str,
    quality: str,
    provider: str | None = None,
    fallback_provider: str | None = None,
    disable_fallback: bool = False,
    fallback_model: str | None = None,
    flux_timeout_seconds: float | None = None,
    flux_poll_interval_seconds: float | None = None,
) -> tuple[list[Path], dict[str, Any]]:
    metadata = load_metadata(metadata_path)
    slug, hero_prompt, section_prompts, diagnostics = validate_metadata(metadata)
    for warning in diagnostics.get("sameness_warnings", []) if isinstance(diagnostics, dict) else []:
        print(f"[images][warning] {warning}")
    provider_settings = build_provider_settings(
        provider_override=provider,
        fallback_provider_override=fallback_provider,
        disable_fallback=disable_fallback,
        model_override=model,
        fallback_model_override=fallback_model,
        flux_timeout_override=flux_timeout_seconds,
        flux_poll_interval_override=flux_poll_interval_seconds,
    )
    openai_api_key = load_openai_api_key(required=False)
    flux_api_key = load_flux_api_key(required=False)
    qa_client = OpenAI(api_key=openai_api_key) if openai_api_key else None

    if not provider_is_configured(
        str(provider_settings["primary_provider"]),
        flux_api_key=flux_api_key,
        openai_api_key=openai_api_key,
    ):
        raise RuntimeError(
            f"Primary image provider '{provider_settings['primary_provider']}' is not configured."
        )

    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "assets" / "img" / slug
    cache_path = build_image_cache_path(slug)
    cache_data = load_cache(cache_path)

    saved_paths: list[Path] = []
    image_details: list[dict[str, Any]] = []
    print(
        "[images] hero policy: "
        f"provider={provider_settings['primary_provider']}, "
        f"fallback={provider_settings['fallback_provider'] or 'none'}, "
        f"quality={quality}, qa={'on' if qa_client else 'basic-only'}"
    )

    hero_path, hero_detail = generate_one_image_with_policy(
        label="hero",
        base_prompt=hero_prompt,
        output_path=output_dir / "hero.png",
        size=size,
        quality=quality,
        max_attempts=HERO_MAX_IMAGE_ATTEMPTS,
        run_qa=True,
        cache_data=cache_data,
        cache_slot="hero",
        provider_settings=provider_settings,
        qa_client=qa_client,
        flux_api_key=flux_api_key,
        openai_api_key=openai_api_key,
    )
    saved_paths.append(hero_path)
    image_details.append(hero_detail)

    section_quality = resolve_section_quality(quality)
    print(
        "[images] section policy: "
        f"provider={provider_settings['primary_provider']}, "
        f"fallback={provider_settings['fallback_provider'] or 'none'}, "
        f"quality={section_quality}, qa=off"
    )

    for index, prompt in enumerate(section_prompts, start=1):
        section_path, section_detail = generate_one_image_with_policy(
            label=f"section-{index}",
            base_prompt=prompt,
            output_path=output_dir / f"section-{index}.png",
            size=size,
            quality=section_quality,
            max_attempts=SECTION_MAX_IMAGE_ATTEMPTS,
            run_qa=False,
            cache_data=cache_data,
            cache_slot=f"section-{index}",
            provider_settings=provider_settings,
            qa_client=qa_client,
            flux_api_key=flux_api_key,
            openai_api_key=openai_api_key,
        )
        saved_paths.append(section_path)
        image_details.append(section_detail)

    save_cache(cache_path, cache_data)

    report = {
        "cache_path": str(cache_path),
        "provider": provider_settings["primary_provider"],
        "provider_model": resolve_provider_model(str(provider_settings["primary_provider"]), provider_settings),
        "model": resolve_provider_model(str(provider_settings["primary_provider"]), provider_settings),
        "fallback_provider": provider_settings["fallback_provider"],
        "fallback_model": (
            resolve_provider_model(str(provider_settings["fallback_provider"]), provider_settings, fallback=True)
            if provider_settings["fallback_provider"]
            else ""
        ),
        "size": size,
        "hero_quality": quality,
        "section_quality": section_quality,
        "allow_fallback": bool(provider_settings["allow_fallback"]),
        "fallback_events": sum(1 for item in image_details if item.get("fallback_used")),
        "generated_images": sum(1 for item in image_details if item["generated"]),
        "cache_hits": sum(1 for item in image_details if item["cache_hit"]),
        "generation_calls": sum(item["generation_calls"] for item in image_details),
        "qa_calls": sum(item["qa_calls"] for item in image_details),
        "details": image_details,
    }

    return saved_paths, report


def generate_and_save_images(
    metadata_path: Path,
    model: str | None,
    size: str,
    quality: str,
    provider: str | None = None,
    fallback_provider: str | None = None,
    disable_fallback: bool = False,
    fallback_model: str | None = None,
    flux_timeout_seconds: float | None = None,
    flux_poll_interval_seconds: float | None = None,
) -> list[Path]:
    saved_paths, _ = generate_and_save_images_with_report(
        metadata_path=metadata_path,
        model=model,
        size=size,
        quality=quality,
        provider=provider,
        fallback_provider=fallback_provider,
        disable_fallback=disable_fallback,
        fallback_model=fallback_model,
        flux_timeout_seconds=flux_timeout_seconds,
        flux_poll_interval_seconds=flux_poll_interval_seconds,
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
            provider=args.provider,
            fallback_provider=args.fallback_provider,
            disable_fallback=args.disable_fallback,
            fallback_model=args.fallback_model,
            flux_timeout_seconds=args.flux_timeout_seconds,
            flux_poll_interval_seconds=args.flux_poll_interval_seconds,
        )

        for path in saved_paths:
            print(path)

        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

