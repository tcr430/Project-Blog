from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from openai import OpenAI


BFL_API_BASE = "https://api.bfl.ai/v1"


def parse_image_size(size: str) -> tuple[int, int]:
    cleaned = str(size or "").strip().lower()
    if "x" not in cleaned:
        raise ValueError(f"Image size must look like WIDTHxHEIGHT, got: {size}")
    width_text, height_text = cleaned.split("x", 1)
    width = int(width_text)
    height = int(height_text)
    if width <= 0 or height <= 0:
        raise ValueError(f"Image size must be positive, got: {size}")
    return width, height


def _read_json_response(response: Any) -> dict[str, Any]:
    raw = response.read().decode("utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise RuntimeError("Provider response must be a JSON object.")
    return payload


def _post_json(url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    request = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return _read_json_response(response)


def _get_json(url: str, headers: dict[str, str]) -> dict[str, Any]:
    request = urllib.request.Request(url=url, headers=headers, method="GET")
    with urllib.request.urlopen(request, timeout=60) as response:
        return _read_json_response(response)


def _download_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.read()


def generate_image_bytes_openai(
    *,
    api_key: str,
    prompt: str,
    model: str,
    size: str,
    quality: str,
) -> bytes:
    client = OpenAI(api_key=api_key)
    response = client.images.generate(
        model=model,
        prompt=prompt,
        size=size,
        quality=quality,
    )

    if not response.data:
        raise RuntimeError("OpenAI image API returned no image data.")

    image_item = response.data[0]
    b64_data = getattr(image_item, "b64_json", None)
    if b64_data:
        return base64.b64decode(b64_data)

    image_url = getattr(image_item, "url", None)
    if image_url:
        return _download_bytes(image_url)

    raise RuntimeError("OpenAI image response did not include image bytes or a URL.")


def generate_image_bytes_flux(
    *,
    api_key: str,
    prompt: str,
    model: str,
    size: str,
    polling_timeout_seconds: float,
    poll_interval_seconds: float,
) -> tuple[bytes, dict[str, Any]]:
    width, height = parse_image_size(size)
    endpoint = model.strip().lstrip("/")
    if not endpoint:
        raise ValueError("FLUX model endpoint cannot be empty.")

    headers = {
        "accept": "application/json",
        "x-key": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": prompt,
        "width": width,
        "height": height,
    }

    started_at = time.monotonic()
    submission = _post_json(f"{BFL_API_BASE}/{endpoint}", payload, headers)
    polling_url = str(submission.get("polling_url") or "").strip()
    request_id = str(submission.get("id") or "").strip()
    if not polling_url:
        raise RuntimeError("FLUX submission did not return a polling_url.")

    poll_attempts = 0
    last_status = "Submitted"
    while True:
        elapsed = time.monotonic() - started_at
        if elapsed > polling_timeout_seconds:
            raise TimeoutError(
                f"FLUX generation timed out after {polling_timeout_seconds:.1f}s "
                f"(last status: {last_status})."
            )

        time.sleep(max(0.1, poll_interval_seconds))
        poll_attempts += 1
        status_payload = _get_json(polling_url, headers)
        last_status = str(status_payload.get("status") or "").strip() or "Unknown"

        if last_status.lower() == "ready":
            result = status_payload.get("result")
            if not isinstance(result, dict):
                raise RuntimeError("FLUX returned Ready without a result payload.")
            sample_url = str(result.get("sample") or "").strip()
            if not sample_url:
                raise RuntimeError("FLUX result payload did not include result.sample.")
            image_bytes = _download_bytes(sample_url)
            diagnostics = {
                "request_id": request_id,
                "polling_url": polling_url,
                "poll_attempts": poll_attempts,
                "provider_status": last_status,
                "elapsed_seconds": round(time.monotonic() - started_at, 2),
            }
            return image_bytes, diagnostics

        if last_status.lower() in {"error", "failed"}:
            message = str(status_payload.get("message") or status_payload.get("error") or "").strip()
            raise RuntimeError(
                f"FLUX generation failed with status {last_status}."
                + (f" {message}" if message else "")
            )


def provider_is_configured(provider: str, *, flux_api_key: str | None, openai_api_key: str | None) -> bool:
    normalized = provider.strip().lower()
    if normalized == "flux":
        return bool(flux_api_key)
    if normalized == "openai":
        return bool(openai_api_key)
    return False


def invoke_image_provider(
    *,
    provider: str,
    prompt: str,
    size: str,
    quality: str,
    flux_api_key: str | None,
    flux_model: str,
    flux_timeout_seconds: float,
    flux_poll_interval_seconds: float,
    openai_api_key: str | None,
    openai_model: str,
) -> tuple[bytes, dict[str, Any]]:
    normalized = provider.strip().lower()
    started_at = time.monotonic()

    if normalized == "flux":
        if not flux_api_key:
            raise RuntimeError("FLUX provider was selected but BFL_API_KEY is not configured.")
        image_bytes, diagnostics = generate_image_bytes_flux(
            api_key=flux_api_key,
            prompt=prompt,
            model=flux_model,
            size=size,
            polling_timeout_seconds=flux_timeout_seconds,
            poll_interval_seconds=flux_poll_interval_seconds,
        )
        diagnostics.update(
            {
                "provider": "flux",
                "model": flux_model,
                "quality": quality,
                "elapsed_seconds_total": round(time.monotonic() - started_at, 2),
            }
        )
        return image_bytes, diagnostics

    if normalized == "openai":
        if not openai_api_key:
            raise RuntimeError("OpenAI provider was selected but OPENAI_API_KEY is not configured.")
        image_bytes = generate_image_bytes_openai(
            api_key=openai_api_key,
            prompt=prompt,
            model=openai_model,
            size=size,
            quality=quality,
        )
        diagnostics = {
            "provider": "openai",
            "model": openai_model,
            "quality": quality,
            "elapsed_seconds_total": round(time.monotonic() - started_at, 2),
        }
        return image_bytes, diagnostics

    raise ValueError(f"Unsupported image provider: {provider}")
