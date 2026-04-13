from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair legacy Pinterest metadata numbering issues such as duplicate pin variant keys."
    )
    parser.add_argument(
        "metadata_path",
        type=str,
        help="Path to the Pinterest metadata JSON file to repair.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Metadata JSON must contain an object: {path}")
    return data


def build_pin_image_path(article_slug: str, index: int) -> str:
    return f"/assets/pins/{article_slug}/pin-{index}.png"


def repair_variant_numbering(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    article_slug = str(payload.get("article_slug", "")).strip()
    variants = payload.get("variants")
    if not isinstance(variants, list):
        raise ValueError("Pinterest metadata must contain a variants list.")

    notes: list[str] = []
    repaired_variants: list[dict[str, Any]] = []
    for index, raw_variant in enumerate(variants, start=1):
        if not isinstance(raw_variant, dict):
            notes.append(f"Skipped non-object variant at position {index}.")
            continue

        variant = dict(raw_variant)
        expected_key = f"pin-{index}"
        expected_image_path = build_pin_image_path(article_slug, index)
        current_key = str(variant.get("variant_key", "")).strip()
        current_image_path = str(variant.get("image_path", "")).strip()

        if current_key != expected_key:
            notes.append(
                f"Variant {index}: variant_key '{current_key or '(missing)'}' -> '{expected_key}'."
            )
            variant["variant_key"] = expected_key

        if current_image_path != expected_image_path:
            notes.append(
                f"Variant {index}: image_path '{current_image_path or '(missing)'}' -> '{expected_image_path}'."
            )
            variant["image_path"] = expected_image_path

        repaired_variants.append(variant)

    payload["variants"] = repaired_variants
    payload["variant_count"] = len(repaired_variants)
    strategy = payload.get("strategy")
    if isinstance(strategy, dict):
        strategy["generated_variant_count"] = len(repaired_variants)
        if notes:
            existing_notes = strategy.get("repair_notes")
            note_list = existing_notes if isinstance(existing_notes, list) else []
            note_list.extend(notes)
            strategy["repair_notes"] = note_list
    return payload, notes


def main() -> int:
    args = parse_args()
    metadata_path = Path(args.metadata_path)
    if not metadata_path.exists():
        print(f"Error: Pinterest metadata not found: {metadata_path}")
        return 1

    try:
        payload = load_json(metadata_path)
        payload, notes = repair_variant_numbering(payload)
        metadata_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        if notes:
            print(f"[pinterest] repaired {metadata_path}")
            for note in notes:
                print(f"[pinterest] {note}")
        else:
            print(f"[pinterest] no numbering repairs needed for {metadata_path}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
