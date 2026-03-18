from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from content_architecture import resolve_intent_id


DEFAULT_HISTORY_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_history.json"
DEFAULT_SUMMARY_PATH = Path(__file__).resolve().parents[1] / "data" / "pinterest_performance_summary.json"
DEFAULT_METADATA_DIR = Path(__file__).resolve().parents[2] / "_data" / "article_metadata"
DEFAULT_PINTEREST_METADATA_DIR = Path(__file__).resolve().parents[2] / "_data" / "pinterest"
DEFAULT_OUTPUT_PATH = Path(__file__).resolve().parents[1] / "reports" / "pinterest_intelligence_report.json"
DEFAULT_NORMALIZED_OUTPUT_PATH = Path(__file__).resolve().parents[2] / "analytics" / "pin_performance.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate architecture-aware Pinterest intelligence from pin history.")
    parser.add_argument("--history-path", type=str, default=str(DEFAULT_HISTORY_PATH))
    parser.add_argument("--summary-path", type=str, default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--metadata-dir", type=str, default=str(DEFAULT_METADATA_DIR))
    parser.add_argument("--pinterest-metadata-dir", type=str, default=str(DEFAULT_PINTEREST_METADATA_DIR))
    parser.add_argument("--output-path", type=str, default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--normalized-output-path", type=str, default=str(DEFAULT_NORMALIZED_OUTPUT_PATH))
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").replace("-", " ").split())


def normalize_id(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


def int_value(value: Any) -> int:
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value.strip()))
        except ValueError:
            return 0
    return 0


def round_average(total: float, count: int) -> float:
    if count <= 0:
        return 0.0
    return round(total / count, 2)


def build_article_metadata_lookup(metadata_dir: Path) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    if not metadata_dir.exists():
        return lookup
    for path in metadata_dir.glob("*.json"):
        payload = load_json(path, {})
        if not isinstance(payload, dict):
            continue
        slug = str(payload.get("slug") or path.stem).strip()
        if slug:
            lookup[slug] = payload
    return lookup


def build_pin_variant_lookup(metadata_dir: Path) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    if not metadata_dir.exists():
        return lookup
    for path in metadata_dir.glob("*.json"):
        payload = load_json(path, {})
        if not isinstance(payload, dict):
            continue
        article_slug = str(payload.get("article_slug") or "").strip()
        if not article_slug:
            continue
        for variant in payload.get("variants", []):
            if not isinstance(variant, dict):
                continue
            variant_type = str(variant.get("variant_type") or "").strip()
            if article_slug and variant_type:
                lookup[(article_slug, variant_type)] = variant
    return lookup


def build_article_summary_lookup(summary_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = summary_data.get("performance_by_article", []) if isinstance(summary_data, dict) else []
    return {
        str(row.get("article_slug") or "").strip(): row
        for row in rows
        if isinstance(row, dict) and str(row.get("article_slug") or "").strip()
    }


def build_visual_style_key(*, style_name: str, family_id: str, shot_library_id: str, intent_id: str) -> str:
    return "|".join(
        [
            normalize_id(style_name) or "unstyled",
            normalize_id(family_id) or "family_unspecified",
            normalize_id(shot_library_id) or "shot_unspecified",
            normalize_id(intent_id) or "intent_unspecified",
        ]
    )


def compute_pin_score(row: dict[str, Any]) -> float:
    return round(
        int_value(row.get("outbound_clicks")) * 5
        + int_value(row.get("saves")) * 3
        + int_value(row.get("pin_clicks")) * 2
        + int_value(row.get("closeups"))
        + (int_value(row.get("impressions")) / 100.0),
        2,
    )


def classify_signal(*, analytics_ready_count: int, average_score: float, average_saves: float, average_clicks: float) -> tuple[str, int]:
    if analytics_ready_count <= 0:
        return "no_data", 0
    if average_clicks >= 1.0 or average_saves >= 3.0 or average_score >= 10.0:
        return "hot", 12
    if average_score >= 3.0 or average_saves >= 1.0:
        return "warm", 6
    if average_score <= 0.5:
        return "cold", -4
    return "steady", 2


def summarize_group(
    *,
    label_fields: dict[str, Any],
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    published_count = sum(1 for entry in entries if bool(entry.get("published")))
    analytics_ready_count = sum(1 for entry in entries if bool(entry.get("analytics_ready")))
    total_impressions = sum(int_value(entry.get("impressions")) for entry in entries)
    total_saves = sum(int_value(entry.get("saves")) for entry in entries)
    total_outbound_clicks = sum(int_value(entry.get("outbound_clicks")) for entry in entries)
    total_pin_clicks = sum(int_value(entry.get("pin_clicks")) for entry in entries)
    total_closeups = sum(int_value(entry.get("closeups")) for entry in entries)
    total_score = sum(float(entry.get("performance_score") or 0.0) for entry in entries)
    average_score = round_average(total_score, analytics_ready_count)
    average_saves = round_average(total_saves, analytics_ready_count)
    average_outbound_clicks = round_average(total_outbound_clicks, analytics_ready_count)
    average_impressions = round_average(total_impressions, analytics_ready_count)
    signal_label, signal_boost = classify_signal(
        analytics_ready_count=analytics_ready_count,
        average_score=average_score,
        average_saves=average_saves,
        average_clicks=average_outbound_clicks,
    )
    sample_articles = []
    seen_articles: set[str] = set()
    for entry in sorted(entries, key=lambda item: (-float(item.get("performance_score") or 0.0), str(item.get("article_slug") or ""))):
        article_slug = str(entry.get("article_slug") or "").strip()
        if not article_slug or article_slug in seen_articles:
            continue
        seen_articles.add(article_slug)
        sample_articles.append(article_slug)
        if len(sample_articles) >= 5:
            break

    return {
        **label_fields,
        "pin_count": len(entries),
        "published_pin_count": published_count,
        "analytics_ready_count": analytics_ready_count,
        "average_score": average_score,
        "average_impressions": average_impressions,
        "average_saves": average_saves,
        "average_outbound_clicks": average_outbound_clicks,
        "average_pin_clicks": round_average(total_pin_clicks, analytics_ready_count),
        "average_closeups": round_average(total_closeups, analytics_ready_count),
        "signal_label": signal_label,
        "signal_boost": signal_boost,
        "top_article_slugs": sample_articles,
    }


def build_normalized_pin_rows(
    *,
    history_rows: list[dict[str, Any]],
    article_lookup: dict[str, dict[str, Any]],
    pin_variant_lookup: dict[tuple[str, str], dict[str, Any]],
    summary_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in history_rows:
        if not isinstance(row, dict):
            continue
        article_slug = str(row.get("article_slug") or "").strip()
        variant_type = str(row.get("variant_type") or "").strip()
        article_metadata = article_lookup.get(article_slug, {})
        variant_metadata = pin_variant_lookup.get((article_slug, variant_type), {})
        article_summary = summary_lookup.get(article_slug, {})
        visual_direction = article_metadata.get("visual_direction", {}) if isinstance(article_metadata.get("visual_direction"), dict) else {}
        family_id = str(visual_direction.get("family_id") or "").strip()
        shot_library_id = str(visual_direction.get("shot_library_id") or "").strip()
        angle_id = str(row.get("angle_id") or article_metadata.get("angle_id") or "").strip()
        intent_id = (
            str(row.get("intent_id") or "").strip()
            or str(article_metadata.get("intent_id") or "").strip()
            or str(visual_direction.get("intent_id") or "").strip()
            or resolve_intent_id(angle_id=angle_id)
        )
        normalized = {
            "article_slug": article_slug,
            "article_title": str(article_metadata.get("title") or "").strip(),
            "cluster_id": str(row.get("cluster_id") or article_metadata.get("cluster_id") or "").strip(),
            "cluster_name": str(article_metadata.get("canonical_cluster_name") or article_metadata.get("cluster_name") or article_metadata.get("topical_cluster") or "").strip(),
            "subtopic_id": str(row.get("subtopic_id") or article_metadata.get("subtopic_id") or "").strip(),
            "subtopic_name": str(article_metadata.get("subtopic_name") or "").strip(),
            "angle_id": angle_id,
            "intent_id": intent_id,
            "variant_type": variant_type,
            "style_name": str(row.get("style_name") or variant_metadata.get("style_name") or "").strip(),
            "visual_direction": visual_direction,
            "visual_family_id": family_id,
            "shot_library_id": shot_library_id,
            "visual_style_key": build_visual_style_key(
                style_name=str(row.get("style_name") or variant_metadata.get("style_name") or ""),
                family_id=family_id,
                shot_library_id=shot_library_id,
                intent_id=intent_id,
            ),
            "board_key": str((row.get("board") or {}).get("key") or "").strip(),
            "board_name": str((row.get("board") or {}).get("name") or "").strip(),
            "status": str(row.get("status") or "").strip(),
            "published": str(row.get("status") or "").strip().lower() == "published",
            "analytics_ready": any(int_value(row.get(field)) > 0 for field in ["impressions", "saves", "outbound_clicks", "pin_clicks", "closeups"]),
            "impressions": int_value(row.get("impressions")),
            "saves": int_value(row.get("saves")),
            "outbound_clicks": int_value(row.get("outbound_clicks")),
            "pin_clicks": int_value(row.get("pin_clicks")),
            "closeups": int_value(row.get("closeups")),
            "created_at": str(row.get("created_at") or "").strip(),
            "published_at": str(row.get("published_at") or "").strip(),
            "article_performance_classification": str(article_summary.get("classification") or "").strip(),
            "article_performance_score": float(article_summary.get("score") or 0.0),
        }
        normalized["performance_score"] = compute_pin_score(normalized)
        normalized_rows.append(normalized)

    normalized_rows.sort(
        key=lambda item: (-float(item.get("performance_score") or 0.0), str(item.get("article_slug") or ""), str(item.get("variant_type") or ""))
    )
    return normalized_rows


def group_rows(rows: list[dict[str, Any]], key_builder) -> dict[Any, list[dict[str, Any]]]:
    grouped: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        key = key_builder(row)
        if not key:
            continue
        grouped.setdefault(key, []).append(row)
    return grouped


def build_pinterest_intelligence_report(
    *,
    history_data: list[dict[str, Any]],
    summary_data: dict[str, Any],
    metadata_dir: Path,
    pinterest_metadata_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    article_lookup = build_article_metadata_lookup(metadata_dir)
    pin_variant_lookup = build_pin_variant_lookup(pinterest_metadata_dir)
    summary_lookup = build_article_summary_lookup(summary_data)
    normalized_rows = build_normalized_pin_rows(
        history_rows=history_data,
        article_lookup=article_lookup,
        pin_variant_lookup=pin_variant_lookup,
        summary_lookup=summary_lookup,
    )

    cluster_rows = [
        summarize_group(
            label_fields={
                "cluster_id": cluster_id,
                "cluster_name": str(next((entry.get("cluster_name") for entry in entries if entry.get("cluster_name")), cluster_id)),
            },
            entries=entries,
        )
        for cluster_id, entries in group_rows(normalized_rows, lambda item: str(item.get("cluster_id") or "").strip()).items()
    ]
    subtopic_rows = [
        summarize_group(
            label_fields={
                "cluster_id": cluster_id,
                "subtopic_id": subtopic_id,
                "subtopic_name": str(next((entry.get("subtopic_name") for entry in entries if entry.get("subtopic_name")), subtopic_id)),
            },
            entries=entries,
        )
        for (cluster_id, subtopic_id), entries in group_rows(
            normalized_rows,
            lambda item: (
                str(item.get("cluster_id") or "").strip(),
                str(item.get("subtopic_id") or "").strip(),
            ) if str(item.get("cluster_id") or "").strip() and str(item.get("subtopic_id") or "").strip() else None,
        ).items()
    ]

    angle_rows = [
        summarize_group(label_fields={"angle_id": angle_id}, entries=entries)
        for angle_id, entries in group_rows(normalized_rows, lambda item: str(item.get("angle_id") or "").strip()).items()
    ]

    visual_rows = [
        summarize_group(
            label_fields={
                "visual_style_key": visual_key,
                "style_name": str(entries[0].get("style_name") or ""),
                "visual_family_id": str(entries[0].get("visual_family_id") or ""),
                "shot_library_id": str(entries[0].get("shot_library_id") or ""),
                "intent_id": str(entries[0].get("intent_id") or ""),
            },
            entries=entries,
        )
        for visual_key, entries in group_rows(normalized_rows, lambda item: str(item.get("visual_style_key") or "").strip()).items()
    ]

    cluster_rows.sort(key=lambda item: (-float(item["average_score"]), -int(item["analytics_ready_count"]), str(item["cluster_id"])))
    subtopic_rows.sort(key=lambda item: (-float(item["average_score"]), -int(item["analytics_ready_count"]), str(item["subtopic_id"])))
    angle_rows.sort(key=lambda item: (-float(item["average_score"]), -int(item["analytics_ready_count"]), str(item["angle_id"])))
    visual_rows.sort(key=lambda item: (-float(item["average_score"]), -int(item["analytics_ready_count"]), str(item["visual_style_key"])))

    normalized_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pin_count": len(normalized_rows),
        "pins": normalized_rows,
    }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overview": {
            "pin_count": len(normalized_rows),
            "published_pin_count": sum(1 for row in normalized_rows if row["published"]),
            "analytics_ready_pin_count": sum(1 for row in normalized_rows if row["analytics_ready"]),
            "tracked_article_count": len({str(row.get("article_slug") or "").strip() for row in normalized_rows if str(row.get("article_slug") or "").strip()}),
        },
        "best_performing_clusters": cluster_rows,
        "best_performing_subtopics": subtopic_rows,
        "best_performing_angles": angle_rows,
        "best_performing_visual_styles": visual_rows,
        "top_rankings": {
            "clusters": cluster_rows[:5],
            "subtopics": subtopic_rows[:5],
            "angles": angle_rows[:5],
            "visual_styles": visual_rows[:5],
        },
    }
    return normalized_payload, report


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_pinterest_intelligence_outputs(
    *,
    history_path: Path,
    summary_path: Path,
    metadata_dir: Path,
    pinterest_metadata_dir: Path,
    output_path: Path,
    normalized_output_path: Path,
) -> dict[str, Any]:
    history_data = load_json(history_path, [])
    if not isinstance(history_data, list):
        history_data = []
    summary_data = load_json(summary_path, {})
    if not isinstance(summary_data, dict):
        summary_data = {}

    normalized_payload, report = build_pinterest_intelligence_report(
        history_data=history_data,
        summary_data=summary_data,
        metadata_dir=metadata_dir,
        pinterest_metadata_dir=pinterest_metadata_dir,
    )
    write_json(normalized_output_path, normalized_payload)
    write_json(output_path, report)
    return {
        "normalized_output_path": normalized_output_path,
        "output_path": output_path,
        "report": report,
    }


def main() -> int:
    args = parse_args()
    result = build_pinterest_intelligence_outputs(
        history_path=Path(args.history_path),
        summary_path=Path(args.summary_path),
        metadata_dir=Path(args.metadata_dir),
        pinterest_metadata_dir=Path(args.pinterest_metadata_dir),
        output_path=Path(args.output_path),
        normalized_output_path=Path(args.normalized_output_path),
    )
    print(result["output_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
