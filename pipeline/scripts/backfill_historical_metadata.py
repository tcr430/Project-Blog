from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backfill_article_architecture import (
    build_cluster_maps,
    build_concept_map,
    infer_architecture_fields,
)
from content_architecture import load_content_clusters
from generate_cluster_report import build_cluster_intelligence_outputs
from topic_clusters import normalize_text


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_METADATA_DIR = PROJECT_ROOT / "_data" / "article_metadata"
DEFAULT_POSTS_DIR = PROJECT_ROOT / "_posts"
DEFAULT_CLUSTER_INDEX_PATH = PROJECT_ROOT / "pipeline" / "data" / "article_cluster_index.json"
DEFAULT_CLUSTER_REPORT_PATH = PROJECT_ROOT / "pipeline" / "data" / "keyword_cluster_report.json"
DEFAULT_PINTEREST_SUMMARY_PATH = PROJECT_ROOT / "pipeline" / "data" / "pinterest_performance_summary.json"
DEFAULT_REPORT_PATH = PROJECT_ROOT / "pipeline" / "reports" / "historical_backfill_report.json"

MOJIBAKE_MARKERS = ("Ã", "Â", "â€™", "â€œ", "â€", "â€“", "â€”", "â€¦")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean up historical metadata and safe mojibake issues without changing published URLs."
    )
    parser.add_argument("--metadata-dir", type=str, default=str(DEFAULT_METADATA_DIR))
    parser.add_argument("--posts-dir", type=str, default=str(DEFAULT_POSTS_DIR))
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--cluster-report-path", type=str, default=str(DEFAULT_CLUSTER_REPORT_PATH))
    parser.add_argument("--pinterest-summary-path", type=str, default=str(DEFAULT_PINTEREST_SUMMARY_PATH))
    parser.add_argument("--report-path", type=str, default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--write", action="store_true", help="Write metadata and post updates to disk.")
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def contains_mojibake(text: str) -> bool:
    return any(marker in text for marker in MOJIBAKE_MARKERS)


def count_mojibake_markers(text: str) -> int:
    return sum(text.count(marker) for marker in MOJIBAKE_MARKERS)


def maybe_fix_mojibake(text: str) -> tuple[str, bool]:
    if not text or not contains_mojibake(text):
        return text, False
    try:
        repaired = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text, False

    old_count = count_mojibake_markers(text)
    new_count = count_mojibake_markers(repaired)
    if "\ufffd" in repaired:
        return text, False
    if new_count >= old_count:
        return text, False
    return repaired, repaired != text


def normalize_string_value(value: str) -> tuple[str, bool]:
    repaired, changed = maybe_fix_mojibake(value)
    normalized = repaired.strip() if repaired == repaired.strip() else repaired.strip()
    return normalized, changed or normalized != value


def normalize_string_list(values: Any) -> tuple[list[str], bool]:
    if not isinstance(values, list):
        return [], False
    updated: list[str] = []
    changed = False
    for item in values:
        normalized, item_changed = normalize_string_value(str(item))
        updated.append(normalized)
        changed = changed or item_changed
    return updated, changed


def build_cluster_lookup() -> dict[str, dict[str, Any]]:
    return {
        str(cluster.get("cluster_id") or "").strip(): cluster
        for cluster in load_content_clusters()
        if str(cluster.get("cluster_id") or "").strip()
    }


def resolve_stable_cluster_name(metadata: dict[str, Any], cluster: dict[str, Any] | None) -> str:
    existing_cluster_name = str(metadata.get("cluster_name") or "").strip()
    topical_cluster = str(metadata.get("topical_cluster") or "").strip()
    canonical_cluster_name = str((cluster or {}).get("cluster_name") or "").strip()
    return existing_cluster_name or topical_cluster or canonical_cluster_name or "uncategorized"


def resolve_actual_post_path(metadata_path: Path, metadata: dict[str, Any]) -> Path:
    post_path_value = str(metadata.get("post_path") or "").strip()
    if post_path_value:
        candidate = Path(post_path_value)
        if candidate.exists():
            return candidate
    fallback = DEFAULT_POSTS_DIR / f"{metadata_path.stem}.md"
    if fallback.exists():
        return fallback
    return Path(post_path_value) if post_path_value else fallback


def apply_metadata_cleanup(
    metadata_path: Path,
    *,
    clusters_by_id: dict[str, dict[str, Any]],
    clusters_by_name: dict[str, dict[str, Any]],
    concepts_by_cluster: dict[str, list[dict[str, Any]]],
    write: bool,
) -> dict[str, Any]:
    metadata = load_json(metadata_path, {})
    if not isinstance(metadata, dict):
        return {
            "metadata_path": str(metadata_path),
            "status": "invalid_json",
            "updated_fields": [],
            "manual_review": ["Metadata file could not be parsed as a JSON object."],
        }

    updated_fields: list[str] = []
    unresolved: list[str] = []
    manual_review: list[str] = []

    inferred = infer_architecture_fields(
        metadata,
        clusters_by_id=clusters_by_id,
        clusters_by_name=clusters_by_name,
        concepts_by_cluster=concepts_by_cluster,
    )
    for key, value in inferred.items():
        normalized = str(value or "").strip()
        if str(metadata.get(key) or "").strip() != normalized:
            metadata[key] = normalized
            updated_fields.append(key)

    cluster_id = str(metadata.get("cluster_id") or "").strip()
    cluster = clusters_by_id.get(cluster_id)
    stable_cluster_name = resolve_stable_cluster_name(metadata, cluster)
    canonical_cluster_name = str((cluster or {}).get("cluster_name") or "").strip()

    if str(metadata.get("cluster_name") or "").strip() != stable_cluster_name:
        metadata["cluster_name"] = stable_cluster_name
        updated_fields.append("cluster_name")

    if canonical_cluster_name and str(metadata.get("canonical_cluster_name") or "").strip() != canonical_cluster_name:
        metadata["canonical_cluster_name"] = canonical_cluster_name
        updated_fields.append("canonical_cluster_name")

    if canonical_cluster_name and stable_cluster_name and stable_cluster_name != canonical_cluster_name:
        if str(metadata.get("legacy_cluster_name") or "").strip() != stable_cluster_name:
            metadata["legacy_cluster_name"] = stable_cluster_name
            updated_fields.append("legacy_cluster_name")

    scalar_keys = [
        "title",
        "slug",
        "meta_description",
        "excerpt",
        "hero_image_prompt",
        "hero_image_alt",
        "article_relative_url",
        "permalink",
        "post_path",
        "primary_keyword",
        "topical_cluster",
        "cluster_name",
        "canonical_cluster_name",
        "legacy_cluster_name",
        "search_intent",
        "subtopic_name",
    ]
    list_keys = [
        "categories",
        "tags",
        "keywords",
        "secondary_keywords",
        "cluster_keywords",
        "section_image_prompts",
        "section_image_paths",
        "section_image_alts",
        "pinterest_titles",
        "pinterest_descriptions",
    ]

    for key in scalar_keys:
        if key not in metadata:
            continue
        normalized, changed = normalize_string_value(str(metadata.get(key) or ""))
        if changed:
            metadata[key] = normalized
            updated_fields.append(key)

    for key in list_keys:
        if key not in metadata:
            continue
        normalized_list, changed = normalize_string_list(metadata.get(key))
        if changed:
            metadata[key] = normalized_list
            updated_fields.append(key)

    if not str(metadata.get("slug") or "").strip():
        metadata["slug"] = metadata_path.stem[11:] if len(metadata_path.stem) > 11 else metadata_path.stem
        updated_fields.append("slug")

    if not str(metadata.get("primary_keyword") or "").strip():
        unresolved.append("Missing primary_keyword after cleanup.")

    if not cluster_id:
        unresolved.append("Missing cluster_id after cleanup.")

    actual_post_path = resolve_actual_post_path(metadata_path, metadata)
    if actual_post_path.exists():
        normalized_post_path = str(actual_post_path)
        if str(metadata.get("post_path") or "").strip() != normalized_post_path:
            metadata["post_path"] = normalized_post_path
            updated_fields.append("post_path")

    if contains_mojibake(json.dumps(metadata, ensure_ascii=False)):
        manual_review.append("Some mojibake markers remain in metadata after automatic cleanup.")

    if write and updated_fields:
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    post_path = resolve_actual_post_path(metadata_path, metadata)
    post_updated = False
    if post_path.exists():
        original_text = post_path.read_text(encoding="utf-8-sig")
        repaired_text, changed = maybe_fix_mojibake(original_text)
        if changed:
            if write:
                post_path.write_text(repaired_text, encoding="utf-8")
            post_updated = True
        elif contains_mojibake(original_text):
            manual_review.append(f"Post still contains possible mojibake: {post_path.name}")

    return {
        "metadata_path": str(metadata_path),
        "post_path": str(post_path) if post_path.exists() else "",
        "status": "updated" if updated_fields or post_updated else "unchanged",
        "updated_fields": sorted(dict.fromkeys(updated_fields)),
        "post_updated": post_updated,
        "cluster_id": str(metadata.get("cluster_id") or "").strip(),
        "cluster_name": str(metadata.get("cluster_name") or "").strip(),
        "canonical_cluster_name": str(metadata.get("canonical_cluster_name") or "").strip(),
        "unresolved": unresolved,
        "manual_review": manual_review,
    }


def run_backfill(
    *,
    metadata_dir: Path,
    posts_dir: Path,
    cluster_index_path: Path,
    cluster_report_path: Path,
    pinterest_summary_path: Path,
    report_path: Path,
    write: bool,
) -> dict[str, Any]:
    del posts_dir
    clusters_by_id, clusters_by_name = build_cluster_maps()
    concepts_by_cluster = build_concept_map()

    results: list[dict[str, Any]] = []
    for metadata_path in sorted(metadata_dir.glob("*.json")):
        results.append(
            apply_metadata_cleanup(
                metadata_path,
                clusters_by_id=clusters_by_id,
                clusters_by_name=clusters_by_name,
                concepts_by_cluster=concepts_by_cluster,
                write=write,
            )
        )

    updated_records = [item for item in results if item["status"] == "updated"]
    unresolved_records = [item for item in results if item.get("unresolved")]
    manual_review_records = [item for item in results if item.get("manual_review")]

    if write:
        build_cluster_intelligence_outputs(
            metadata_dir=metadata_dir,
            pinterest_summary_path=pinterest_summary_path,
            cluster_report_path=cluster_report_path,
            cluster_index_path=cluster_index_path,
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "write": write,
        "scanned_records": len(results),
        "updated_records": len(updated_records),
        "records_with_unresolved_issues": len(unresolved_records),
        "records_requiring_manual_review": len(manual_review_records),
        "normalized_records": results,
        "unresolved_issues": [
            {
                "metadata_path": item["metadata_path"],
                "issues": item.get("unresolved", []),
            }
            for item in unresolved_records
        ],
        "manual_review": [
            {
                "metadata_path": item["metadata_path"],
                "issues": item.get("manual_review", []),
            }
            for item in manual_review_records
        ],
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    args = parse_args()
    result = run_backfill(
        metadata_dir=Path(args.metadata_dir),
        posts_dir=Path(args.posts_dir),
        cluster_index_path=Path(args.cluster_index_path),
        cluster_report_path=Path(args.cluster_report_path),
        pinterest_summary_path=Path(args.pinterest_summary_path),
        report_path=Path(args.report_path),
        write=args.write,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
