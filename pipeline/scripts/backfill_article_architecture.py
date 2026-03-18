from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from content_architecture import build_article_concepts, load_content_clusters
from topic_clusters import normalize_text


DEFAULT_METADATA_DIR = Path(__file__).resolve().parents[2] / "_data" / "article_metadata"
DEFAULT_CLUSTER_INDEX_PATH = Path(__file__).resolve().parents[1] / "data" / "article_cluster_index.json"
DEFAULT_CLUSTER_REPORT_PATH = Path(__file__).resolve().parents[1] / "data" / "keyword_cluster_report.json"
DEFAULT_CONTENT_PLAN_PATH = Path(__file__).resolve().parents[1] / "reports" / "content_plan.json"
DEFAULT_PILLAR_DIR = Path(__file__).resolve().parents[2] / "clusters"

LEGACY_CLUSTER_ALIASES = {
    "living room curtain styling": "living_room_window_treatments",
    "bedroom curtain styling": "bedroom_window_treatments",
    "linen and boucle styling": "living_room_textures_and_materials",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill legacy article metadata with structured architecture fields."
    )
    parser.add_argument("--metadata-dir", type=str, default=str(DEFAULT_METADATA_DIR))
    parser.add_argument("--write", action="store_true", help="Write inferred metadata updates to disk.")
    return parser.parse_args()


def normalize_identifier(value: Any) -> str:
    return normalize_text(value).replace(" ", "_")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def build_cluster_maps() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    clusters = load_content_clusters()
    by_id: dict[str, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for cluster in clusters:
        cluster_id = str(cluster.get("cluster_id") or "").strip()
        cluster_name = normalize_text(cluster.get("cluster_name") or "")
        if cluster_id:
            by_id[cluster_id] = cluster
        if cluster_name:
            by_name[cluster_name] = cluster
    return by_id, by_name


def build_concept_map() -> dict[str, list[dict[str, Any]]]:
    concepts_by_cluster: dict[str, list[dict[str, Any]]] = {}
    for concept in build_article_concepts():
        cluster_id = str(concept.get("cluster_id") or "").strip()
        if not cluster_id:
            continue
        concepts_by_cluster.setdefault(cluster_id, []).append(concept)
    return concepts_by_cluster


def detect_angle_from_intent(search_intent: str) -> str:
    normalized_intent = normalize_identifier(search_intent)
    if normalized_intent == "problem_solution":
        return "mistakes"
    if normalized_intent == "comparison":
        return "best_options"
    if normalized_intent in {"ideas", "how_to", "style_specific", "budget", "mistakes", "best_options"}:
        return normalized_intent
    return ""


def tokenize_keywords(*values: str) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        for token in normalize_text(value).split():
            if token:
                tokens.add(token)
    return tokens


def keyword_overlap_score(left: str, right: str) -> int:
    left_tokens = tokenize_keywords(left)
    right_tokens = tokenize_keywords(right)
    if not left_tokens or not right_tokens:
        return 0
    overlap = left_tokens & right_tokens
    return len(overlap) * 4


def resolve_cluster_id(
    metadata: dict[str, Any],
    clusters_by_id: dict[str, dict[str, Any]],
    clusters_by_name: dict[str, dict[str, Any]],
) -> str:
    existing_cluster_id = str(metadata.get("cluster_id") or "").strip()
    if existing_cluster_id and existing_cluster_id in clusters_by_id:
        return existing_cluster_id

    cluster_name = normalize_text(metadata.get("cluster_name") or metadata.get("topical_cluster") or "")
    if not cluster_name:
        return ""
    if cluster_name in clusters_by_name:
        return str(clusters_by_name[cluster_name]["cluster_id"])

    aliased_cluster_id = LEGACY_CLUSTER_ALIASES.get(cluster_name, "")
    if aliased_cluster_id in clusters_by_id:
        return aliased_cluster_id
    return ""


def score_concept_match(metadata: dict[str, Any], concept: dict[str, Any], cluster_name: str) -> int:
    score = 0
    primary_keyword = str(metadata.get("primary_keyword") or "").strip()
    secondary_keywords = [
        str(item).strip()
        for item in metadata.get("secondary_keywords", [])
        if str(item).strip()
    ]
    concept_primary = str(concept.get("primary_keyword") or "").strip()
    article_context = " ".join(
        [
            primary_keyword,
            *secondary_keywords,
            str(metadata.get("title") or ""),
            str(metadata.get("excerpt") or ""),
            str(metadata.get("meta_description") or ""),
        ]
    )
    subtopic_id = str(concept.get("subtopic_id") or "").strip()
    subtopic_name = str(concept.get("subtopic_name") or "").strip()

    if normalize_text(primary_keyword) == normalize_text(concept_primary):
        score += 100

    score += keyword_overlap_score(primary_keyword, concept_primary)
    for keyword in secondary_keywords:
        score += min(8, keyword_overlap_score(keyword, concept_primary))

    cluster_tokens = tokenize_keywords(cluster_name, metadata.get("title") or "", metadata.get("excerpt") or "")
    concept_tokens = tokenize_keywords(concept_primary, concept.get("subtopic_name") or "")
    score += len(cluster_tokens & concept_tokens) * 2
    score += len(tokenize_keywords(article_context) & tokenize_keywords(subtopic_name)) * 3

    inferred_angle = detect_angle_from_intent(str(metadata.get("search_intent") or ""))
    if inferred_angle and inferred_angle == str(concept.get("angle_id") or ""):
        score += 12

    normalized_context = normalize_text(article_context)
    if subtopic_id == "layout" and "ideas" in normalize_text(primary_keyword):
        score += 2
    if subtopic_id == "curtain_styles" and "curtain styling" in normalized_context:
        score += 20
    if subtopic_id == "mixes" and "boucle" in normalized_context and "linen" in normalized_context:
        score += 12
    if subtopic_id == "layout" and any(token in normalized_context for token in ["cozy", "corner", "layout"]):
        score += 8
    if subtopic_id == "textures" and any(token in normalized_context for token in ["texture", "layered", "natural materials"]):
        score += 8

    return score


def infer_architecture_fields(
    metadata: dict[str, Any],
    *,
    clusters_by_id: dict[str, dict[str, Any]],
    clusters_by_name: dict[str, dict[str, Any]],
    concepts_by_cluster: dict[str, list[dict[str, Any]]],
) -> dict[str, str]:
    cluster_id = resolve_cluster_id(metadata, clusters_by_id, clusters_by_name)
    if not cluster_id:
        return {}

    cluster = clusters_by_id.get(cluster_id, {})
    cluster_name = str(cluster.get("cluster_name") or metadata.get("cluster_name") or metadata.get("topical_cluster") or "").strip()
    concepts = concepts_by_cluster.get(cluster_id, [])
    if not concepts:
        return {
            "domain_id": str(cluster.get("domain_id") or ""),
            "cluster_id": cluster_id,
            "subtopic_id": "",
            "subtopic_name": "",
            "angle_id": "",
            "modifier": "",
        }

    best_concept = max(
        concepts,
        key=lambda concept: (
            score_concept_match(metadata, concept, cluster_name),
            str(concept.get("subtopic_id") or ""),
            str(concept.get("angle_id") or ""),
        ),
    )

    return {
        "domain_id": str(best_concept.get("domain_id") or cluster.get("domain_id") or ""),
        "cluster_id": cluster_id,
        "subtopic_id": str(best_concept.get("subtopic_id") or ""),
        "subtopic_name": str(best_concept.get("subtopic_name") or ""),
        "angle_id": str(best_concept.get("angle_id") or ""),
        "modifier": str(best_concept.get("modifier") or ""),
    }


def backfill_metadata_file(
    metadata_path: Path,
    *,
    clusters_by_id: dict[str, dict[str, Any]],
    clusters_by_name: dict[str, dict[str, Any]],
    concepts_by_cluster: dict[str, list[dict[str, Any]]],
    write: bool,
) -> dict[str, Any]:
    metadata = load_json(metadata_path, {})
    if not isinstance(metadata, dict):
        return {"path": str(metadata_path), "updated": False, "reason": "invalid_json"}

    inferred = infer_architecture_fields(
        metadata,
        clusters_by_id=clusters_by_id,
        clusters_by_name=clusters_by_name,
        concepts_by_cluster=concepts_by_cluster,
    )
    if not inferred:
        return {"path": str(metadata_path), "updated": False, "reason": "no_match"}

    changed = False
    for key, value in inferred.items():
        if str(metadata.get(key) or "").strip() == str(value or "").strip():
            continue
        metadata[key] = value
        changed = True

    if changed and write:
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "path": str(metadata_path),
        "updated": changed,
        "reason": "backfilled" if changed else "already_set",
        "cluster_id": inferred.get("cluster_id", ""),
        "subtopic_id": inferred.get("subtopic_id", ""),
        "angle_id": inferred.get("angle_id", ""),
    }


def run_backfill(metadata_dir: Path, *, write: bool) -> dict[str, Any]:
    clusters_by_id, clusters_by_name = build_cluster_maps()
    concepts_by_cluster = build_concept_map()

    results: list[dict[str, Any]] = []
    for metadata_path in sorted(metadata_dir.glob("*.json")):
        result = backfill_metadata_file(
            metadata_path,
            clusters_by_id=clusters_by_id,
            clusters_by_name=clusters_by_name,
            concepts_by_cluster=concepts_by_cluster,
            write=write,
        )
        results.append(result)

    updated_count = sum(1 for item in results if item.get("updated"))
    return {
        "metadata_dir": str(metadata_dir),
        "write": write,
        "updated_count": updated_count,
        "results": results,
    }


def main() -> int:
    args = parse_args()
    result = run_backfill(Path(args.metadata_dir), write=args.write)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
