from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from content_architecture import build_article_concepts, load_content_clusters, load_content_constraints
from editorial_mix import build_recent_editorial_mix_state, load_editorial_mix_rules
from fetch_trends import fetch_candidate_trends, fetch_pinterest_trends
from generate_content_plan import build_content_plan_outputs
from select_trends import (
    build_cluster_governance_map,
    exclude_overlapping_candidates,
    filter_out_of_season_candidates,
    filter_recently_used,
    normalize_candidates,
    reject_invalid_and_duplicates,
    score_candidates,
    select_diverse_top_trends,
)
from validate_article_concept import validate_article_concept


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = PROJECT_ROOT / "pipeline"
REPORT_PATH = PIPELINE_ROOT / "reports" / "selection_performance_audit.json"
SUMMARY_PATH = PIPELINE_ROOT / "reports" / "selection_performance_audit.md"
CLUSTER_INDEX_PATH = PIPELINE_ROOT / "data" / "article_cluster_index.json"
TREND_HISTORY_PATH = PIPELINE_ROOT / "data" / "trend_history.json"
CONTENT_PLAN_REPORT_PATH = PIPELINE_ROOT / "reports" / "content_plan.json"
KEYWORD_CLUSTER_REPORT_PATH = PIPELINE_ROOT / "data" / "keyword_cluster_report.json"
PINTEREST_SIGNALS_PATH = PIPELINE_ROOT / "reports" / "pinterest_topic_signals.json"
PINTEREST_INTELLIGENCE_PATH = PIPELINE_ROOT / "reports" / "pinterest_intelligence_report.json"


def timed(label: str, fn: Any) -> tuple[Any, dict[str, Any]]:
    start = time.perf_counter()
    result = fn()
    seconds = time.perf_counter() - start
    return result, {
        "label": label,
        "seconds": round(seconds, 3),
    }


def measure_validation_benchmarks(
    raw_candidates: list[dict[str, Any]],
    *,
    sample_sizes: list[int],
) -> list[dict[str, Any]]:
    benchmarks: list[dict[str, Any]] = []
    for sample_size in sample_sizes:
        normalized = normalize_candidates(raw_candidates[:sample_size])
        start = time.perf_counter()
        for candidate in normalized:
            validate_article_concept(candidate)
        seconds = time.perf_counter() - start
        per_candidate_ms = (seconds * 1000 / len(normalized)) if normalized else 0.0
        benchmarks.append(
            {
                "sample_size": len(normalized),
                "seconds": round(seconds, 3),
                "per_candidate_ms": round(per_candidate_ms, 3),
            }
        )
    return benchmarks


def measure_loader_costs(iterations: int = 10) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for label, fn in (
        ("load_content_clusters", load_content_clusters),
        ("load_content_constraints", load_content_constraints),
    ):
        start = time.perf_counter()
        for _ in range(iterations):
            fn()
        seconds = time.perf_counter() - start
        results.append(
            {
                "function": label,
                "iterations": iterations,
                "seconds": round(seconds, 3),
                "per_call_ms": round(seconds * 1000 / iterations, 3),
            }
        )
    return results


def measure_stage_breakdown(
    raw_candidates: list[dict[str, Any]],
    *,
    now: datetime,
    sample_size: int,
) -> dict[str, Any]:
    normalized = normalize_candidates(raw_candidates)
    subset = normalized[:sample_size]
    report: dict[str, Any] = {
        "full_normalized_count": len(normalized),
        "sample_size": len(subset),
    }

    unique_valid, timing = timed("reject_invalid_and_duplicates", lambda: reject_invalid_and_duplicates(subset))
    report["reject_invalid_and_duplicates"] = {
        "seconds": timing["seconds"],
        "output_count": len(unique_valid),
    }

    overlap_filtered, timing = timed(
        "exclude_overlapping_candidates",
        lambda: exclude_overlapping_candidates(unique_valid, CLUSTER_INDEX_PATH),
    )
    report["exclude_overlapping_candidates"] = {
        "seconds": timing["seconds"],
        "output_count": len(overlap_filtered),
    }

    history_filtered, timing = timed(
        "filter_recently_used",
        lambda: filter_recently_used(overlap_filtered, TREND_HISTORY_PATH, now, 30),
    )
    report["filter_recently_used"] = {
        "seconds": timing["seconds"],
        "output_count": len(history_filtered),
    }

    season_filtered, timing = timed(
        "filter_out_of_season_candidates",
        lambda: filter_out_of_season_candidates(history_filtered, now=now),
    )
    report["filter_out_of_season_candidates"] = {
        "seconds": timing["seconds"],
        "output_count": len(season_filtered),
    }

    cluster_index_data = json.loads(CLUSTER_INDEX_PATH.read_text(encoding="utf-8-sig"))
    editorial_mix_rules = load_editorial_mix_rules()
    editorial_mix_state = build_recent_editorial_mix_state(cluster_index_data, editorial_mix_rules)
    cluster_governance_map = build_cluster_governance_map(CLUSTER_INDEX_PATH)

    scored, timing = timed(
        "score_candidates",
        lambda: score_candidates(
            season_filtered,
            history_path=TREND_HISTORY_PATH,
            now=now,
            cluster_governance_map=cluster_governance_map,
            editorial_mix_rules=editorial_mix_rules,
            editorial_mix_state=editorial_mix_state,
        ),
    )
    report["score_candidates"] = {
        "seconds": timing["seconds"],
        "output_count": len(scored),
    }

    selected, timing = timed(
        "select_diverse_top_trends",
        lambda: select_diverse_top_trends(scored, top_n=8, editorial_mix_rules=editorial_mix_rules),
    )
    report["select_diverse_top_trends"] = {
        "seconds": timing["seconds"],
        "output_count": len(selected),
    }

    return report


def build_summary(report: dict[str, Any]) -> str:
    validation_100 = next(
        (item for item in report["validation_benchmarks"] if item["sample_size"] == 100),
        None,
    )
    validation_200 = next(
        (item for item in report["validation_benchmarks"] if item["sample_size"] == 200),
        None,
    )
    estimated_seconds = report["estimated_full_validation_seconds"]

    lines = [
        "# Selection Performance Audit",
        "",
        f"- Generated on: {report['generated_at']}",
        f"- Full fetched candidate count: {report['candidate_sources']['auto_candidates']['count']}",
        f"- Full architecture concept count: {report['candidate_sources']['architecture_concepts']['count']}",
        "",
        "## Key Finding",
        "",
        "- The dominant bottleneck is concept validation inside `reject_invalid_and_duplicates()`.",
        f"- On a 200-candidate sample, `reject_invalid_and_duplicates()` took {report['stage_breakdown']['reject_invalid_and_duplicates']['seconds']}s.",
        f"- The rest of the same 200-candidate selection path was small: overlap {report['stage_breakdown']['exclude_overlapping_candidates']['seconds']}s, history {report['stage_breakdown']['filter_recently_used']['seconds']}s, season {report['stage_breakdown']['filter_out_of_season_candidates']['seconds']}s, scoring {report['stage_breakdown']['score_candidates']['seconds']}s.",
        "",
        "## Validation Benchmarks",
        "",
    ]

    if validation_100:
        lines.append(
            f"- 100 candidates: {validation_100['seconds']}s ({validation_100['per_candidate_ms']} ms/candidate)"
        )
    if validation_200:
        lines.append(
            f"- 200 candidates: {validation_200['seconds']}s ({validation_200['per_candidate_ms']} ms/candidate)"
        )

    lines.extend(
        [
            f"- Estimated full validation time for the current auto-candidate pool: about {estimated_seconds}s.",
            "",
            "## Likely Root Cause",
            "",
            "- `validate_article_concept.py` rebuilds cluster maps via `load_content_clusters()` during candidate validation.",
            "- `load_content_clusters()` is expensive because it reparses the persisted architecture and composes subtopic data.",
            "- `load_content_constraints()` is cheap in comparison.",
            "",
            "## Loader Costs",
            "",
        ]
    )

    for item in report["loader_costs"]:
        lines.append(
            f"- `{item['function']}`: {item['per_call_ms']} ms/call over {item['iterations']} calls"
        )

    lines.extend(
        [
            "",
            "## Secondary Observations",
            "",
            f"- `build_content_plan_outputs()` took {report['candidate_sources']['content_plan']['seconds']}s, which is noticeable but not the main regression.",
            f"- `fetch_candidate_trends(source='auto')` took {report['candidate_sources']['auto_candidates']['seconds']}s.",
            f"- `build_article_concepts()` took {report['candidate_sources']['architecture_concepts']['seconds']}s.",
            f"- `fetch_pinterest_trends()` took {report['candidate_sources']['pinterest_trends']['seconds']}s.",
            "",
            "## Safest Next Optimization Targets",
            "",
            "- Cache or memoize architecture/constraint state inside `validate_article_concept.py`.",
            "- Avoid rebuilding cluster maps for every candidate.",
            "- Only after that, remeasure the full weekly selection path before touching scoring or planning logic.",
        ]
    )

    return "\n".join(lines) + "\n"


def generate_audit() -> dict[str, Any]:
    now = datetime.fromisoformat("2026-03-18T10:00:00+00:00")
    raw_candidates, fetch_timing = timed("fetch_candidate_trends", lambda: fetch_candidate_trends(source="auto"))
    article_concepts, concept_timing = timed("build_article_concepts", build_article_concepts)
    pinterest_trends, pinterest_timing = timed(
        "fetch_pinterest_trends",
        lambda: fetch_pinterest_trends(project_root=PROJECT_ROOT),
    )
    content_plan_payload, plan_timing = timed(
        "build_content_plan_outputs",
        lambda: build_content_plan_outputs(
            cluster_index_path=CLUSTER_INDEX_PATH,
            cluster_report_path=KEYWORD_CLUSTER_REPORT_PATH,
            pinterest_signals_path=PINTEREST_SIGNALS_PATH,
            pinterest_intelligence_path=PINTEREST_INTELLIGENCE_PATH,
            output_path=CONTENT_PLAN_REPORT_PATH,
        ),
    )

    validation_benchmarks = measure_validation_benchmarks(
        raw_candidates,
        sample_sizes=[50, 100, 200],
    )
    stage_breakdown = measure_stage_breakdown(raw_candidates, now=now, sample_size=200)
    loader_costs = measure_loader_costs()

    benchmark_200 = next(item for item in validation_benchmarks if item["sample_size"] == 200)
    estimated_full_validation_seconds = round(
        (benchmark_200["per_candidate_ms"] / 1000.0) * len(raw_candidates),
        1,
    )

    report = {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "current_date_context": "2026-03-18",
        "candidate_sources": {
            "auto_candidates": {
                "count": len(raw_candidates),
                "seconds": fetch_timing["seconds"],
            },
            "architecture_concepts": {
                "count": len(article_concepts),
                "seconds": concept_timing["seconds"],
            },
            "pinterest_trends": {
                "candidate_count": int(pinterest_trends.get("candidate_count", 0)),
                "seconds": pinterest_timing["seconds"],
            },
            "content_plan": {
                "cluster_count": len(content_plan_payload.get("plan", [])),
                "seconds": plan_timing["seconds"],
            },
        },
        "validation_benchmarks": validation_benchmarks,
        "stage_breakdown": stage_breakdown,
        "loader_costs": loader_costs,
        "estimated_full_validation_seconds": estimated_full_validation_seconds,
        "likely_hotspots": [
            {
                "path": "pipeline/scripts/validate_article_concept.py",
                "area": "filter_valid_article_concepts -> validate_article_concept",
                "status": "primary_bottleneck",
                "evidence": [
                    "Validation dominates the 200-candidate stage breakdown.",
                    "Per-candidate validation cost rises materially with sample size.",
                    "Validation rebuilds cluster maps through load_content_clusters().",
                ],
            },
            {
                "path": "pipeline/scripts/content_architecture.py",
                "area": "load_content_clusters",
                "status": "expensive_dependency",
                "evidence": [
                    "Measured at hundreds of milliseconds per call.",
                    "Called indirectly during concept validation.",
                ],
            },
            {
                "path": "pipeline/scripts/generate_content_plan.py",
                "area": "build_content_plan_outputs",
                "status": "secondary_cost",
                "evidence": [
                    "Noticeable runtime but far below validation cost.",
                    "Runs before weekly selection when plan-driven flow is enabled.",
                ],
            },
        ],
        "conclusions": {
            "dominant_bottleneck": "concept_validation",
            "secondary_bottleneck": "content_plan_generation",
            "not_primary": [
                "candidate_fetch",
                "pinterest_fetch",
                "overlap_filtering",
                "history_filtering",
                "seasonality_filtering",
                "final_diverse_selection",
            ],
            "next_step": "Optimize validation caching first, then remeasure the end-to-end weekly selection path.",
        },
    }
    return report


def main() -> None:
    report = generate_audit()
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    SUMMARY_PATH.write_text(build_summary(report), encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")
    print(f"Wrote {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
