from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = PROJECT_ROOT / "pipeline"
SCRIPTS_ROOT = PIPELINE_ROOT / "scripts"
REPORT_PATH = PIPELINE_ROOT / "reports" / "pipeline_cost_audit.json"
ARTICLE_METADATA_DIR = PROJECT_ROOT / "_data" / "article_metadata"
ARTICLE_PACKAGE_DIR = PIPELINE_ROOT / "data" / "article_packages"
COST_REPORTS_DIR = PIPELINE_ROOT / "data" / "cost_reports"

OPENAI_PRICING = {
    "retrieved_at": "2026-03-14",
    "sources": {
        "gpt_4_1_mini": "https://platform.openai.com/docs/pricing",
        "gpt_image_1": "https://openai.com/api/pricing/",
    },
    "gpt_4_1_mini": {
        "input_per_million_usd": 0.40,
        "output_per_million_usd": 1.60,
    },
    "gpt_image_1": {
        "text_input_per_million_usd": 5.0,
        "image_output_square_low_usd": 0.01,
        "image_output_square_medium_usd": 0.04,
        "image_output_square_high_usd": 0.17,
    },
}

QA_ASSUMPTIONS = {
    "input_tokens_per_call": 3000,
    "output_tokens_per_call": 150,
}

PREVIOUS_AUDIT_REFERENCE = {
    "found_in_repo": False,
    "reference_type": "conversation baseline",
    "value_usd_per_article": 1.50,
    "note": "No prior cost audit file was found in the repo. This baseline comes from the earlier project estimate mentioned in the task context.",
}

MISSING_SCRIPTS = [
    "validate_article.py",
    "generate_cluster_hubs.py",
    "generate_seo_report.py",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an updated cost audit for the content pipeline.")
    parser.add_argument("--output-path", type=str, default=str(REPORT_PATH))
    return parser.parse_args()


def ensure_script_imports() -> None:
    scripts_path = str(SCRIPTS_ROOT)
    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)


def approx_tokens(text: str) -> int:
    cleaned = str(text or "")
    if not cleaned:
        return 0
    return max(1, math.ceil(len(cleaned) / 4))


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def list_recent_metadata(limit: int = 5) -> list[dict[str, Any]]:
    if not ARTICLE_METADATA_DIR.exists():
        return []

    rows: list[dict[str, Any]] = []
    for path in sorted(ARTICLE_METADATA_DIR.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
        payload = load_json(path, default={})
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def list_recent_article_packages(limit: int = 10) -> list[dict[str, Any]]:
    if not ARTICLE_PACKAGE_DIR.exists():
        return []

    packages: list[dict[str, Any]] = []
    for path in sorted(ARTICLE_PACKAGE_DIR.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
        payload = load_json(path, default={})
        package = payload.get("package") if isinstance(payload, dict) else None
        if isinstance(package, dict):
            packages.append(package)
    return packages


def list_recent_cost_reports(limit: int = 10) -> list[dict[str, Any]]:
    if not COST_REPORTS_DIR.exists():
        return []

    reports: list[dict[str, Any]] = []
    for path in sorted(COST_REPORTS_DIR.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
        payload = load_json(path, default={})
        if isinstance(payload, dict):
            reports.append(payload)
    return reports


def average(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def image_area_factor(size: str) -> float:
    try:
        width_text, height_text = size.lower().split("x", 1)
        width = int(width_text)
        height = int(height_text)
    except Exception:
        return 1.0
    return round((width * height) / (1024 * 1024), 4)


def estimate_article_prompt_tokens(sample_articles: list[dict[str, Any]]) -> dict[str, Any]:
    ensure_script_imports()
    import generate_article
    import fetch_products

    prompts: list[dict[str, Any]] = []
    article_template = generate_article.load_article_template()

    for metadata in sample_articles:
        trend = str(metadata.get("primary_keyword") or metadata.get("title") or metadata.get("slug") or "").strip()
        if not trend:
            continue

        topic_context = generate_article.normalize_topic_candidate(
            {
                "trend_cluster": metadata.get("topical_cluster") or metadata.get("trend_cluster") or trend,
                "trend_keyword": metadata.get("primary_keyword") or trend,
                "primary_keyword": metadata.get("primary_keyword") or trend,
                "secondary_keywords": metadata.get("secondary_keywords") or [],
                "cluster_keywords": metadata.get("cluster_keywords") or [],
                "search_intent": metadata.get("search_intent") or "ideas",
                "season": metadata.get("season") or "",
                "holiday": metadata.get("holiday") or "",
                "source": metadata.get("source") or "metadata",
            },
            trend=trend,
        )
        selected_format_name, selected_format_prompt = generate_article.resolve_format_prompt(
            trend=trend,
            format_name=None,
        )
        selected_persona_name, selected_persona_prompt = generate_article.resolve_persona_prompt(
            trend=trend,
            persona_name=None,
        )
        products = fetch_products.fetch_mock_products_for_trend(trend=trend, limit=generate_article.SECTION_COUNT)
        article_prompt = article_template.format(trend=trend)
        user_prompt = (
            f"Use this writing persona ({selected_persona_name}):\n"
            f"{selected_persona_prompt}\n\n"
            f"{article_prompt}\n\n"
            f"{generate_article.build_search_strategy_prompt(topic_context)}\n\n"
            f"Use this article format template ({selected_format_name}):\n"
            f"{selected_format_prompt}\n\n"
            f"{generate_article.build_products_prompt(products)}\n\n"
            f"{generate_article.OUTPUT_REQUIREMENTS_PROMPT}"
        )
        prompts.append(
            {
                "trend": trend,
                "system_tokens": approx_tokens(generate_article.SYSTEM_PROMPT),
                "user_tokens": approx_tokens(user_prompt),
            }
        )

    total_tokens = [row["system_tokens"] + row["user_tokens"] for row in prompts]
    return {
        "sample_count": len(prompts),
        "samples": prompts,
        "average_input_tokens": round(average([float(item) for item in total_tokens]), 2),
    }


def estimate_article_output_tokens(article_packages: list[dict[str, Any]]) -> dict[str, Any]:
    output_tokens: list[int] = []
    word_counts: list[int] = []

    for package in article_packages:
        output_tokens.append(approx_tokens(json.dumps(package, ensure_ascii=False)))
        article_markdown = str(package.get("article_markdown") or "")
        word_counts.append(len(article_markdown.split()))

    return {
        "sample_count": len(output_tokens),
        "average_output_tokens": round(average([float(item) for item in output_tokens]), 2),
        "average_article_words": round(average([float(item) for item in word_counts]), 2),
    }


def estimate_article_stage(sample_articles: list[dict[str, Any]], article_packages: list[dict[str, Any]], cost_reports: list[dict[str, Any]]) -> dict[str, Any]:
    prompt_estimate = estimate_article_prompt_tokens(sample_articles)
    output_estimate = estimate_article_output_tokens(article_packages)

    pricing = OPENAI_PRICING["gpt_4_1_mini"]
    avg_generation_calls = average(
        [
            float(report.get("article_generation", {}).get("generation_calls", 0))
            for report in cost_reports
        ]
    ) or 1.0

    input_tokens = prompt_estimate["average_input_tokens"] or 3200
    output_tokens = output_estimate["average_output_tokens"] or 3200
    per_call_cost = (
        (input_tokens / 1_000_000) * pricing["input_per_million_usd"]
        + (output_tokens / 1_000_000) * pricing["output_per_million_usd"]
    )
    normal_cost = per_call_cost * avg_generation_calls
    worst_case_calls = 3

    return {
        "stage": "article_generation",
        "script": "pipeline/scripts/generate_article.py",
        "model": "gpt-4.1-mini",
        "external_cost_source": "OpenAI Responses API",
        "expected_calls_per_article": round(avg_generation_calls, 2),
        "worst_case_calls_per_article": worst_case_calls,
        "estimated_input_tokens_per_call": round(input_tokens, 2),
        "estimated_output_tokens_per_call": round(output_tokens, 2),
        "estimated_cost_per_call_usd": round(per_call_cost, 4),
        "estimated_cost_per_article_usd": round(normal_cost, 4),
        "estimated_worst_case_cost_per_article_usd": round(per_call_cost * worst_case_calls, 4),
        "observations": {
            "average_article_words": output_estimate["average_article_words"],
            "cache_enabled": True,
            "short_retry_limit": 1,
            "product_retry_limit": 1,
        },
        "recent_feature_effects": [
            {
                "feature": "keyword targeting and topical cluster context",
                "effect": "increase",
                "reason": "Adds prompt tokens to the same article-generation call, but does not add a new call.",
            },
            {
                "feature": "FAQ generation",
                "effect": "increase",
                "reason": "Adds article body and JSON output size inside the same call.",
            },
            {
                "feature": "affiliate card rendering",
                "effect": "neutral",
                "reason": "Rendering is local; affiliate product metadata already exists inside the article package flow.",
            },
        ],
        "prompt_samples": prompt_estimate,
    }


def estimate_image_stage(cost_reports: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    image_reports = [report.get("image_generation", {}) for report in cost_reports if isinstance(report.get("image_generation"), dict)]
    size = "1536x1024"
    if image_reports:
        size = str(image_reports[0].get("size") or size)

    factor = image_area_factor(size)
    pricing = OPENAI_PRICING["gpt_image_1"]
    hero_image_cost = pricing["image_output_square_high_usd"] * factor
    section_image_cost = pricing["image_output_square_medium_usd"] * factor

    avg_generation_calls = average([float(report.get("generation_calls", 0)) for report in image_reports]) or 6.0
    avg_qa_calls = average([float(report.get("qa_calls", 0)) for report in image_reports]) or 1.0

    normal_generation_cost = hero_image_cost + (5 * section_image_cost)
    worst_case_generation_cost = (2 * hero_image_cost) + (5 * section_image_cost)

    qa_pricing = OPENAI_PRICING["gpt_4_1_mini"]
    qa_cost_per_call = (
        (QA_ASSUMPTIONS["input_tokens_per_call"] / 1_000_000) * qa_pricing["input_per_million_usd"]
        + (QA_ASSUMPTIONS["output_tokens_per_call"] / 1_000_000) * qa_pricing["output_per_million_usd"]
    )
    qa_stage_cost = qa_cost_per_call * avg_qa_calls

    image_stage = {
        "stage": "image_generation",
        "script": "pipeline/scripts/generate_images.py",
        "model": "gpt-image-1",
        "external_cost_source": "OpenAI Images API",
        "expected_calls_per_article": round(avg_generation_calls, 2),
        "worst_case_calls_per_article": 7,
        "configured_size": size,
        "configured_mix": {
            "hero": {"count": 1, "quality": "high", "estimated_cost_each_usd": round(hero_image_cost, 4)},
            "sections": {"count": 5, "quality": "medium", "estimated_cost_each_usd": round(section_image_cost, 4)},
        },
        "estimated_cost_per_article_usd": round(normal_generation_cost, 4),
        "estimated_worst_case_cost_per_article_usd": round(worst_case_generation_cost, 4),
        "assumptions": {
            "size_area_factor_vs_1024_square": factor,
            "pricing_basis": "Official square-image pricing scaled by pixel area for 1536x1024 output.",
        },
        "recent_feature_effects": [
            {
                "feature": "improved Pinterest pin templates",
                "effect": "neutral",
                "reason": "Pin templates render locally from the hero image and do not add OpenAI image calls.",
            },
            {
                "feature": "body section images",
                "effect": "already accounted",
                "reason": "This remains the largest direct cost driver because five section images are still generated per article.",
            },
        ],
    }

    qa_stage = {
        "stage": "image_quality_assurance",
        "script": "pipeline/scripts/generate_images.py",
        "model": "gpt-4.1-mini",
        "external_cost_source": "OpenAI Responses API",
        "expected_calls_per_article": round(avg_qa_calls, 2),
        "worst_case_calls_per_article": 2,
        "estimated_input_tokens_per_call": QA_ASSUMPTIONS["input_tokens_per_call"],
        "estimated_output_tokens_per_call": QA_ASSUMPTIONS["output_tokens_per_call"],
        "estimated_cost_per_call_usd": round(qa_cost_per_call, 4),
        "estimated_cost_per_article_usd": round(qa_stage_cost, 4),
        "estimated_worst_case_cost_per_article_usd": round(qa_cost_per_call * 2, 4),
        "assumptions": {
            "note": "Image-input tokenization for vision QA is approximated as 3,000 input-token equivalents because the repo does not record exact billed image tokens.",
        },
        "recent_feature_effects": [
            {
                "feature": "section-image QA removal",
                "effect": "decrease",
                "reason": "Only the hero image still receives a QA model pass.",
            }
        ],
    }
    return image_stage, qa_stage


def cost_neutral_stage(
    *,
    stage: str,
    script: str,
    reason: str,
    recent_effects: list[dict[str, str]] | None = None,
    external_cost_source: str = "none",
) -> dict[str, Any]:
    return {
        "stage": stage,
        "script": script,
        "external_cost_source": external_cost_source,
        "estimated_cost_per_article_usd": 0.0,
        "estimated_worst_case_cost_per_article_usd": 0.0,
        "status": "cost_neutral",
        "reason": reason,
        "recent_feature_effects": recent_effects or [],
    }


def build_audit() -> dict[str, Any]:
    sample_articles = list_recent_metadata(limit=5)
    article_packages = list_recent_article_packages(limit=10)
    cost_reports = list_recent_cost_reports(limit=10)

    article_stage = estimate_article_stage(sample_articles, article_packages, cost_reports)
    image_stage, qa_stage = estimate_image_stage(cost_reports)

    stages: list[dict[str, Any]] = [
        cost_neutral_stage(
            stage="trend_selection_and_keyword_planning",
            script="pipeline/scripts/weekly_pipeline.py, pipeline/scripts/fetch_trends.py, pipeline/scripts/select_trends.py, pipeline/scripts/generate_content_plan.py",
            reason="Topic selection, keyword targeting, cluster scoring, and content-plan generation are rule-based local operations with no model calls.",
            recent_effects=[
                {
                    "feature": "content planning engine",
                    "effect": "neutral",
                    "reason": "It reuses local cluster metadata and does not call a model.",
                },
                {
                    "feature": "keyword targeting and topical clusters",
                    "effect": "neutral",
                    "reason": "Selection logic is local; only the article-generation prompt grew slightly.",
                },
            ],
        ),
        cost_neutral_stage(
            stage="product_fetching",
            script="pipeline/scripts/fetch_products.py",
            reason="Amazon/mock product fetching uses external APIs or local JSON, but the repo does not indicate a direct per-call usage fee. Operationally important, but treated as $0 marginal cost here.",
            external_cost_source="Amazon PA-API or mock data (no direct usage fee modeled)",
            recent_effects=[
                {
                    "feature": "partial affiliate coverage and manual products file support",
                    "effect": "neutral",
                    "reason": "These change behavior and resilience, not direct API spend.",
                }
            ],
        ),
        article_stage,
        cost_neutral_stage(
            stage="basic_article_validation",
            script="pipeline/scripts/generate_article.py",
            reason="Structure, FAQ, product-link, and reading-time checks are local validation logic inside article generation.",
            recent_effects=[
                {
                    "feature": "FAQ presence checks",
                    "effect": "neutral",
                    "reason": "Validation is local and deterministic.",
                }
            ],
        ),
        cost_neutral_stage(
            stage="advanced_seo_validation",
            script="pipeline/scripts/validate_article_seo.py",
            reason="Cannibalization, semantic coverage, and title checks are all rule-based local comparisons against JSON metadata.",
            recent_effects=[
                {
                    "feature": "SEO validation layer",
                    "effect": "neutral",
                    "reason": "No model call is used; it only compares strings and headings locally.",
                }
            ],
        ),
        cost_neutral_stage(
            stage="publishing_and_affiliate_card_rendering",
            script="pipeline/scripts/publish_post.py",
            reason="Publishing, internal-link surfacing, FAQ extraction, and affiliate-card HTML rendering are local file transformations.",
            recent_effects=[
                {
                    "feature": "affiliate card rendering",
                    "effect": "neutral",
                    "reason": "Cards are rendered from existing metadata and markdown without extra API calls.",
                },
                {
                    "feature": "FAQ structured data",
                    "effect": "neutral",
                    "reason": "Schema is derived locally from article markdown.",
                },
            ],
        ),
        image_stage,
        qa_stage,
        cost_neutral_stage(
            stage="pinterest_metadata_generation",
            script="pipeline/scripts/generate_pin_metadata.py",
            reason="Pinterest title/description packaging, board selection, and strategy weighting are local logic.",
            recent_effects=[
                {
                    "feature": "improved Pinterest strategy weighting",
                    "effect": "neutral",
                    "reason": "Variant planning is local and uses saved analytics JSON only.",
                }
            ],
        ),
        cost_neutral_stage(
            stage="pinterest_pin_asset_rendering",
            script="pipeline/scripts/generate_pin_assets.py",
            reason="Branded pin templates are rendered locally as SVGs from the existing hero image; no new image-generation API calls occur.",
            recent_effects=[
                {
                    "feature": "pin templates and branded overlays",
                    "effect": "neutral",
                    "reason": "This added local SVG rendering only.",
                }
            ],
        ),
        cost_neutral_stage(
            stage="pinterest_publishing_and_analytics",
            script="pipeline/scripts/publish_pins.py, pipeline/scripts/fetch_pinterest_analytics.py",
            reason="Pinterest publishing and analytics syncing use external APIs but no direct per-request usage fee is modeled in the repo.",
            external_cost_source="Pinterest API (no direct usage fee modeled)",
        ),
        cost_neutral_stage(
            stage="newsletter_generation",
            script="pipeline/scripts/generate_weekly_newsletter.py",
            reason="The weekly newsletter draft is assembled from local article metadata, not generated by a model.",
            recent_effects=[
                {
                    "feature": "newsletter generation",
                    "effect": "neutral",
                    "reason": "This is a local markdown assembly step.",
                }
            ],
        ),
        cost_neutral_stage(
            stage="newsletter_push_to_kit",
            script="pipeline/scripts/push_newsletter_to_kit.py",
            reason="Kit broadcast draft creation is an external API operation but no direct per-call usage fee is modeled.",
            external_cost_source="Kit API (no direct usage fee modeled)",
        ),
        cost_neutral_stage(
            stage="cluster_reporting_and_pillar_pages",
            script="pipeline/scripts/generate_cluster_report.py, pipeline/scripts/generate_pillar_pages.py",
            reason="Cluster reports, hub/pillar pages, and article-cluster indexes are generated from local JSON metadata.",
            recent_effects=[
                {
                    "feature": "cluster performance reports",
                    "effect": "neutral",
                    "reason": "These are local JSON summaries.",
                },
                {
                    "feature": "cluster pillar pages",
                    "effect": "neutral",
                    "reason": "Static markdown pages are generated locally from the cluster index.",
                },
            ],
        ),
    ]

    direct_cost_total = round(sum(stage["estimated_cost_per_article_usd"] for stage in stages), 4)
    worst_case_total = round(sum(stage["estimated_worst_case_cost_per_article_usd"] for stage in stages), 4)

    cost_changing_features = [
        {
            "feature": "keyword targeting and topical clusters",
            "change_type": "increase",
            "magnitude": "small",
            "reason": "Slightly longer article prompts and article output, but still inside the single article-generation call.",
        },
        {
            "feature": "FAQ generation",
            "change_type": "increase",
            "magnitude": "small",
            "reason": "Adds article body length and output JSON size to the same text-generation call.",
        },
        {
            "feature": "SEO validation layer",
            "change_type": "neutral",
            "magnitude": "none",
            "reason": "Entirely local rule-based checks.",
        },
        {
            "feature": "cluster performance reports, content planning, hub pages, pillar pages",
            "change_type": "neutral",
            "magnitude": "none",
            "reason": "Generated from local metadata and markdown only.",
        },
        {
            "feature": "newsletter generation and Kit draft creation",
            "change_type": "neutral",
            "magnitude": "none",
            "reason": "No model calls were added; the draft is assembled locally.",
        },
        {
            "feature": "improved Pinterest pin generation and templates",
            "change_type": "neutral",
            "magnitude": "none",
            "reason": "Metadata and branded pin rendering stay local and reuse the hero image.",
        },
        {
            "feature": "section-image QA reduction",
            "change_type": "decrease",
            "magnitude": "small",
            "reason": "Only hero images still receive a QA model call.",
        },
        {
            "feature": "article and image caching",
            "change_type": "decrease",
            "magnitude": "large_on_reruns",
            "reason": "Reruns can avoid most expensive model and image calls entirely.",
        },
    ]

    optimization_recommendations = [
        {
            "priority": "high",
            "opportunity": "Reduce section image count or make sections conditional on article type.",
            "reason": "Five medium-quality section images remain the largest marginal cost driver.",
            "estimated_effect": "Could reduce per-article cost by roughly $0.06 per skipped section image at current settings.",
        },
        {
            "priority": "high",
            "opportunity": "Generate fewer fresh article images for low-priority or experimental posts.",
            "reason": "Image generation dominates total cost; text generation is comparatively cheap.",
            "estimated_effect": "A hero-only mode would likely cut fresh-article cost by more than half.",
        },
        {
            "priority": "medium",
            "opportunity": "Track actual token usage from OpenAI responses if the SDK exposes usage consistently for article generation.",
            "reason": "Current text costs are estimated from prompt/package size, not billed token counts.",
            "estimated_effect": "Would improve audit accuracy more than it would reduce spend.",
        },
        {
            "priority": "medium",
            "opportunity": "Skip Pinterest analytics sync and summary rebuild on runs where no pins were published.",
            "reason": "These stages are cost-neutral in API-billing terms but add runtime and external dependency churn.",
            "estimated_effect": "Operational simplification rather than direct cost savings.",
        },
        {
            "priority": "low",
            "opportunity": "Bundle image QA signals into generation policy only if bad hero acceptance becomes an issue.",
            "reason": "Hero QA cost is already very small after section QA removal.",
            "estimated_effect": "Savings are marginal compared with image output costs.",
        },
    ]

    comparison = {
        "previous_audit_reference": PREVIOUS_AUDIT_REFERENCE,
        "current_estimated_cost_per_article_usd": direct_cost_total,
        "current_estimated_worst_case_cost_per_article_usd": worst_case_total,
        "delta_vs_reference_usd": round(direct_cost_total - PREVIOUS_AUDIT_REFERENCE["value_usd_per_article"], 4),
        "summary": (
            "Recent SEO, cluster, newsletter, and Pinterest template additions are mostly cost-neutral because they are local transformations. "
            "The main direct spend still comes from one text-generation call plus six image-generation calls."
        ),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "audit_type": "per_article_pipeline_cost",
            "notes": "Shared weekly tasks are included explicitly. In the current code, they are cost-neutral in API-billing terms because they do not use paid model calls.",
        },
        "pricing": OPENAI_PRICING,
        "missing_requested_scripts": [
            {
                "script": f"pipeline/scripts/{name}",
                "present": False,
                "cost_effect": "not_applicable",
            }
            for name in MISSING_SCRIPTS
        ],
        "observed_repo_signals": {
            "recent_article_metadata_samples": len(sample_articles),
            "recent_article_package_samples": len(article_packages),
            "recent_cost_report_samples": len(cost_reports),
        },
        "stages": stages,
        "totals": {
            "estimated_direct_cost_per_article_usd": direct_cost_total,
            "estimated_direct_worst_case_cost_per_article_usd": worst_case_total,
            "largest_cost_driver": "image_generation",
            "largest_cost_driver_estimated_usd": image_stage["estimated_cost_per_article_usd"],
        },
        "feature_impact_since_last_audit": cost_changing_features,
        "comparison_with_previous_audit": comparison,
        "optimization_recommendations": optimization_recommendations,
    }


def write_report(output_path: Path, payload: dict[str, Any]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def main() -> int:
    args = parse_args()
    report = build_audit()
    output_path = write_report(Path(args.output_path), report)
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
