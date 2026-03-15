from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from openai import OpenAI

from fetch_products import Product, fetch_products_with_context
from fetch_trends import fetch_candidate_trends
from generate_article import (
    generate_article_package_with_report,
    load_openai_api_key,
    load_products_from_file,
    slugify,
)
from generate_images import generate_and_save_images_with_report
from generate_weekly_newsletter import generate_weekly_newsletter_draft
from push_newsletter_to_kit import push_weekly_newsletter_to_kit
from generate_weekly_report import build_weekly_report
from generate_cluster_report import build_cluster_intelligence_outputs
from generate_pillar_pages import build_pillar_pages
from generate_content_plan import build_content_plan_outputs, select_topic_candidates_from_plan
from generate_pinterest_topic_signals import build_pinterest_topic_signal_outputs
from validate_article_seo import validate_article_seo, write_validation_report
from fetch_pinterest_analytics import should_sync_pinterest_analytics, sync_pinterest_analytics
from generate_pin_assets import generate_pin_assets
from pinterest_performance_summary import build_performance_summary
from plan_pinterest_repins import plan_pinterest_repins
from generate_pin_metadata import generate_pinterest_metadata
from publish_pins import publish_or_queue_pins
from select_trends import (
    filter_recently_used,
    normalize_candidates,
    reject_invalid_and_duplicates,
    score_candidates,
    select_diverse_top_trends,
)
from topic_clusters import TopicCandidate, build_manual_topic_candidate
from trend_history import DEFAULT_NON_SEASONAL_COOLDOWN_DAYS, add_trend_entry

PINTEREST_VARIANT_COUNT = 4
COST_REPORTS_DIR = Path(__file__).resolve().parents[1] / "data" / "cost_reports"
SEO_VALIDATION_REPORT_PATH = Path(__file__).resolve().parents[1] / "reports" / "article_seo_validation_report.json"
CONTENT_PLAN_REPORT_PATH = Path(__file__).resolve().parents[1] / "reports" / "content_plan.json"
PINTEREST_TOPIC_SIGNALS_REPORT_PATH = Path(__file__).resolve().parents[1] / "reports" / "pinterest_topic_signals.json"


def log_phase(message: str) -> None:
    print(f"[phase] {message}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the MVP weekly pipeline in manual mode or automatic trend-selection mode."
    )
    parser.add_argument(
        "trend",
        nargs="?",
        type=str,
        help='Optional manual trend text, e.g. "terracotta kitchen decor".',
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4.1-mini",
        help="OpenAI model to use for article generation (default: gpt-4.1-mini).",
    )
    parser.add_argument(
        "--image-model",
        type=str,
        default="gpt-image-1",
        help="OpenAI image model to use (default: gpt-image-1).",
    )
    parser.add_argument(
        "--image-size",
        type=str,
        default="1536x1024",
        help="Generated image size (default: 1536x1024).",
    )
    parser.add_argument(
        "--image-quality",
        type=str,
        default="high",
        help="Generated image quality hint (default: high).",
    )
    parser.add_argument(
        "--product-provider",
        type=str,
        default=None,
        choices=["mock", "amazon"],
        help="Optional product provider override (default: uses PRODUCT_PROVIDER env var).",
    )
    parser.add_argument(
        "--products-file",
        type=str,
        default=None,
        help=(
            "Optional JSON file with manually selected product objects. "
            "If provided, the pipeline uses these products directly and skips provider fetching."
        ),
    )
    parser.add_argument(
        "--product-strict",
        action="store_true",
        help="Disable fallback to mock products when provider fetch fails.",
    )
    parser.add_argument(
        "--candidates-file",
        type=str,
        default=None,
        help="Optional JSON file with trend candidates for automatic mode.",
    )
    parser.add_argument(
        "--trend-source",
        type=str,
        default="auto",
        choices=["auto", "file", "mock"],
        help="Trend source for automatic mode (default: auto).",
    )
    parser.add_argument(
        "--top-trends",
        type=int,
        default=3,
        help="How many trends to generate in automatic mode (default: 3).",
    )
    parser.add_argument(
        "--cooldown-days",
        type=int,
        default=DEFAULT_NON_SEASONAL_COOLDOWN_DAYS,
        help=(
            "Non-seasonal cooldown days for trend reuse filtering "
            f"(default: {DEFAULT_NON_SEASONAL_COOLDOWN_DAYS})."
        ),
    )
    parser.add_argument(
        "--use-content-plan",
        action="store_true",
        help="Use the generated content plan as the first source of automatic topic candidates.",
    )
    parser.add_argument(
        "--skip-newsletter",
        action="store_true",
        help="Skip newsletter draft generation and Kit sync for this run.",
    )
    return parser.parse_args()


def build_temp_json_path(project_root: Path, slug: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    filename = f"article-package-{timestamp}-{slug}.json"
    return project_root / "pipeline" / "data" / filename


def save_article_package(package: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(package, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def load_publish_module(project_root: Path) -> Any:
    publish_script_path = project_root / "pipeline" / "scripts" / "publish_post.py"
    if not publish_script_path.exists():
        raise RuntimeError(f"publish_post.py was not found at {publish_script_path}")

    spec = importlib.util.spec_from_file_location("publish_post", publish_script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load publish_post.py module spec.")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def find_publish_function(module: Any) -> Callable[..., Any]:
    candidate_names = [
        "publish_post_from_package_file",
        "publish_post_from_json_file",
        "publish_post",
        "publish",
    ]

    for name in candidate_names:
        candidate = getattr(module, name, None)
        if callable(candidate):
            return candidate

    raise RuntimeError(
        "publish_post.py does not expose a supported publish function. "
        f"Expected one of: {', '.join(candidate_names)}"
    )


def find_sync_images_function(module: Any) -> Callable[..., Any]:
    candidate = getattr(module, "sync_post_images", None)
    if callable(candidate):
        return candidate

    raise RuntimeError("publish_post.py does not expose a sync_post_images function.")


def build_cost_report_path(slug: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return COST_REPORTS_DIR / f"{timestamp}-{slug}.json"


def write_cost_report(output_path: Path, payload: dict[str, Any]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def run_generate_step(
    trend: str,
    model: str,
    topic_context: TopicCandidate,
    products: list[Product],
) -> tuple[dict[str, Any], dict[str, Any]]:
    log_phase("generating article")
    api_key = load_openai_api_key()
    client = OpenAI(api_key=api_key)
    return generate_article_package_with_report(
        client=client,
        trend=trend,
        model=model,
        topic_context=topic_context,
        products=products,
    )


def run_seo_validation_step(project_root: Path, article_package: dict[str, Any]) -> dict[str, Any]:
    log_phase("running advanced seo validation")
    cluster_index_path = project_root / "pipeline" / "data" / "article_cluster_index.json"
    cluster_report_path = project_root / "pipeline" / "data" / "keyword_cluster_report.json"
    trend_history_path = project_root / "pipeline" / "data" / "trend_history.json"

    existing_index_data: dict[str, Any] = {"articles": []}
    if cluster_index_path.exists():
        raw_index = cluster_index_path.read_text(encoding="utf-8-sig").strip()
        if raw_index:
            existing_index_data = json.loads(raw_index)

    cluster_report_data: dict[str, Any] = {"clusters": []}
    if cluster_report_path.exists():
        raw_report = cluster_report_path.read_text(encoding="utf-8-sig").strip()
        if raw_report:
            cluster_report_data = json.loads(raw_report)

    trend_history_data: dict[str, Any] = {"entries": []}
    if trend_history_path.exists():
        raw_history = trend_history_path.read_text(encoding="utf-8-sig").strip()
        if raw_history:
            trend_history_data = json.loads(raw_history)

    result = validate_article_seo(
        article_package=article_package,
        existing_index_data=existing_index_data,
        cluster_report_data=cluster_report_data,
        trend_history_data=trend_history_data,
    )
    report_path = write_validation_report(SEO_VALIDATION_REPORT_PATH, result)
    print(f"[seo] article validation: {result['validation_status']} ({report_path})")
    for warning in result.get("warnings", []):
        print(f"[seo][warning] {warning}")

    if result["validation_status"] == "fail":
        raise RuntimeError(
            "Advanced SEO validation failed: " + "; ".join(result.get("errors", []))
        )

    return result


def run_publish_step(project_root: Path, package_json_path: Path) -> tuple[Path, Path]:
    log_phase("publishing post")
    module = load_publish_module(project_root)
    publish_fn = find_publish_function(module)

    publish_result = publish_fn(package_json_path)

    if isinstance(publish_result, dict):
        post_value = (
            publish_result.get("post_path")
            or publish_result.get("markdown_path")
            or publish_result.get("output_path")
        )
        metadata_value = publish_result.get("metadata_path")

        if isinstance(post_value, (str, Path)) and isinstance(metadata_value, (str, Path)):
            return Path(post_value), Path(metadata_value)

    if isinstance(publish_result, Path):
        post_path = publish_result
    elif isinstance(publish_result, str):
        post_path = Path(publish_result)
    else:
        raise RuntimeError(
            "Publish step did not return a recognized result. "
            "Expected dict with post_path/metadata_path or a markdown path."
        )

    metadata_path = project_root / "_data" / "article_metadata" / f"{post_path.stem}.json"
    if not metadata_path.exists():
        raise RuntimeError(f"Metadata file was not found after publish: {metadata_path}")

    return post_path, metadata_path


def run_image_step(
    project_root: Path,
    post_path: Path,
    metadata_path: Path,
    image_model: str,
    image_size: str,
    image_quality: str,
) -> tuple[list[Path], dict[str, Any]]:
    log_phase("generating images")
    try:
        saved_paths, image_report = generate_and_save_images_with_report(
            metadata_path=metadata_path,
            model=image_model,
            size=image_size,
            quality=image_quality,
        )

        log_phase("syncing post body images")
        module = load_publish_module(project_root)
        sync_images_fn = find_sync_images_function(module)
        sync_images_fn(post_path=post_path, metadata_path=metadata_path)

        return saved_paths, image_report
    except Exception as exc:
        raise RuntimeError(
            "Image generation failed after publish. The markdown post is kept as-is. "
            f"Details: {exc}"
        ) from exc


def run_pinterest_step(metadata_path: Path) -> dict[str, Any] | None:
    try:
        log_phase("generating pinterest metadata")
        pinterest_metadata_path = generate_pinterest_metadata(
            metadata_path=metadata_path,
            variant_count=PINTEREST_VARIANT_COUNT,
        )

        log_phase("generating pin images")
        pin_image_paths = generate_pin_assets(pinterest_metadata_path=pinterest_metadata_path)

        log_phase("scheduling and queueing pins")
        publish_result = publish_or_queue_pins(pinterest_metadata_path=pinterest_metadata_path)
        publish_result["metadata_path"] = pinterest_metadata_path
        publish_result["pin_image_paths"] = pin_image_paths

        project_root = Path(__file__).resolve().parents[2]
        if should_sync_pinterest_analytics(project_root) and publish_result.get("history_path"):
            log_phase("syncing pinterest analytics")
            analytics_result = sync_pinterest_analytics(
                history_path=Path(publish_result["history_path"]),
            )
            publish_result["analytics_result"] = analytics_result

        if publish_result.get("history_path"):
            log_phase("building pinterest performance summary")
            summary_result = build_performance_summary(
                history_path=Path(publish_result["history_path"]),
                summary_path=project_root / "pipeline" / "data" / "pinterest_performance_summary.json",
                article_scores_path=project_root / "pipeline" / "data" / "pinterest_article_scores.json",
            )
            publish_result["performance_summary"] = summary_result

            log_phase("planning pinterest repins")
            repin_result = plan_pinterest_repins(
                article_scores_path=Path(summary_result["article_scores_path"]),
                history_path=Path(publish_result["history_path"]),
                queue_path=project_root / "pipeline" / "data" / "pinterest_queue.json",
            )
            publish_result["repin_plan"] = repin_result

        return publish_result
    except Exception as exc:
        print(
            "[warning] Pinterest automation failed after publish. The blog post remains published. "
            f"Details: {exc}"
        )
        return None


def fetch_products_for_pipeline(
    trend: str,
    product_provider: str | None,
    product_strict: bool,
    products_file: str | None = None,
) -> tuple[list[Product], str]:
    manual_products = load_products_from_file(products_file)
    if manual_products is not None:
        log_phase("loading manual products")
        print(f"[products] source: manual products file")
        print(f"[products] fetched: {len(manual_products)}")
        return manual_products, "manual products"

    log_phase("fetching products")
    product_result = fetch_products_with_context(
        trend=trend,
        limit=5,
        provider_name=product_provider,
        allow_fallback_to_mock=not product_strict,
    )

    resolved_provider = product_result["resolved_provider"]
    used_fallback = product_result["used_fallback"]

    if resolved_provider == "amazon":
        source_label = "real Amazon products"
    elif used_fallback:
        source_label = "fallback mock products"
    else:
        source_label = "mock products"

    print(f"[products] source: {source_label}")
    print(f"[products] fetched: {len(product_result['products'])}")
    return product_result["products"], source_label


def run_pipeline_for_trend(
    trend: str,
    topic_context: TopicCandidate,
    model: str,
    image_model: str,
    image_size: str,
    image_quality: str,
    products: list[Product],
) -> tuple[Path, list[Path], str, dict[str, Any] | None, Path]:
    project_root = Path(__file__).resolve().parents[2]

    article_package, article_report = run_generate_step(
        trend=trend,
        model=model,
        topic_context=topic_context,
        products=products,
    )
    print(
        f"[article] mode: {'affiliate-enabled' if article_report.get('affiliate_mode') else 'editorial-only'}; "
        f"primary keyword: {article_report.get('primary_keyword', topic_context['primary_keyword'])}; "
        f"products passed: {article_report.get('selected_products', 0)}; "
        f"visible affiliate links: {article_report.get('generated_link_count', 0)}"
    )

    seo_validation_result = run_seo_validation_step(project_root=project_root, article_package=article_package)

    log_phase("saving temporary article package")
    slug = slugify(str(article_package.get("slug") or article_package.get("title") or "decor-article"))
    package_json_path = build_temp_json_path(project_root=project_root, slug=slug)
    save_article_package(package=article_package, output_path=package_json_path)

    post_path, metadata_path = run_publish_step(project_root=project_root, package_json_path=package_json_path)
    image_paths, image_report = run_image_step(
        project_root=project_root,
        post_path=post_path,
        metadata_path=metadata_path,
        image_model=image_model,
        image_size=image_size,
        image_quality=image_quality,
    )
    pinterest_result = run_pinterest_step(metadata_path=metadata_path)

    cost_report_path = write_cost_report(
        output_path=build_cost_report_path(slug=slug),
        payload={
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "trend": trend,
            "slug": slug,
            "article_generation": article_report,
            "seo_validation": seo_validation_result,
            "image_generation": image_report,
            "pinterest": {
                "variants_requested": PINTEREST_VARIANT_COUNT,
                "pin_assets_generated": bool(pinterest_result),
                "mode": pinterest_result.get("mode") if pinterest_result else None,
            },
        },
    )

    return post_path, image_paths, slug, pinterest_result, cost_report_path


def finalize_selected_trends(
    raw_candidates: list[dict[str, Any]],
    history_path: Path,
    top_trends: int,
    cooldown_days: int,
    pinterest_signal_data: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    log_phase("selecting trends: normalizing candidates")
    normalized = normalize_candidates([dict(item) for item in raw_candidates])

    log_phase("selecting trends: filtering invalid and duplicate candidates")
    valid_unique = reject_invalid_and_duplicates(normalized)
    if not valid_unique:
        return []

    log_phase("selecting trends: filtering repeats from trend history")
    allowed = filter_recently_used(
        candidates=valid_unique,
        history_path=history_path,
        now=datetime.now(timezone.utc),
        cooldown_days=cooldown_days,
    )
    if not allowed:
        return []

    log_phase("selecting trends: scoring and selecting top trends")
    scored = score_candidates(allowed, pinterest_signal_data=pinterest_signal_data)
    return select_diverse_top_trends(scored, top_n=top_trends)


def select_automatic_trends(
    candidates_file: Path | None,
    trend_source: str,
    top_trends: int,
    cooldown_days: int,
    use_content_plan: bool = False,
) -> list[dict[str, Any]]:
    history_path = Path(__file__).resolve().parents[1] / "data" / "trend_history.json"

    raw_candidates: list[dict[str, Any]] | None = None
    project_root = Path(__file__).resolve().parents[2]
    pinterest_signal_result = build_pinterest_topic_signal_outputs(
        cluster_index_path=project_root / "pipeline" / "data" / "article_cluster_index.json",
        pinterest_summary_path=project_root / "pipeline" / "data" / "pinterest_performance_summary.json",
        output_path=PINTEREST_TOPIC_SIGNALS_REPORT_PATH,
    )
    pinterest_signal_data = pinterest_signal_result["signals"]
    print(f"[pinterest] topic signals: {pinterest_signal_result['output_path']}")

    if use_content_plan:
        log_phase("selecting trends: building content plan candidates")
        plan_result = build_content_plan_outputs(
            cluster_index_path=project_root / "pipeline" / "data" / "article_cluster_index.json",
            cluster_report_path=project_root / "pipeline" / "data" / "keyword_cluster_report.json",
            pinterest_signals_path=PINTEREST_TOPIC_SIGNALS_REPORT_PATH,
            output_path=CONTENT_PLAN_REPORT_PATH,
        )
        planned_candidates = select_topic_candidates_from_plan(plan_result["plan"], limit=max(top_trends * 3, top_trends))
        if planned_candidates:
            raw_candidates = [dict(item) for item in planned_candidates]
            print(f"[plan] using {len(raw_candidates)} planned topic candidate(s)")
        else:
            print("[plan] no content-plan candidates available; falling back to trend fetch")

    if raw_candidates is None:
        log_phase("selecting trends: fetching candidates")
        raw_candidates = fetch_candidate_trends(candidates_file=candidates_file, source=trend_source)
        if not raw_candidates:
            raise RuntimeError("No candidate trends were fetched.")

    selected = finalize_selected_trends(
        raw_candidates=raw_candidates,
        history_path=history_path,
        top_trends=top_trends,
        cooldown_days=cooldown_days,
        pinterest_signal_data=pinterest_signal_data,
    )

    if not selected and use_content_plan:
        print("[plan] planned candidates were exhausted by validation/history filters; falling back to trend fetch")
        log_phase("selecting trends: fetching fallback candidates")
        fallback_candidates = fetch_candidate_trends(candidates_file=candidates_file, source=trend_source)
        if not fallback_candidates:
            raise RuntimeError("No candidate trends were fetched.")
        selected = finalize_selected_trends(
            raw_candidates=fallback_candidates,
            history_path=history_path,
            top_trends=top_trends,
            cooldown_days=cooldown_days,
            pinterest_signal_data=pinterest_signal_data,
        )

    if not selected:
        raise RuntimeError("No trends were selected.")

    for item in selected:
        print(f"  selected: {item['trend_keyword']} (score={item['score']})")

    return selected


def run_manual_mode(args: argparse.Namespace) -> int:
    trend = (args.trend or "").strip()
    if not trend:
        print("Error: trend cannot be empty in manual mode.", file=sys.stderr)
        return 1

    try:
        log_phase("selecting trends: manual input")
        print(f"[manual] running pipeline for trend: {trend}")
        topic_context = build_manual_topic_candidate(trend)
        products, source_label = fetch_products_for_pipeline(
            trend=trend,
            product_provider=args.product_provider,
            product_strict=args.product_strict,
            products_file=args.products_file,
        )
        post_path, image_paths, _, pinterest_result, cost_report_path = run_pipeline_for_trend(
            trend=trend,
            topic_context=topic_context,
            model=args.model,
            image_model=args.image_model,
            image_size=args.image_size,
            image_quality=args.image_quality,
            products=products,
        )

        print(f"Success: article published to {post_path} ({source_label})")
        for image_path in image_paths:
            print(f"Image saved: {image_path}")
        if pinterest_result:
            print(f"Pinterest metadata: {pinterest_result['metadata_path']}")
            for pin_path in pinterest_result['pin_image_paths']:
                print(f"Pin image saved: {pin_path}")
            if pinterest_result.get('history_path'):
                print(f"Pinterest history: {pinterest_result['history_path']}")
            if pinterest_result.get('analytics_result'):
                print(f"Pinterest analytics sync updated: {pinterest_result['analytics_result']['updated_count']}")
            if pinterest_result.get('performance_summary'):
                print(f"Pinterest performance summary: {pinterest_result['performance_summary']['summary_path']}")
            if pinterest_result.get('repin_plan'):
                print(f"Pinterest repins planned: {pinterest_result['repin_plan']['planned_count']}")
            if pinterest_result.get('mode') == 'queue':
                print(f"Pins queued in: {pinterest_result['queue_path']}")
        print(f"Cost report: {cost_report_path}")
        project_root = Path(__file__).resolve().parents[2]
        if args.skip_newsletter:
            print("[newsletter] skipped for this run")
            newsletter_path, kit_result = None, None
        else:
            newsletter_path, kit_result = run_newsletter_step(project_root)
        run_report_step(project_root)
        run_seo_intelligence_step(project_root)
        run_pinterest_topic_signal_step(project_root)
        run_content_plan_step(project_root)
        run_cluster_pages_step(project_root)
        if newsletter_path is not None:
            print(f"Newsletter draft: {newsletter_path}")
        if kit_result and kit_result.get("sidecar_path"):
            print(f"Kit newsletter metadata: {kit_result['sidecar_path']}")
        return 0
    except Exception as exc:
        print(
            "Error: pipeline failed. Published posts are not rolled back. "
            f"Details: {exc}",
            file=sys.stderr,
        )
        return 1


def run_newsletter_step(project_root: Path) -> tuple[Path, dict[str, Any] | None]:
    log_phase("generating weekly newsletter draft")
    draft_path = generate_weekly_newsletter_draft(
        metadata_dir=project_root / "_data" / "article_metadata",
        output_dir=project_root / "pipeline" / "data" / "newsletter_drafts",
    )
    print(f"[newsletter] weekly draft generated: {draft_path}")

    try:
        log_phase("pushing weekly newsletter draft to Kit")
        kit_result = push_weekly_newsletter_to_kit(draft_path=draft_path)
        if kit_result.get("status") == "created":
            print(f"[kit] draft broadcast id: {kit_result['kit_broadcast_id']}")
        elif kit_result.get("status") == "unchanged":
            print(f"[kit] existing draft broadcast id: {kit_result['kit_broadcast_id']}")
        return draft_path, kit_result
    except Exception as exc:
        print(
            "[warning] Kit draft creation failed. The local weekly newsletter draft is kept. "
            f"Details: {exc}"
        )
        return draft_path, None


def run_report_step(project_root: Path) -> Path:
    log_phase("generating weekly report")
    report_path = build_weekly_report(
        history_path=project_root / "pipeline" / "data" / "pinterest_history.json",
        summary_path=project_root / "pipeline" / "data" / "pinterest_performance_summary.json",
        output_path=project_root / "pipeline" / "reports" / "weekly_report.md",
    )
    print(f"[report] weekly report generated: {report_path}")
    return report_path


def run_seo_intelligence_step(project_root: Path) -> dict[str, Any]:
    log_phase("building seo cluster intelligence")
    result = build_cluster_intelligence_outputs(
        metadata_dir=project_root / "_data" / "article_metadata",
        pinterest_summary_path=project_root / "pipeline" / "data" / "pinterest_performance_summary.json",
        cluster_report_path=project_root / "pipeline" / "data" / "keyword_cluster_report.json",
        cluster_index_path=project_root / "pipeline" / "data" / "article_cluster_index.json",
    )
    print(f"[seo] keyword cluster report: {result['cluster_report_path']}")
    print(f"[seo] article cluster index: {result['cluster_index_path']}")
    return result


def run_content_plan_step(project_root: Path) -> dict[str, Any]:
    log_phase("generating content plan")
    result = build_content_plan_outputs(
        cluster_index_path=project_root / "pipeline" / "data" / "article_cluster_index.json",
        cluster_report_path=project_root / "pipeline" / "data" / "keyword_cluster_report.json",
        pinterest_signals_path=project_root / "pipeline" / "reports" / "pinterest_topic_signals.json",
        output_path=CONTENT_PLAN_REPORT_PATH,
    )
    print(f"[plan] content plan: {result['output_path']}")
    return result


def run_pinterest_topic_signal_step(project_root: Path) -> dict[str, Any]:
    log_phase("generating pinterest topic signals")
    result = build_pinterest_topic_signal_outputs(
        cluster_index_path=project_root / "pipeline" / "data" / "article_cluster_index.json",
        pinterest_summary_path=project_root / "pipeline" / "data" / "pinterest_performance_summary.json",
        output_path=project_root / "pipeline" / "reports" / "pinterest_topic_signals.json",
    )
    print(f"[pinterest] topic signals: {result['output_path']}")
    return result


def run_cluster_pages_step(project_root: Path) -> dict[str, Any]:
    log_phase("generating cluster pillar pages")
    result = build_pillar_pages(
        cluster_index_path=project_root / "pipeline" / "data" / "article_cluster_index.json",
        output_dir=project_root / "clusters",
        min_articles=1,
    )
    print(f"[seo] cluster pillar pages: {result['generated_count']} generated, {result['removed_count']} removed")
    return result


def run_automatic_mode(args: argparse.Namespace) -> int:
    if args.top_trends <= 0:
        print("Error: --top-trends must be greater than zero.", file=sys.stderr)
        return 1

    candidates_file = Path(args.candidates_file) if args.candidates_file else None
    history_path = Path(__file__).resolve().parents[1] / "data" / "trend_history.json"

    try:
        selected_trends = select_automatic_trends(
            candidates_file=candidates_file,
            trend_source=args.trend_source,
            top_trends=args.top_trends,
            cooldown_days=args.cooldown_days,
            use_content_plan=args.use_content_plan,
        )

        generated_posts: list[Path] = []
        for index, trend_item in enumerate(selected_trends, start=1):
            trend_keyword = trend_item["trend_keyword"]
            log_phase(f"processing trend {index}/{len(selected_trends)}")
            print(f"[auto] trend: {trend_keyword}")

            products, source_label = fetch_products_for_pipeline(
                trend=trend_keyword,
                product_provider=args.product_provider,
                product_strict=args.product_strict,
                products_file=args.products_file,
            )
            post_path, image_paths, article_slug, pinterest_result, cost_report_path = run_pipeline_for_trend(
                trend=trend_keyword,
                topic_context=trend_item,
                model=args.model,
                image_model=args.image_model,
                image_size=args.image_size,
                image_quality=args.image_quality,
                products=products,
            )

            add_trend_entry(
                trend_cluster=trend_item["trend_cluster"],
                trend_keyword=trend_item["trend_keyword"],
                season=trend_item.get("season", ""),
                holiday=trend_item.get("holiday", ""),
                article_slug=article_slug,
                history_path=history_path,
            )

            generated_posts.append(post_path)
            print(f"[auto] published: {post_path} ({source_label})")
            for image_path in image_paths:
                print(f"[auto] image saved: {image_path}")
            if pinterest_result:
                print(f"[auto] pinterest metadata: {pinterest_result['metadata_path']}")
                for pin_path in pinterest_result['pin_image_paths']:
                    print(f"[auto] pin image saved: {pin_path}")
                if pinterest_result.get('history_path'):
                    print(f"[auto] pinterest history: {pinterest_result['history_path']}")
                if pinterest_result.get('analytics_result'):
                    print(f"[auto] pinterest analytics sync updated: {pinterest_result['analytics_result']['updated_count']}")
                if pinterest_result.get('performance_summary'):
                    print(f"[auto] pinterest performance summary: {pinterest_result['performance_summary']['summary_path']}")
                if pinterest_result.get('repin_plan'):
                    print(f"[auto] pinterest repins planned: {pinterest_result['repin_plan']['planned_count']}")
                if pinterest_result.get('mode') == 'queue':
                    print(f"[auto] pins queued in: {pinterest_result['queue_path']}")
            print(f"[auto] cost report: {cost_report_path}")

        project_root = Path(__file__).resolve().parents[2]
        if args.skip_newsletter:
            print("[newsletter] skipped for this run")
            newsletter_path, kit_result = None, None
        else:
            newsletter_path, kit_result = run_newsletter_step(project_root)
        run_report_step(project_root)
        run_seo_intelligence_step(project_root)
        run_pinterest_topic_signal_step(project_root)
        run_content_plan_step(project_root)
        run_cluster_pages_step(project_root)
        if newsletter_path is not None:
            print(f"Newsletter draft: {newsletter_path}")
        if kit_result and kit_result.get("sidecar_path"):
            print(f"Kit newsletter metadata: {kit_result['sidecar_path']}")
        print(f"Success: generated {len(generated_posts)} posts in automatic mode.")
        return 0
    except Exception as exc:
        print(
            "Error: automatic pipeline failed. Any published posts are not rolled back. "
            f"Details: {exc}",
            file=sys.stderr,
        )
        return 1


def main() -> int:
    args = parse_args()

    if args.trend:
        return run_manual_mode(args)

    return run_automatic_mode(args)


if __name__ == "__main__":
    raise SystemExit(main())



