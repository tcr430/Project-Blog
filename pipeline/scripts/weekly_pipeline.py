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
from generate_article import generate_article_package, load_openai_api_key, slugify
from generate_images import generate_and_save_images
from generate_pin_assets import generate_pin_assets
from generate_pin_metadata import generate_pinterest_metadata
from publish_pins import publish_or_queue_pins
from select_trends import (
    filter_recently_used,
    normalize_candidates,
    reject_invalid_and_duplicates,
    score_candidates,
)
from trend_history import DEFAULT_NON_SEASONAL_COOLDOWN_DAYS, add_trend_entry

PINTEREST_VARIANT_COUNT = 4


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


def run_generate_step(trend: str, model: str, products: list[Product]) -> dict[str, Any]:
    log_phase("generating article")
    api_key = load_openai_api_key()
    client = OpenAI(api_key=api_key)
    return generate_article_package(
        client=client,
        trend=trend,
        model=model,
        products=products,
    )


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


def run_image_step(metadata_path: Path, image_model: str, image_size: str, image_quality: str) -> list[Path]:
    log_phase("generating images")
    try:
        return generate_and_save_images(
            metadata_path=metadata_path,
            model=image_model,
            size=image_size,
            quality=image_quality,
        )
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

        log_phase("queueing pins")
        publish_result = publish_or_queue_pins(pinterest_metadata_path=pinterest_metadata_path)
        publish_result["metadata_path"] = pinterest_metadata_path
        publish_result["pin_image_paths"] = pin_image_paths
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
) -> tuple[list[Product], str]:
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
    return product_result["products"], source_label


def run_pipeline_for_trend(
    trend: str,
    model: str,
    image_model: str,
    image_size: str,
    image_quality: str,
    products: list[Product],
) -> tuple[Path, list[Path], str, dict[str, Any] | None]:
    project_root = Path(__file__).resolve().parents[2]

    article_package = run_generate_step(trend=trend, model=model, products=products)

    log_phase("saving temporary article package")
    slug = slugify(str(article_package.get("slug") or article_package.get("title") or "decor-article"))
    package_json_path = build_temp_json_path(project_root=project_root, slug=slug)
    save_article_package(package=article_package, output_path=package_json_path)

    post_path, metadata_path = run_publish_step(project_root=project_root, package_json_path=package_json_path)
    image_paths = run_image_step(
        metadata_path=metadata_path,
        image_model=image_model,
        image_size=image_size,
        image_quality=image_quality,
    )
    pinterest_result = run_pinterest_step(metadata_path=metadata_path)

    return post_path, image_paths, slug, pinterest_result


def select_automatic_trends(
    candidates_file: Path | None,
    trend_source: str,
    top_trends: int,
    cooldown_days: int,
) -> list[dict[str, Any]]:
    history_path = Path(__file__).resolve().parents[1] / "data" / "trend_history.json"

    log_phase("selecting trends: fetching candidates")
    raw_candidates = fetch_candidate_trends(candidates_file=candidates_file, source=trend_source)
    if not raw_candidates:
        raise RuntimeError("No candidate trends were fetched.")

    log_phase("selecting trends: normalizing candidates")
    normalized = normalize_candidates([dict(item) for item in raw_candidates])

    log_phase("selecting trends: filtering invalid and duplicate candidates")
    valid_unique = reject_invalid_and_duplicates(normalized)
    if not valid_unique:
        raise RuntimeError("No valid candidate trends remained after normalization and deduplication.")

    log_phase("selecting trends: filtering repeats from trend history")
    allowed = filter_recently_used(
        candidates=valid_unique,
        history_path=history_path,
        now=datetime.now(timezone.utc),
        cooldown_days=cooldown_days,
    )
    if not allowed:
        raise RuntimeError("No candidate trends remained after trend history filtering.")

    log_phase("selecting trends: scoring and selecting top trends")
    scored = score_candidates(allowed)
    selected = scored[:top_trends]
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
        products, source_label = fetch_products_for_pipeline(
            trend=trend,
            product_provider=args.product_provider,
            product_strict=args.product_strict,
        )
        post_path, image_paths, _, pinterest_result = run_pipeline_for_trend(
            trend=trend,
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
            if pinterest_result.get('mode') == 'queue':
                print(f"Pins queued in: {pinterest_result['queue_path']}")
        return 0
    except Exception as exc:
        print(
            "Error: pipeline failed. Published posts are not rolled back. "
            f"Details: {exc}",
            file=sys.stderr,
        )
        return 1


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
            )
            post_path, image_paths, article_slug, pinterest_result = run_pipeline_for_trend(
                trend=trend_keyword,
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
                if pinterest_result.get('mode') == 'queue':
                    print(f"[auto] pins queued in: {pinterest_result['queue_path']}")

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


