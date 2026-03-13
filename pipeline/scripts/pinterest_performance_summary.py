from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pinterest_analytics import (
    ARTICLE_SCORES_FILE_PATH,
    HISTORY_FILE_PATH,
    SUMMARY_FILE_PATH,
    build_metrics_snapshot,
    compute_engagement_rate,
    is_published_entry,
    load_history,
    save_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Pinterest performance summary files from Pinterest history."
    )
    parser.add_argument(
        "--history-path",
        type=str,
        default=str(HISTORY_FILE_PATH),
        help="Path to pinterest_history.json.",
    )
    parser.add_argument(
        "--summary-path",
        type=str,
        default=str(SUMMARY_FILE_PATH),
        help="Path to write the performance summary JSON.",
    )
    parser.add_argument(
        "--article-scores-path",
        type=str,
        default=str(ARTICLE_SCORES_FILE_PATH),
        help="Path to write the article scores JSON.",
    )
    return parser.parse_args()


def group_entries(entries: list[dict[str, Any]], key_fn) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        key = key_fn(entry)
        grouped.setdefault(key, []).append(entry)
    return grouped


def build_group_summary(
    entries: list[dict[str, Any]],
    *,
    key_name: str,
    key_fn,
    extra_fn=None,
) -> list[dict[str, Any]]:
    grouped = group_entries(entries, key_fn)
    results: list[dict[str, Any]] = []

    for key, group_entries_list in grouped.items():
        item: dict[str, Any] = {
            key_name: key,
            **build_metrics_snapshot(group_entries_list),
        }
        if extra_fn is not None:
            item.update(extra_fn(group_entries_list))
        results.append(item)

    results.sort(
        key=lambda item: (
            item.get("average_outbound_clicks") or 0,
            item.get("average_saves") or 0,
            item.get("average_impressions") or 0,
        ),
        reverse=True,
    )
    return results


def build_global_baseline(entries: list[dict[str, Any]]) -> dict[str, float]:
    snapshot = build_metrics_snapshot(entries)
    return {
        "average_impressions": float(snapshot.get("average_impressions") or 0),
        "average_saves": float(snapshot.get("average_saves") or 0),
        "average_outbound_clicks": float(snapshot.get("average_outbound_clicks") or 0),
        "average_engagement_rate": float(snapshot.get("average_engagement_rate") or 0),
    }


def score_article(entry: dict[str, Any], baseline: dict[str, float]) -> dict[str, Any]:
    score = 0
    reasons: list[str] = []

    comparisons = [
        ("average_impressions", "impressions"),
        ("average_saves", "saves"),
        ("average_outbound_clicks", "outbound clicks"),
        ("average_engagement_rate", "engagement rate"),
    ]

    for field_name, label in comparisons:
        article_value = entry.get(field_name)
        baseline_value = baseline.get(field_name, 0)
        if article_value is None or baseline_value <= 0:
            continue
        if float(article_value) >= baseline_value:
            score += 1
            reasons.append(f"Above baseline for {label}")

    analytics_ready_pins = int(entry.get("analytics_ready_pins") or 0)
    if analytics_ready_pins >= 2:
        score += 1
        reasons.append("Has at least two pins with analytics")

    if score >= 4:
        classification = "strong_candidate"
    elif score >= 2:
        classification = "neutral"
    else:
        classification = "underperforming"

    return {
        "score": score,
        "classification": classification,
        "reasons": reasons,
    }


def summarize_articles(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = group_entries(entries, lambda item: str(item.get("article_slug") or "unknown"))
    baseline = build_global_baseline(entries)
    results: list[dict[str, Any]] = []

    for article_slug, group_entries_list in grouped.items():
        metrics = build_metrics_snapshot(group_entries_list)
        article_record = {
            "article_slug": article_slug,
            **metrics,
        }
        article_record.update(score_article(article_record, baseline))
        results.append(article_record)

    results.sort(
        key=lambda item: (
            item.get("score") or 0,
            item.get("average_outbound_clicks") or 0,
            item.get("average_saves") or 0,
            item.get("average_impressions") or 0,
        ),
        reverse=True,
    )
    return results


def top_slice(items: list[dict[str, Any]], size: int = 5) -> list[dict[str, Any]]:
    return items[:size]


def weak_articles(items: list[dict[str, Any]], size: int = 5) -> list[dict[str, Any]]:
    ordered = sorted(
        items,
        key=lambda item: (
            item.get("score") or 0,
            item.get("average_outbound_clicks") or 0,
            item.get("average_saves") or 0,
            item.get("average_impressions") or 0,
        ),
    )
    return ordered[:size]


def build_performance_summary(history_path: Path, summary_path: Path, article_scores_path: Path) -> dict[str, Path]:
    print("[pinterest] building performance summary")
    history = load_history(history_path)
    published_entries = [entry for entry in history if is_published_entry(entry)]

    print("[pinterest] aggregating by variant type")
    variant_summary = build_group_summary(
        published_entries,
        key_name="variant_type",
        key_fn=lambda item: str(item.get("variant_type") or "unknown"),
    )

    print("[pinterest] aggregating by board")
    board_summary = build_group_summary(
        published_entries,
        key_name="board_key",
        key_fn=lambda item: str((item.get("board") or {}).get("key") or "default"),
        extra_fn=lambda items: {
            "board_name": str((items[0].get("board") or {}).get("name") or "Default Board")
        },
    )

    print("[pinterest] aggregating by article")
    article_summary = summarize_articles(published_entries)

    summary_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "history_path": str(history_path),
        "published_pin_count": len(published_entries),
        "global_metrics": build_metrics_snapshot(published_entries),
        "performance_by_variant_type": variant_summary,
        "performance_by_board": board_summary,
        "performance_by_article": article_summary,
        "rankings": {
            "top_variant_types": top_slice(variant_summary),
            "top_boards": top_slice(board_summary),
            "top_articles": top_slice(article_summary),
            "weak_articles": weak_articles(article_summary),
        },
    }

    article_scores_payload = {
        "generated_at": summary_payload["generated_at"],
        "articles": [
            {
                "article_slug": item["article_slug"],
                "score": item["score"],
                "classification": item["classification"],
                "reasons": item["reasons"],
                "average_impressions": item["average_impressions"],
                "average_saves": item["average_saves"],
                "average_outbound_clicks": item["average_outbound_clicks"],
                "average_engagement_rate": item["average_engagement_rate"],
                "published_pins": item["published_pins"],
            }
            for item in article_summary
        ],
    }

    print("[pinterest] writing summary output")
    save_json(summary_path, summary_payload)
    save_json(article_scores_path, article_scores_payload)

    return {
        "summary_path": summary_path,
        "article_scores_path": article_scores_path,
    }


def main() -> int:
    args = parse_args()
    try:
        result = build_performance_summary(
            history_path=Path(args.history_path),
            summary_path=Path(args.summary_path),
            article_scores_path=Path(args.article_scores_path),
        )
        print(result["summary_path"])
        print(result["article_scores_path"])
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
