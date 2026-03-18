from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from validate_article_editorial import (
    validate_article_editorial,
)
from validate_article_seo import (
    DEFAULT_CLUSTER_INDEX_PATH,
    DEFAULT_CLUSTER_REPORT_PATH,
    DEFAULT_TREND_HISTORY_PATH,
    validate_article_seo,
)

DEFAULT_REPORT_PATH = Path(__file__).resolve().parents[1] / "reports" / "article_validation_report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run combined SEO and editorial validation for an article package.")
    parser.add_argument("--package-path", type=str, required=True, help="Path to the article package JSON file.")
    parser.add_argument("--cluster-index-path", type=str, default=str(DEFAULT_CLUSTER_INDEX_PATH))
    parser.add_argument("--cluster-report-path", type=str, default=str(DEFAULT_CLUSTER_REPORT_PATH))
    parser.add_argument("--trend-history-path", type=str, default=str(DEFAULT_TREND_HISTORY_PATH))
    parser.add_argument("--report-path", type=str, default=str(DEFAULT_REPORT_PATH))
    return parser.parse_args()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    raw = path.read_text(encoding="utf-8-sig").strip()
    if not raw:
        return default
    return json.loads(raw)


def combine_status(*statuses: str) -> str:
    if "fail" in statuses:
        return "fail"
    if "warning" in statuses:
        return "warning"
    return "pass"


def main() -> int:
    args = parse_args()
    package = load_json(Path(args.package_path), {})
    if isinstance(package, dict) and isinstance(package.get("package"), dict):
        package = package["package"]

    cluster_index_path = Path(args.cluster_index_path)
    cluster_index_data = load_json(cluster_index_path, {"articles": []})
    cluster_report_data = load_json(Path(args.cluster_report_path), {"clusters": []})
    trend_history_data = load_json(Path(args.trend_history_path), {"entries": []})

    seo_result = validate_article_seo(
        article_package=package,
        existing_index_data=cluster_index_data,
        cluster_report_data=cluster_report_data,
        trend_history_data=trend_history_data,
    )
    editorial_result = validate_article_editorial(
        article_package=package,
        existing_index_data=cluster_index_data,
    )

    payload = {
        "article_slug": seo_result.get("article_slug") or editorial_result.get("article_slug"),
        "validation_status": combine_status(
            str(seo_result.get("validation_status") or "pass"),
            str(editorial_result.get("validation_status") or "pass"),
        ),
        "seo": seo_result,
        "editorial": editorial_result,
    }
    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["validation_status"] != "fail" else 1


if __name__ == "__main__":
    raise SystemExit(main())
