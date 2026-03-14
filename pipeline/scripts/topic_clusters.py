from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, TypedDict


class TopicCluster(TypedDict):
    cluster_name: str
    keywords: list[str]
    season: str
    holiday: str
    source: str


class TopicCandidate(TypedDict):
    trend_cluster: str
    trend_keyword: str
    primary_keyword: str
    secondary_keywords: list[str]
    cluster_keywords: list[str]
    search_intent: str
    season: str
    holiday: str
    source: str


TOPIC_CLUSTERS_PATH = Path(__file__).resolve().parents[1] / "data" / "topic_clusters.json"

BUILTIN_TOPIC_CLUSTERS: list[TopicCluster] = [
    {
        "cluster_name": "neutral living room",
        "keywords": [
            "neutral living room ideas",
            "how to style a neutral living room",
            "neutral living room decor",
            "warm neutral living room furniture",
        ],
        "season": "",
        "holiday": "",
        "source": "builtin_cluster",
    },
    {
        "cluster_name": "small dining nook",
        "keywords": [
            "small apartment dining nook ideas",
            "how to make a dining nook feel cozy",
            "small dining nook decor",
            "best layout for a small dining nook",
        ],
        "season": "",
        "holiday": "",
        "source": "builtin_cluster",
    },
    {
        "cluster_name": "organic modern living room",
        "keywords": [
            "organic modern living room ideas",
            "how to style an organic modern living room",
            "organic modern living room decor",
            "mistakes to avoid in an organic modern living room",
        ],
        "season": "",
        "holiday": "",
        "source": "builtin_cluster",
    },
    {
        "cluster_name": "linen and boucle styling",
        "keywords": [
            "linen and boucle living room ideas",
            "how to mix linen and boucle textures",
            "linen and boucle decor ideas",
            "boucle styling mistakes to avoid",
        ],
        "season": "",
        "holiday": "",
        "source": "builtin_cluster",
    },
    {
        "cluster_name": "entryway storage styling",
        "keywords": [
            "minimalist entryway storage ideas",
            "how to style a small entryway",
            "entryway storage decor ideas",
            "best entryway storage for small spaces",
        ],
        "season": "",
        "holiday": "",
        "source": "builtin_cluster",
    },
    {
        "cluster_name": "spring mantel styling",
        "keywords": [
            "spring mantel styling ideas",
            "how to decorate a mantel for spring",
            "spring mantel decor ideas",
            "simple spring mantel styling tips",
        ],
        "season": "spring",
        "holiday": "",
        "source": "builtin_cluster",
    },
]


def normalize_text(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", normalized)


def classify_search_intent(keyword: str) -> str:
    normalized = normalize_text(keyword)
    if normalized.startswith("how to "):
        return "how_to"
    if any(token in normalized for token in {"mistake", "avoid", "fix"}):
        return "problem_solution"
    if any(token in normalized for token in {"best", "vs", "comparison", "compare"}):
        return "comparison"
    if any(token in normalized for token in {"idea", "inspiration"}):
        return "ideas"
    return "styling_advice"


def normalize_cluster(raw: dict[str, Any], source: str) -> TopicCluster:
    cluster_name = normalize_text(raw.get("cluster_name") or raw.get("trend_cluster") or "")
    keywords_raw = raw.get("keywords", [])
    if isinstance(keywords_raw, str):
        keywords = [normalize_text(item) for item in keywords_raw.split(",") if normalize_text(item)]
    elif isinstance(keywords_raw, list):
        keywords = [normalize_text(item) for item in keywords_raw if normalize_text(item)]
    else:
        keywords = []

    if not cluster_name:
        raise ValueError("Topic cluster is missing cluster_name.")
    if not keywords:
        raise ValueError(f"Topic cluster '{cluster_name}' must include at least one keyword.")

    deduped_keywords: list[str] = []
    seen: set[str] = set()
    for keyword in keywords:
        if keyword in seen:
            continue
        seen.add(keyword)
        deduped_keywords.append(keyword)

    return {
        "cluster_name": cluster_name,
        "keywords": deduped_keywords,
        "season": normalize_text(raw.get("season", "")),
        "holiday": normalize_text(raw.get("holiday", "")),
        "source": normalize_text(raw.get("source", "")) or source,
    }


def load_topic_clusters_from_file(path: Path) -> list[TopicCluster]:
    if not path.exists():
        raise FileNotFoundError(f"Topic clusters file not found: {path}")

    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, list):
        raise ValueError("Topic clusters file must contain a JSON array.")

    return [normalize_cluster(item, source="file") for item in raw if isinstance(item, dict)]


def load_default_topic_clusters() -> list[TopicCluster]:
    if TOPIC_CLUSTERS_PATH.exists():
        return load_topic_clusters_from_file(TOPIC_CLUSTERS_PATH)
    return [normalize_cluster(item, source="builtin_cluster") for item in BUILTIN_TOPIC_CLUSTERS]


def build_topic_candidate(
    *,
    cluster_name: str,
    primary_keyword: str,
    all_keywords: list[str],
    season: str = "",
    holiday: str = "",
    source: str = "cluster",
) -> TopicCandidate:
    normalized_primary = normalize_text(primary_keyword)
    normalized_cluster = normalize_text(cluster_name) or normalized_primary
    normalized_keywords = [normalize_text(item) for item in all_keywords if normalize_text(item)]

    cluster_keywords: list[str] = []
    seen: set[str] = set()
    for keyword in normalized_keywords:
        if keyword in seen:
            continue
        seen.add(keyword)
        cluster_keywords.append(keyword)

    secondary_keywords = [keyword for keyword in cluster_keywords if keyword != normalized_primary][:4]

    return {
        "trend_cluster": normalized_cluster,
        "trend_keyword": normalized_primary,
        "primary_keyword": normalized_primary,
        "secondary_keywords": secondary_keywords,
        "cluster_keywords": cluster_keywords,
        "search_intent": classify_search_intent(normalized_primary),
        "season": normalize_text(season),
        "holiday": normalize_text(holiday),
        "source": normalize_text(source) or "cluster",
    }


def expand_clusters_to_candidates(clusters: list[TopicCluster]) -> list[TopicCandidate]:
    candidates: list[TopicCandidate] = []
    for cluster in clusters:
        for keyword in cluster["keywords"]:
            candidates.append(
                build_topic_candidate(
                    cluster_name=cluster["cluster_name"],
                    primary_keyword=keyword,
                    all_keywords=cluster["keywords"],
                    season=cluster["season"],
                    holiday=cluster["holiday"],
                    source=cluster["source"],
                )
            )
    return candidates


def build_manual_topic_candidate(trend: str) -> TopicCandidate:
    normalized_trend = normalize_text(trend)
    if not normalized_trend:
        raise ValueError("Trend cannot be empty.")

    generated_keywords = [
        normalized_trend,
        f"{normalized_trend} ideas",
        f"how to style {normalized_trend}",
        f"{normalized_trend} decor",
        f"{normalized_trend} mistakes to avoid",
    ]
    cluster_name = re.sub(r"\b(ideas|decor|style|styling|mistakes|avoid|how|to)\b", "", normalized_trend)
    cluster_name = re.sub(r"\s+", " ", cluster_name).strip() or normalized_trend

    return build_topic_candidate(
        cluster_name=cluster_name,
        primary_keyword=normalized_trend,
        all_keywords=generated_keywords,
        source="manual",
    )
