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
TOPIC_TAXONOMY_PATH = Path(__file__).resolve().parents[1] / "data" / "topic_taxonomy.json"

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


def load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise ValueError(f"Topic taxonomy file must contain a JSON object: {path}")
    return raw


def build_standard_cluster_keywords(base_phrase: str) -> list[str]:
    normalized_base = normalize_text(base_phrase)
    return [
        f"{normalized_base} ideas",
        f"how to style {normalized_base}",
        f"{normalized_base} decor",
        f"{normalized_base} mistakes to avoid",
    ]


def build_feature_cluster_keywords(room: str, feature: str) -> list[str]:
    normalized_room = normalize_text(room)
    normalized_feature = normalize_text(feature)
    return [
        f"{normalized_room} {normalized_feature} ideas",
        f"how to style {normalized_feature} in {normalized_room}",
        f"best {normalized_feature} for {normalized_room}",
        f"{normalized_room} {normalized_feature} mistakes to avoid",
    ]


def load_topic_taxonomy(path: Path = TOPIC_TAXONOMY_PATH) -> dict[str, Any]:
    return load_json_object(path)


def generate_taxonomy_clusters(taxonomy: dict[str, Any]) -> list[TopicCluster]:
    generated: list[TopicCluster] = []

    style_room_pairs = taxonomy.get("style_room_pairs", {})
    if isinstance(style_room_pairs, dict):
        styles = [normalize_text(item) for item in style_room_pairs.get("styles", []) if normalize_text(item)]
        rooms = [normalize_text(item) for item in style_room_pairs.get("rooms", []) if normalize_text(item)]
        for style in styles:
            for room in rooms:
                generated.append(
                    normalize_cluster(
                        {
                            "cluster_name": f"{style} {room}",
                            "keywords": build_standard_cluster_keywords(f"{style} {room}"),
                            "season": "",
                            "holiday": "",
                            "source": "taxonomy_style_room",
                        },
                        source="taxonomy_style_room",
                    )
                )

    feature_room_pairs = taxonomy.get("feature_room_pairs", {})
    if isinstance(feature_room_pairs, dict):
        features = [normalize_text(item) for item in feature_room_pairs.get("features", []) if normalize_text(item)]
        rooms = [normalize_text(item) for item in feature_room_pairs.get("rooms", []) if normalize_text(item)]
        for room in rooms:
            for feature in features:
                generated.append(
                    normalize_cluster(
                        {
                            "cluster_name": f"{room} {feature}",
                            "keywords": build_feature_cluster_keywords(room, feature),
                            "season": "",
                            "holiday": "",
                            "source": "taxonomy_feature_room",
                        },
                        source="taxonomy_feature_room",
                    )
                )

    material_room_pairs = taxonomy.get("material_room_pairs", {})
    if isinstance(material_room_pairs, dict):
        mixes = [normalize_text(item) for item in material_room_pairs.get("material_mixes", []) if normalize_text(item)]
        rooms = [normalize_text(item) for item in material_room_pairs.get("rooms", []) if normalize_text(item)]
        for mix in mixes:
            for room in rooms:
                generated.append(
                    normalize_cluster(
                        {
                            "cluster_name": f"{mix} {room}",
                            "keywords": build_standard_cluster_keywords(f"{mix} {room}"),
                            "season": "",
                            "holiday": "",
                            "source": "taxonomy_material_room",
                        },
                        source="taxonomy_material_room",
                    )
                )

    color_room_pairs = taxonomy.get("color_room_pairs", {})
    if isinstance(color_room_pairs, dict):
        palettes = [normalize_text(item) for item in color_room_pairs.get("palettes", []) if normalize_text(item)]
        rooms = [normalize_text(item) for item in color_room_pairs.get("rooms", []) if normalize_text(item)]
        for palette in palettes:
            for room in rooms:
                generated.append(
                    normalize_cluster(
                        {
                            "cluster_name": f"{palette} {room}",
                            "keywords": build_standard_cluster_keywords(f"{palette} {room}"),
                            "season": "",
                            "holiday": "",
                            "source": "taxonomy_color_room",
                        },
                        source="taxonomy_color_room",
                    )
                )

    small_space_clusters = taxonomy.get("small_space_clusters", [])
    if isinstance(small_space_clusters, list):
        for cluster_name in small_space_clusters:
            normalized_cluster_name = normalize_text(cluster_name)
            if not normalized_cluster_name:
                continue
            generated.append(
                normalize_cluster(
                    {
                        "cluster_name": normalized_cluster_name,
                        "keywords": build_standard_cluster_keywords(normalized_cluster_name),
                        "season": "",
                        "holiday": "",
                        "source": "taxonomy_small_space",
                    },
                    source="taxonomy_small_space",
                )
            )

    seasonal_clusters = taxonomy.get("seasonal_clusters", [])
    if isinstance(seasonal_clusters, list):
        for item in seasonal_clusters:
            if not isinstance(item, dict):
                continue
            cluster_name = normalize_text(item.get("cluster_name", ""))
            if not cluster_name:
                continue
            generated.append(
                normalize_cluster(
                    {
                        "cluster_name": cluster_name,
                        "keywords": build_standard_cluster_keywords(cluster_name),
                        "season": normalize_text(item.get("season", "")),
                        "holiday": normalize_text(item.get("holiday", "")),
                        "source": "taxonomy_seasonal",
                    },
                    source="taxonomy_seasonal",
                )
            )

    deduped_clusters: list[TopicCluster] = []
    seen_cluster_names: set[str] = set()
    for cluster in generated:
        if cluster["cluster_name"] in seen_cluster_names:
            continue
        seen_cluster_names.add(cluster["cluster_name"])
        deduped_clusters.append(cluster)

    return deduped_clusters


def load_default_topic_clusters() -> list[TopicCluster]:
    curated_clusters = (
        load_topic_clusters_from_file(TOPIC_CLUSTERS_PATH)
        if TOPIC_CLUSTERS_PATH.exists()
        else [normalize_cluster(item, source="builtin_cluster") for item in BUILTIN_TOPIC_CLUSTERS]
    )
    taxonomy = load_topic_taxonomy()
    generated_clusters = generate_taxonomy_clusters(taxonomy)

    merged_clusters: list[TopicCluster] = []
    seen_cluster_names: set[str] = set()
    for cluster in [*curated_clusters, *generated_clusters]:
        if cluster["cluster_name"] in seen_cluster_names:
            continue
        seen_cluster_names.add(cluster["cluster_name"])
        merged_clusters.append(cluster)

    return merged_clusters


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
