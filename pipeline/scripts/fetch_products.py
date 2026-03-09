from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, TypedDict

from dotenv import load_dotenv

from product_providers.amazon_provider import (
    AmazonProviderError,
    AmazonProductProvider,
    create_amazon_provider_from_env,
    should_fallback_to_mock,
)


class Product(TypedDict):
    title: str
    affiliate_url: str
    image_url: str
    short_reason: str
    price: str | None
    source: str
    # Backward-compatibility key used by current article-generation validation logic.
    reason_for_recommendation: str


class ProductProvider(Protocol):
    """Provider contract for fetching products for a trend."""

    provider_name: str

    def fetch_products(self, trend: str, limit: int) -> list[Product]:
        ...


class ProductFetchResult(TypedDict):
    products: list[Product]
    requested_provider: str
    resolved_provider: str
    used_fallback: bool

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "pipeline" / "data"
DEFAULT_MOCK_CATALOG_PATH = DATA_DIR / "mock_products.json"
DEFAULT_CACHE_PATH = DATA_DIR / "products_cache.json"
SUPPORTED_PROVIDERS = {"mock", "amazon"}

DECOR_RELEVANCE_TERMS = {
    "decor",
    "home",
    "interior",
    "living",
    "room",
    "kitchen",
    "bathroom",
    "bedroom",
    "dining",
    "table",
    "shelf",
    "vase",
    "rug",
    "lamp",
    "throw",
    "pillow",
    "basket",
    "wall",
    "art",
    "curtain",
    "planter",
}

MATERIAL_TERMS = {
    "wood",
    "linen",
    "cotton",
    "ceramic",
    "stone",
    "rattan",
    "bamboo",
    "jute",
    "brass",
    "glass",
    "marble",
    "wool",
    "boucle",
    "metal",
}

JUNK_TERMS = {
    "phone",
    "charger",
    "cable",
    "screen protector",
    "earbuds",
    "headphones",
    "battery",
    "usb",
    "car mount",
    "tripod",
    "game",
    "controller",
    "laptop",
    "smartwatch",
}


BUILTIN_MOCK_PRODUCT_CATALOG: list[dict[str, str]] = [
    {
        "title": "Handcrafted Terracotta Vase Set",
        "affiliate_url": "https://www.amazon.com/dp/B0TERRA001?tag=decorblog-20",
        "image_url": "https://images-na.ssl-images-amazon.com/images/I/terracotta-vase.jpg",
        "short_reason": "Adds warm earthy color and organic shape to shelves or counters.",
    },
    {
        "title": "Linen Textured Throw Pillow Covers",
        "affiliate_url": "https://www.amazon.com/dp/B0LINEN002?tag=decorblog-20",
        "image_url": "https://images-na.ssl-images-amazon.com/images/I/linen-pillows.jpg",
        "short_reason": "Softens hard surfaces and layers texture without visual clutter.",
    },
    {
        "title": "Matte Ceramic Pendant Light",
        "affiliate_url": "https://www.amazon.com/dp/B0LIGHT003?tag=decorblog-20",
        "image_url": "https://images-na.ssl-images-amazon.com/images/I/ceramic-light.jpg",
        "short_reason": "Creates a focal point while reinforcing the trend's material palette.",
    },
    {
        "title": "Natural Jute Area Rug",
        "affiliate_url": "https://www.amazon.com/dp/B0RUG004?tag=decorblog-20",
        "image_url": "https://images-na.ssl-images-amazon.com/images/I/jute-rug.jpg",
        "short_reason": "Grounds the room with natural texture and helps tie furniture together.",
    },
    {
        "title": "Wood and Metal Accent Shelf",
        "affiliate_url": "https://www.amazon.com/dp/B0SHELF005?tag=decorblog-20",
        "image_url": "https://images-na.ssl-images-amazon.com/images/I/accent-shelf.jpg",
        "short_reason": "Displays decor vertically and balances storage with styling.",
    },
    {
        "title": "Woven Storage Basket Set",
        "affiliate_url": "https://www.amazon.com/dp/B0BASKET006?tag=decorblog-20",
        "image_url": "https://images-na.ssl-images-amazon.com/images/I/woven-baskets.jpg",
        "short_reason": "Keeps everyday items tidy while adding visual warmth.",
    },
    {
        "title": "Brushed Brass Table Lamp",
        "affiliate_url": "https://www.amazon.com/dp/B0LAMP007?tag=decorblog-20",
        "image_url": "https://images-na.ssl-images-amazon.com/images/I/brass-lamp.jpg",
        "short_reason": "Introduces a subtle metallic accent that elevates the palette.",
    },
    {
        "title": "Stoneware Dinnerware Set",
        "affiliate_url": "https://www.amazon.com/dp/B0STONE008?tag=decorblog-20",
        "image_url": "https://images-na.ssl-images-amazon.com/images/I/stoneware-set.jpg",
        "short_reason": "Makes everyday table styling feel cohesive and trend-aligned.",
    },
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _debug_enabled() -> bool:
    return _normalize_text(os.getenv("PRODUCT_DEBUG")).lower() in {"1", "true", "yes", "on"}


def _debug_log(message: str) -> None:
    if _debug_enabled():
        print(f"[fetch_products:debug] {message}")


def _normalize_product(raw: dict[str, Any], source: str) -> Product:
    title = _normalize_text(raw.get("title"))
    affiliate_url = _normalize_text(raw.get("affiliate_url"))
    image_url = _normalize_text(raw.get("image_url"))
    short_reason = _normalize_text(raw.get("short_reason")) or _normalize_text(
        raw.get("reason_for_recommendation")
    )
    price_raw = raw.get("price")
    price = _normalize_text(price_raw) if price_raw is not None and str(price_raw).strip() else None

    if not title:
        raise ValueError("Product title cannot be empty.")
    if not affiliate_url.startswith("http"):
        raise ValueError(f"Invalid affiliate_url: {affiliate_url}")
    if not image_url.startswith("http"):
        raise ValueError(f"Invalid image_url: {image_url}")
    if not short_reason:
        raise ValueError("Product short_reason cannot be empty.")

    resolved_source = _normalize_text(raw.get("source")) or source
    return {
        "title": title,
        "affiliate_url": affiliate_url,
        "image_url": image_url,
        "short_reason": short_reason,
        "price": price,
        "source": resolved_source,
        "reason_for_recommendation": short_reason,
    }


def _load_mock_catalog(catalog_path: Path | None = None) -> list[dict[str, Any]]:
    path = catalog_path or DEFAULT_MOCK_CATALOG_PATH
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("Mock catalog file must contain a JSON array.")
        return [item for item in data if isinstance(item, dict)]

    return [dict(item) for item in BUILTIN_MOCK_PRODUCT_CATALOG]


def _start_index_for_trend(trend: str, size: int) -> int:
    trend_key = trend.strip().lower()
    if not trend_key:
        return 0

    digest = hashlib.sha256(trend_key.encode("utf-8")).hexdigest()
    return int(digest, 16) % size


def _is_candidate_usable(product: Product) -> bool:
    title = product["title"].strip().lower()
    reason = product["short_reason"].strip().lower()
    image_url = product["image_url"].strip().lower()
    affiliate_url = product["affiliate_url"].strip().lower()

    if len(_tokenize(title)) < 2:
        return False
    if len(title) < 6 or len(reason) < 12:
        return False
    if any(term in title for term in JUNK_TERMS):
        return False
    if "placeholder" in image_url:
        return False
    if not ("amazon." in affiliate_url or affiliate_url.startswith("http")):
        return False

    return True


def _score_candidate(product: Product, trend: str) -> tuple[int, list[str]]:
    title_tokens = set(_tokenize(product["title"]))
    reason_tokens = set(_tokenize(product["short_reason"]))
    trend_tokens = set(_tokenize(trend))

    searchable_tokens = title_tokens | reason_tokens

    score = 0
    notes: list[str] = []

    overlap = len(searchable_tokens & trend_tokens)
    overlap_points = min(35, overlap * 7)
    score += overlap_points
    notes.append(f"trend overlap +{overlap_points}")

    decor_hits = len(searchable_tokens & DECOR_RELEVANCE_TERMS)
    decor_points = min(25, decor_hits * 5)
    score += decor_points
    notes.append(f"decor relevance +{decor_points}")

    material_hits = len(searchable_tokens & MATERIAL_TERMS)
    material_points = min(20, material_hits * 5)
    score += material_points
    notes.append(f"material/style detail +{material_points}")

    if "/dp/" in product["affiliate_url"]:
        score += 8
        notes.append("clean affiliate path +8")

    if len(product["short_reason"]) >= 45:
        score += 7
        notes.append("useful short_reason +7")

    if any(word in product["title"].lower() for word in ("set", "pair")):
        score += 5
        notes.append("recommendation-friendly format +5")

    return score, notes


def _select_best_products(products: list[Product], trend: str, limit: int) -> list[Product]:
    usable = [product for product in products if _is_candidate_usable(product)]
    _debug_log(f"usable products after hard filters: {len(usable)}/{len(products)}")

    scored: list[tuple[int, Product, list[str]]] = []
    for product in usable:
        score, notes = _score_candidate(product, trend=trend)
        scored.append((score, product, notes))

    scored.sort(key=lambda item: (-item[0], item[1]["title"].lower()))

    deduped: list[Product] = []
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()

    for score, product, notes in scored:
        url_key = product["affiliate_url"].strip().lower()
        title_key = product["title"].strip().lower()
        if url_key in seen_urls or title_key in seen_titles:
            continue

        seen_urls.add(url_key)
        seen_titles.add(title_key)
        deduped.append(product)
        _debug_log(
            f"selected ({product['source']}): {product['title']} | score={score} | {'; '.join(notes)}"
        )

        if len(deduped) >= limit:
            break

    return deduped


def _cache_enabled() -> bool:
    value = _normalize_text(os.getenv("PRODUCT_CACHE_ENABLED"))
    if not value:
        return True
    return value.lower() in {"1", "true", "yes", "on"}


def _cache_ttl_minutes() -> int:
    raw = _normalize_text(os.getenv("PRODUCT_CACHE_TTL_MINUTES"))
    if not raw:
        return 180
    try:
        return max(1, int(raw))
    except ValueError:
        return 180


def _cache_key(provider_name: str, trend: str, limit: int) -> str:
    return f"{provider_name.lower()}::{trend.strip().lower()}::{limit}"


def _load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return raw if isinstance(raw, dict) else {}


def _save_cache(path: Path, cache: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_cached_products(cache_path: Path, key: str) -> list[Product] | None:
    if not _cache_enabled():
        return None

    cache = _load_cache(cache_path)
    payload = cache.get(key)
    if not isinstance(payload, dict):
        return None

    ts_raw = _normalize_text(payload.get("saved_at"))
    items = payload.get("products")
    if not ts_raw or not isinstance(items, list):
        return None

    try:
        saved_at = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        if saved_at.tzinfo is None:
            saved_at = saved_at.replace(tzinfo=UTC)
    except ValueError:
        return None

    expires_at = saved_at + timedelta(minutes=_cache_ttl_minutes())
    if datetime.now(UTC) > expires_at:
        return None

    try:
        return [_normalize_product(dict(item), source=_normalize_text(item.get("source")) or "cache") for item in items]
    except Exception:
        return None


def _set_cached_products(cache_path: Path, key: str, products: list[Product]) -> None:
    if not _cache_enabled():
        return

    cache = _load_cache(cache_path)
    cache[key] = {
        "saved_at": datetime.now(UTC).isoformat(),
        "products": products,
    }
    _save_cache(cache_path, cache)


class MockProductProvider:
    provider_name = "mock"

    def __init__(self, catalog_path: Path | None = None) -> None:
        self.catalog_path = catalog_path

    def fetch_products(self, trend: str, limit: int) -> list[Product]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        catalog = _load_mock_catalog(self.catalog_path)
        if not catalog:
            raise RuntimeError("Mock product catalog is empty.")

        selected: list[Product] = []
        start = _start_index_for_trend(trend=trend, size=len(catalog))

        # Fetch extra candidates so ranking has room.
        fetch_count = min(max(limit * 3, limit), len(catalog))
        for offset in range(fetch_count):
            idx = (start + offset) % len(catalog)
            selected.append(_normalize_product(catalog[idx], source=self.provider_name))

        ranked = _select_best_products(selected, trend=trend, limit=limit)
        if len(ranked) >= limit:
            return ranked

        # Fill from normalized candidates if ranking is too strict for a small mock catalog.
        filled: list[Product] = list(ranked)
        seen_urls = {item["affiliate_url"].strip().lower() for item in filled}
        for candidate in selected:
            url_key = candidate["affiliate_url"].strip().lower()
            if url_key in seen_urls:
                continue
            seen_urls.add(url_key)
            filled.append(candidate)
            if len(filled) >= limit:
                break

        if len(filled) < limit:
            raise RuntimeError("Mock product catalog cannot provide enough products.")

        return filled[:limit]


def _load_env() -> None:
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(env_path)


def _resolve_fallback_setting(allow_fallback_to_mock: bool | None) -> bool:
    if allow_fallback_to_mock is not None:
        return allow_fallback_to_mock
    return should_fallback_to_mock(default=True)


def resolve_provider(
    provider_name: str | None = None,
    catalog_path: Path | None = None,
    allow_fallback_to_mock: bool | None = None,
) -> ProductProvider:
    _load_env()
    fallback_allowed = _resolve_fallback_setting(allow_fallback_to_mock)
    requested = _normalize_text(provider_name or os.getenv("PRODUCT_PROVIDER") or "mock").lower()

    if requested not in SUPPORTED_PROVIDERS:
        message = (
            f"[fetch_products] Unknown provider '{requested}'. "
            "Supported: mock, amazon."
        )
        if fallback_allowed:
            print(f"{message} Falling back to mock provider.")
            requested = "mock"
        else:
            raise ValueError(message)

    if requested == "amazon":
        try:
            provider: AmazonProductProvider = create_amazon_provider_from_env()
            print("[fetch_products] Using Amazon provider.")
            return provider
        except AmazonProviderError as exc:
            if not fallback_allowed:
                raise RuntimeError(
                    f"Amazon provider is enabled but unavailable: {exc}"
                ) from exc

            print(
                f"[fetch_products] Amazon provider unavailable ({exc}). "
                "Falling back to mock provider."
            )

    return MockProductProvider(catalog_path=catalog_path)


def fetch_products_for_trend(
    trend: str,
    limit: int = 5,
    provider_name: str | None = None,
    catalog_path: Path | None = None,
    allow_fallback_to_mock: bool | None = None,
) -> list[Product]:
    """Fetch normalized products from the selected provider with optional fallback to mock."""
    provider = resolve_provider(
        provider_name=provider_name,
        catalog_path=catalog_path,
        allow_fallback_to_mock=allow_fallback_to_mock,
    )
    fallback_allowed = _resolve_fallback_setting(allow_fallback_to_mock)

    cache_key = _cache_key(provider.provider_name, trend, limit)
    if provider.provider_name != "mock":
        cached = _get_cached_products(DEFAULT_CACHE_PATH, cache_key)
        if cached:
            _debug_log(f"cache hit for provider={provider.provider_name}, trend='{trend}'")
            ranked_cached = _select_best_products(cached, trend=trend, limit=limit)
            if len(ranked_cached) >= limit:
                return ranked_cached

    try:
        fetch_limit = limit if provider.provider_name == "mock" else max(limit * 3, limit)
        products = provider.fetch_products(trend=trend, limit=fetch_limit)
        if not products:
            raise RuntimeError("Selected provider returned no products.")

        normalized = [_normalize_product(dict(item), source=provider.provider_name) for item in products]
        ranked = _select_best_products(normalized, trend=trend, limit=limit)
        if len(ranked) < limit:
            raise RuntimeError(
                f"Provider '{provider.provider_name}' returned too few high-quality products "
                f"({len(ranked)}/{limit})."
            )

        if provider.provider_name != "mock":
            _set_cached_products(DEFAULT_CACHE_PATH, cache_key, normalized)

        return ranked
    except Exception as exc:
        if provider.provider_name == "mock" or not fallback_allowed:
            raise

        print(
            f"[fetch_products] Provider '{provider.provider_name}' failed ({exc}). "
            "Falling back to mock provider."
        )
        return MockProductProvider(catalog_path=catalog_path).fetch_products(trend=trend, limit=limit)



def fetch_products_with_context(
    trend: str,
    limit: int = 5,
    provider_name: str | None = None,
    catalog_path: Path | None = None,
    allow_fallback_to_mock: bool | None = None,
) -> ProductFetchResult:
    requested = _normalize_text(provider_name or os.getenv("PRODUCT_PROVIDER") or "mock").lower()

    products = fetch_products_for_trend(
        trend=trend,
        limit=limit,
        provider_name=provider_name,
        catalog_path=catalog_path,
        allow_fallback_to_mock=allow_fallback_to_mock,
    )

    first_source = _normalize_text(products[0].get("source") if products else "")
    resolved_provider = "amazon" if first_source.startswith("amazon") else "mock"

    return {
        "products": products,
        "requested_provider": requested or "mock",
        "resolved_provider": resolved_provider,
        "used_fallback": requested == "amazon" and resolved_provider == "mock",
    }


def fetch_mock_products_for_trend(trend: str, limit: int = 5) -> list[Product]:
    """Backward-compatible helper kept for existing callers."""
    return MockProductProvider().fetch_products(trend=trend, limit=limit)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch products for a trend using provider-based routing.")
    parser.add_argument("trend", type=str, help='Trend text, e.g. "terracotta kitchen decor"')
    parser.add_argument("--limit", type=int, default=5, help="Number of products to return (default: 5).")
    parser.add_argument(
        "--provider",
        type=str,
        default=None,
        choices=["mock", "amazon"],
        help="Override provider selection (otherwise uses PRODUCT_PROVIDER env var).",
    )
    parser.add_argument(
        "--mock-catalog",
        type=str,
        default=None,
        help="Optional path to a mock catalog JSON file.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Disable fallback to mock provider and fail if the selected provider fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    trend = args.trend.strip()
    if not trend:
        print("Error: trend cannot be empty.")
        return 1

    try:
        catalog_path = Path(args.mock_catalog) if args.mock_catalog else None
        products = fetch_products_for_trend(
            trend=trend,
            limit=args.limit,
            provider_name=args.provider,
            catalog_path=catalog_path,
            allow_fallback_to_mock=not args.strict,
        )
        print(json.dumps(products, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())





