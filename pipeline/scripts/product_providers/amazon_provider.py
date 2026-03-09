from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypedDict
from urllib import error, parse, request


class AmazonProductRecord(TypedDict):
    title: str
    affiliate_url: str
    image_url: str
    short_reason: str
    price: str | None
    source: str


class AmazonProviderError(RuntimeError):
    pass


class AmazonConfigError(AmazonProviderError):
    pass


class AmazonCredentialsError(AmazonProviderError):
    pass


class AmazonMarketplaceError(AmazonProviderError):
    pass


class AmazonRateLimitError(AmazonProviderError):
    pass


class AmazonApiError(AmazonProviderError):
    pass


class AmazonEmptyResultsError(AmazonProviderError):
    pass


@dataclass
class AmazonProviderConfig:
    api_flavor: str
    marketplace: str
    partner_tag: str
    partner_type: str
    access_key: str
    secret_key: str
    host: str
    region: str
    marketplace_domain: str
    timeout_seconds: float
    max_retries: int
    retry_backoff_seconds: float


MARKETPLACE_SETTINGS: dict[str, dict[str, str]] = {
    "us": {"host": "webservices.amazon.com", "region": "us-east-1", "domain": "www.amazon.com"},
    "uk": {"host": "webservices.amazon.co.uk", "region": "eu-west-1", "domain": "www.amazon.co.uk"},
    "de": {"host": "webservices.amazon.de", "region": "eu-west-1", "domain": "www.amazon.de"},
    "fr": {"host": "webservices.amazon.fr", "region": "eu-west-1", "domain": "www.amazon.fr"},
    "it": {"host": "webservices.amazon.it", "region": "eu-west-1", "domain": "www.amazon.it"},
    "es": {"host": "webservices.amazon.es", "region": "eu-west-1", "domain": "www.amazon.es"},
    "ca": {"host": "webservices.amazon.ca", "region": "us-east-1", "domain": "www.amazon.ca"},
    "jp": {"host": "webservices.amazon.co.jp", "region": "us-west-2", "domain": "www.amazon.co.jp"},
}

SUPPORTED_API_FLAVORS = {"paapi_v5", "creators"}


def _get_env(name: str) -> str:
    return str(os.getenv(name, "")).strip()


def _parse_bool(value: str, default: bool) -> bool:
    if not value:
        return default
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def _parse_int(value: str, default: int) -> int:
    try:
        return int(value.strip()) if value.strip() else default
    except ValueError:
        return default


def _parse_float(value: str, default: float) -> float:
    try:
        return float(value.strip()) if value.strip() else default
    except ValueError:
        return default


def should_fallback_to_mock(default: bool = True) -> bool:
    return _parse_bool(_get_env("AMAZON_FALLBACK_TO_MOCK"), default=default)


def load_amazon_config_from_env() -> AmazonProviderConfig:
    api_flavor = (_get_env("AMAZON_API_FLAVOR") or "paapi_v5").lower()
    marketplace = (_get_env("AMAZON_MARKETPLACE") or "us").lower()

    if api_flavor not in SUPPORTED_API_FLAVORS:
        raise AmazonConfigError(
            f"Unsupported AMAZON_API_FLAVOR '{api_flavor}'. "
            f"Use one of: {', '.join(sorted(SUPPORTED_API_FLAVORS))}."
        )

    market = MARKETPLACE_SETTINGS.get(marketplace)
    if not market:
        raise AmazonMarketplaceError(
            f"Unsupported AMAZON_MARKETPLACE '{marketplace}'. "
            f"Use one of: {', '.join(sorted(MARKETPLACE_SETTINGS.keys()))}."
        )

    partner_tag = _get_env("AMAZON_ASSOCIATE_TAG")
    partner_type = _get_env("AMAZON_PARTNER_TYPE") or "Associates"

    access_key = _get_env("AMAZON_PAAPI_ACCESS_KEY")
    secret_key = _get_env("AMAZON_PAAPI_SECRET_KEY")

    timeout_seconds = _parse_float(_get_env("AMAZON_TIMEOUT_SECONDS"), default=12.0)
    max_retries = _parse_int(_get_env("AMAZON_MAX_RETRIES"), default=2)
    retry_backoff_seconds = _parse_float(_get_env("AMAZON_RETRY_BACKOFF_SECONDS"), default=1.5)

    if api_flavor == "paapi_v5":
        if not partner_tag:
            raise AmazonConfigError(
                "Missing AMAZON_ASSOCIATE_TAG for PA-API mode."
            )
        if not access_key or not secret_key:
            raise AmazonConfigError(
                "Missing AMAZON_PAAPI_ACCESS_KEY or AMAZON_PAAPI_SECRET_KEY for PA-API mode."
            )

    if api_flavor == "creators":
        raise AmazonConfigError(
            "AMAZON_API_FLAVOR=creators is planned but not implemented yet in this MVP."
        )

    return AmazonProviderConfig(
        api_flavor=api_flavor,
        marketplace=marketplace,
        partner_tag=partner_tag,
        partner_type=partner_type,
        access_key=access_key,
        secret_key=secret_key,
        host=market["host"],
        region=market["region"],
        marketplace_domain=market["domain"],
        timeout_seconds=max(1.0, timeout_seconds),
        max_retries=max(0, max_retries),
        retry_backoff_seconds=max(0.5, retry_backoff_seconds),
    )


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _get_signature_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    return _sign(k_service, "aws4_request")


def _build_auth_headers(config: AmazonProviderConfig, payload_json: str) -> dict[str, str]:
    amz_target = "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.SearchItems"
    content_type = "application/json; charset=utf-8"
    amz_date = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]

    method = "POST"
    canonical_uri = "/paapi5/searchitems"
    canonical_query = ""
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    canonical_headers = (
        f"content-encoding:amz-1.0\n"
        f"content-type:{content_type}\n"
        f"host:{config.host}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:{amz_target}\n"
    )
    signed_headers = "content-encoding;content-type;host;x-amz-date;x-amz-target"
    canonical_request = (
        f"{method}\n{canonical_uri}\n{canonical_query}\n"
        f"{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )

    service = "ProductAdvertisingAPI"
    credential_scope = f"{date_stamp}/{config.region}/{service}/aws4_request"
    string_to_sign = (
        "AWS4-HMAC-SHA256\n"
        f"{amz_date}\n"
        f"{credential_scope}\n"
        f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    )

    signing_key = _get_signature_key(config.secret_key, date_stamp, config.region, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        "AWS4-HMAC-SHA256 "
        f"Credential={config.access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    return {
        "Content-Encoding": "amz-1.0",
        "Content-Type": content_type,
        "Host": config.host,
        "X-Amz-Date": amz_date,
        "X-Amz-Target": amz_target,
        "Authorization": authorization,
    }


def _extract_display_price(item: dict[str, Any]) -> str | None:
    price = (
        item.get("Offers", {})
        .get("Listings", [{}])[0]
        .get("Price", {})
        .get("DisplayAmount")
    )
    return str(price).strip() if price else None


def _extract_image_url(item: dict[str, Any]) -> str:
    images = item.get("Images", {}).get("Primary", {})
    for size_key in ("Large", "Medium", "Small"):
        url = images.get(size_key, {}).get("URL")
        if isinstance(url, str) and url.strip():
            return url.strip()
    return ""


def _ensure_partner_tag(url: str, partner_tag: str) -> str:
    if not url:
        return ""

    parsed = parse.urlparse(url)
    query = dict(parse.parse_qsl(parsed.query, keep_blank_values=True))
    if "tag" not in query and partner_tag:
        query["tag"] = partner_tag

    rebuilt = parsed._replace(query=parse.urlencode(query))
    return parse.urlunparse(rebuilt)


def _map_paapi_item_to_product(item: dict[str, Any], trend: str, config: AmazonProviderConfig) -> AmazonProductRecord | None:
    title = (
        item.get("ItemInfo", {})
        .get("Title", {})
        .get("DisplayValue", "")
    )
    title_text = str(title).strip()
    if not title_text:
        return None

    detail_page_url = _ensure_partner_tag(str(item.get("DetailPageURL", "")).strip(), config.partner_tag)
    image_url = _extract_image_url(item)
    if not detail_page_url or not image_url:
        return None

    return {
        "title": title_text,
        "affiliate_url": detail_page_url,
        "image_url": image_url,
        "short_reason": f"Fits the '{trend}' decor theme with practical styling potential.",
        "price": _extract_display_price(item),
        "source": f"amazon-{config.api_flavor}",
    }


def _parse_amazon_errors(body: dict[str, Any]) -> str:
    errors = body.get("Errors")
    if isinstance(errors, list) and errors:
        first = errors[0] if isinstance(errors[0], dict) else {}
        code = str(first.get("Code", "UnknownError")).strip()
        message = str(first.get("Message", "Amazon API returned an error.")).strip()
        return f"{code}: {message}"
    return "Amazon API returned an error."


class AmazonProductProvider:
    provider_name = "amazon"

    def __init__(self, config: AmazonProviderConfig) -> None:
        self.config = config

    def fetch_products(self, trend: str, limit: int) -> list[AmazonProductRecord]:
        if limit <= 0:
            raise ValueError("limit must be greater than 0")

        keywords = trend.strip()
        if not keywords:
            raise ValueError("trend cannot be empty")

        payload = {
            "Keywords": keywords,
            "SearchIndex": "HomeGarden",
            "ItemCount": min(limit, 10),
            "PartnerTag": self.config.partner_tag,
            "PartnerType": self.config.partner_type,
            "Marketplace": self.config.marketplace_domain,
            "Resources": [
                "Images.Primary.Small",
                "Images.Primary.Medium",
                "Images.Primary.Large",
                "ItemInfo.Title",
                "Offers.Listings.Price",
            ],
        }

        body = self._request_paapi(payload)
        items = body.get("SearchResult", {}).get("Items", [])
        if not isinstance(items, list) or not items:
            raise AmazonEmptyResultsError(
                f"No Amazon products found for trend '{trend}'."
            )

        normalized: list[AmazonProductRecord] = []
        for raw_item in items:
            if not isinstance(raw_item, dict):
                continue
            mapped = _map_paapi_item_to_product(raw_item, trend=trend, config=self.config)
            if mapped:
                normalized.append(mapped)
            if len(normalized) >= limit:
                break

        if not normalized:
            raise AmazonEmptyResultsError(
                "Amazon returned items, but none had enough data for affiliate use."
            )

        return normalized

    def _request_paapi(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload_json = json.dumps(payload, ensure_ascii=False)
        headers = _build_auth_headers(self.config, payload_json)
        url = f"https://{self.config.host}/paapi5/searchitems"

        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                req = request.Request(
                    url=url,
                    data=payload_json.encode("utf-8"),
                    headers=headers,
                    method="POST",
                )
                with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
                    raw = response.read().decode("utf-8")
                    body = json.loads(raw)
                    if not isinstance(body, dict):
                        raise AmazonApiError("Amazon API returned a non-object payload.")

                    if body.get("Errors"):
                        message = _parse_amazon_errors(body)
                        raise AmazonApiError(message)

                    return body
            except error.HTTPError as http_error:
                status = http_error.code
                raw_error = http_error.read().decode("utf-8", errors="ignore")
                message = raw_error or str(http_error)

                if status in {400, 401, 403}:
                    raise AmazonCredentialsError(
                        "Amazon credentials were rejected. "
                        "Check AMAZON_PAAPI_ACCESS_KEY, AMAZON_PAAPI_SECRET_KEY, and AMAZON_ASSOCIATE_TAG. "
                        f"HTTP {status}."
                    ) from http_error

                if status == 429:
                    last_error = AmazonRateLimitError(
                        "Amazon API rate limit reached (HTTP 429)."
                    )
                elif status >= 500:
                    last_error = AmazonApiError(
                        f"Amazon service error (HTTP {status}). {message}"
                    )
                else:
                    raise AmazonApiError(
                        f"Amazon API request failed with HTTP {status}. {message}"
                    ) from http_error
            except error.URLError as url_error:
                last_error = AmazonApiError(f"Network error contacting Amazon API: {url_error}")
            except json.JSONDecodeError as json_error:
                last_error = AmazonApiError(f"Invalid JSON from Amazon API: {json_error}")

            if attempt < self.config.max_retries:
                sleep_seconds = self.config.retry_backoff_seconds * (attempt + 1)
                print(
                    f"[amazon_provider] Request attempt {attempt + 1} failed. "
                    f"Retrying in {sleep_seconds:.1f}s..."
                )
                time.sleep(sleep_seconds)

        if last_error:
            raise last_error

        raise AmazonApiError("Amazon API request failed without a clear error.")


def create_amazon_provider_from_env() -> AmazonProductProvider:
    config = load_amazon_config_from_env()
    return AmazonProductProvider(config)
