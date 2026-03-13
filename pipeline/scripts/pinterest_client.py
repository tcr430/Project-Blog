from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

API_BASE_URL = "https://api.pinterest.com/v5"
DEFAULT_ANALYTICS_METRICS = ["IMPRESSION", "OUTBOUND_CLICK", "SAVE", "PIN_CLICK"]


@dataclass
class PinterestClient:
    mode: str
    access_token: str | None = None
    default_board_id: str | None = None
    board_ids: dict[str, str] = field(default_factory=dict)
    api_base_url: str = API_BASE_URL

    @classmethod
    def from_env(cls, project_root: Path) -> "PinterestClient":
        load_dotenv(project_root / ".env")
        mode = os.getenv("PINTEREST_MODE", "queue").strip().lower() or "queue"
        board_ids: dict[str, str] = {}

        for env_name, env_value in os.environ.items():
            if not env_name.startswith("PINTEREST_BOARD_ID_"):
                continue

            board_id = env_value.strip()
            if not board_id:
                continue

            board_key = env_name.removeprefix("PINTEREST_BOARD_ID_").strip().lower().replace("_", "-")
            if board_key:
                board_ids[board_key] = board_id

        return cls(
            mode=mode,
            access_token=(os.getenv("PINTEREST_ACCESS_TOKEN") or "").strip() or None,
            default_board_id=(os.getenv("PINTEREST_BOARD_ID") or "").strip() or None,
            board_ids=board_ids,
        )

    def should_attempt_publish(self) -> bool:
        return self.mode == "publish"

    def is_configured_for_publish(self) -> bool:
        return bool(self.access_token and (self.default_board_id or self.board_ids))

    def is_configured_for_analytics(self) -> bool:
        return bool(self.access_token)

    def resolve_board_id(self, board_key: str) -> str | None:
        normalized_key = board_key.strip().lower().replace("_", "-")
        if normalized_key in self.board_ids:
            return self.board_ids[normalized_key]
        return self.default_board_id

    def build_pin_payload(
        self,
        *,
        board_key: str,
        title: str,
        description: str,
        article_url: str,
        image_url: str,
    ) -> dict[str, Any]:
        board_id = self.resolve_board_id(board_key)
        if not board_id:
            raise RuntimeError(
                "Pinterest board ID is not configured. Set PINTEREST_BOARD_ID or a board-specific "
                "variable like PINTEREST_BOARD_ID_DECOR_TRENDS."
            )

        return {
            "board_id": board_id,
            "title": title,
            "description": description,
            "link": article_url,
            "media_source": {
                "source_type": "image_url",
                "url": image_url,
            },
        }

    def api_request(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.access_token:
            raise RuntimeError("PINTEREST_ACCESS_TOKEN is not configured.")

        url = f"{self.api_base_url}{path}"
        if query:
            normalized_query: list[tuple[str, str]] = []
            for key, value in query.items():
                if value is None:
                    continue
                if isinstance(value, (list, tuple)):
                    normalized_query.append((key, ",".join(str(item) for item in value)))
                else:
                    normalized_query.append((key, str(value)))
            if normalized_query:
                url = f"{url}?{urllib.parse.urlencode(normalized_query)}"

        request = urllib.request.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8") if payload is not None else None,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method=method.upper(),
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Pinterest API error ({exc.code}): {body or exc.reason}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Pinterest connection error: {exc.reason}") from exc

        try:
            response_payload = json.loads(body)
        except json.JSONDecodeError:
            response_payload = {"raw_response": body}

        if not isinstance(response_payload, dict):
            raise RuntimeError("Pinterest API returned an unexpected response format.")

        return response_payload

    def publish_variant(
        self,
        *,
        board_key: str,
        title: str,
        description: str,
        article_url: str,
        image_url: str,
    ) -> dict[str, Any]:
        payload = self.build_pin_payload(
            board_key=board_key,
            title=title,
            description=description,
            article_url=article_url,
            image_url=image_url,
        )
        return self.api_request(method="POST", path="/pins", payload=payload)

    def fetch_pin_analytics(
        self,
        *,
        pin_id: str,
        start_date: str,
        end_date: str,
        metric_types: list[str] | None = None,
    ) -> dict[str, Any]:
        if not pin_id.strip():
            raise RuntimeError("Pin analytics require a non-empty provider pin ID.")

        return self.api_request(
            method="GET",
            path=f"/pins/{pin_id.strip()}/analytics",
            query={
                "start_date": start_date,
                "end_date": end_date,
                "app_types": "ALL",
                "metric_types": metric_types or DEFAULT_ANALYTICS_METRICS,
                "split_field": "NO_SPLIT",
            },
        )
