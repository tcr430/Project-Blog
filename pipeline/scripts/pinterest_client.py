from __future__ import annotations

import json
import os
import base64
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

API_BASE_URL = "https://api.pinterest.com/v5"
AUTH_BASE_URL = "https://www.pinterest.com/oauth/"
DEFAULT_ANALYTICS_METRICS = ["IMPRESSION", "OUTBOUND_CLICK", "SAVE", "PIN_CLICK"]
DEFAULT_OAUTH_SCOPES = [
    "boards:read",
    "boards:write",
    "pins:read",
    "pins:write",
    "user_accounts:read",
]


@dataclass
class PinterestClient:
    mode: str
    access_token: str | None = None
    refresh_token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    redirect_uri: str | None = None
    default_board_id: str | None = None
    board_ids: dict[str, str] = field(default_factory=dict)
    api_base_url: str = API_BASE_URL
    auth_base_url: str = AUTH_BASE_URL
    oauth_scopes: list[str] = field(default_factory=lambda: list(DEFAULT_OAUTH_SCOPES))

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
            refresh_token=(os.getenv("PINTEREST_REFRESH_TOKEN") or "").strip() or None,
            client_id=(os.getenv("PINTEREST_CLIENT_ID") or "").strip() or None,
            client_secret=(os.getenv("PINTEREST_CLIENT_SECRET") or "").strip() or None,
            redirect_uri=(os.getenv("PINTEREST_REDIRECT_URI") or "").strip() or None,
            default_board_id=(os.getenv("PINTEREST_BOARD_ID") or "").strip() or None,
            board_ids=board_ids,
            oauth_scopes=cls.parse_scopes(os.getenv("PINTEREST_OAUTH_SCOPES")),
        )

    @staticmethod
    def parse_scopes(raw_value: str | None) -> list[str]:
        if not raw_value:
            return list(DEFAULT_OAUTH_SCOPES)

        scopes = [
            item.strip()
            for item in raw_value.replace("\n", ",").split(",")
            if item.strip()
        ]
        return scopes or list(DEFAULT_OAUTH_SCOPES)

    def should_attempt_publish(self) -> bool:
        return self.mode == "publish"

    def has_oauth_client_credentials(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def can_exchange_authorization_code(self) -> bool:
        return bool(self.has_oauth_client_credentials() and self.redirect_uri)

    def can_refresh_access_token(self) -> bool:
        return bool(self.has_oauth_client_credentials() and self.refresh_token)

    def is_configured_for_publish(self) -> bool:
        return bool((self.access_token or self.can_refresh_access_token()) and (self.default_board_id or self.board_ids))

    def is_configured_for_analytics(self) -> bool:
        return bool(self.access_token or self.can_refresh_access_token())

    def resolve_board_id(self, board_key: str) -> str | None:
        normalized_key = board_key.strip().lower().replace("_", "-")
        if normalized_key in self.board_ids:
            return self.board_ids[normalized_key]
        return self.default_board_id

    def build_authorization_url(self, state: str | None = None) -> str:
        if not self.client_id:
            raise RuntimeError("PINTEREST_CLIENT_ID is not configured.")
        if not self.redirect_uri:
            raise RuntimeError("PINTEREST_REDIRECT_URI is not configured.")

        query = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": ",".join(self.oauth_scopes),
        }
        if state:
            query["state"] = state
        return f"{self.auth_base_url}?{urllib.parse.urlencode(query)}"

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

    def _build_basic_auth_header(self) -> str:
        if not self.has_oauth_client_credentials():
            raise RuntimeError(
                "Pinterest OAuth client credentials are not configured. "
                "Set PINTEREST_CLIENT_ID and PINTEREST_CLIENT_SECRET."
            )
        raw = f"{self.client_id}:{self.client_secret}".encode("utf-8")
        return base64.b64encode(raw).decode("ascii")

    def oauth_token_request(self, payload: dict[str, str]) -> dict[str, Any]:
        request = urllib.request.Request(
            url=f"{self.api_base_url}/oauth/token",
            data=urllib.parse.urlencode(payload).encode("utf-8"),
            headers={
                "Authorization": f"Basic {self._build_basic_auth_header()}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Pinterest OAuth error ({exc.code}): {body or exc.reason}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Pinterest OAuth connection error: {exc.reason}") from exc

        try:
            response_payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Pinterest OAuth token endpoint returned invalid JSON.") from exc

        if not isinstance(response_payload, dict):
            raise RuntimeError("Pinterest OAuth token endpoint returned an unexpected response format.")
        return response_payload

    def exchange_authorization_code(self, code: str) -> dict[str, Any]:
        clean_code = code.strip()
        if not clean_code:
            raise RuntimeError("A non-empty authorization code is required.")
        if not self.can_exchange_authorization_code():
            raise RuntimeError(
                "Pinterest OAuth code exchange requires PINTEREST_CLIENT_ID, "
                "PINTEREST_CLIENT_SECRET, and PINTEREST_REDIRECT_URI."
            )

        payload = {
            "grant_type": "authorization_code",
            "code": clean_code,
            "redirect_uri": str(self.redirect_uri),
        }
        response_payload = self.oauth_token_request(payload)
        self._apply_token_response(response_payload)
        return response_payload

    def refresh_access_token(self) -> dict[str, Any]:
        if not self.can_refresh_access_token():
            raise RuntimeError(
                "Pinterest token refresh requires PINTEREST_REFRESH_TOKEN, "
                "PINTEREST_CLIENT_ID, and PINTEREST_CLIENT_SECRET."
            )

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": str(self.refresh_token),
            "scope": ",".join(self.oauth_scopes),
        }
        response_payload = self.oauth_token_request(payload)
        self._apply_token_response(response_payload)
        return response_payload

    def _apply_token_response(self, response_payload: dict[str, Any]) -> None:
        access_token = response_payload.get("access_token")
        if isinstance(access_token, str) and access_token.strip():
            self.access_token = access_token.strip()

        refresh_token = response_payload.get("refresh_token")
        if isinstance(refresh_token, str) and refresh_token.strip():
            self.refresh_token = refresh_token.strip()

    def ensure_access_token(self) -> str:
        if self.access_token:
            return self.access_token
        if self.can_refresh_access_token():
            self.refresh_access_token()
        if not self.access_token:
            raise RuntimeError(
                "Pinterest access token is not configured. Set PINTEREST_ACCESS_TOKEN "
                "or configure PINTEREST_CLIENT_ID, PINTEREST_CLIENT_SECRET, and PINTEREST_REFRESH_TOKEN."
            )
        return self.access_token

    def api_request(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        allow_refresh_retry: bool = True,
    ) -> dict[str, Any]:
        access_token = self.ensure_access_token()

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
                "Authorization": f"Bearer {access_token}",
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
            if exc.code == 401 and allow_refresh_retry and self.can_refresh_access_token():
                self.refresh_access_token()
                return self.api_request(
                    method=method,
                    path=path,
                    query=query,
                    payload=payload,
                    allow_refresh_retry=False,
                )
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
