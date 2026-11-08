"""Control the optional antfly-source companion at its own base URL."""

from typing import Any
from urllib.parse import quote, urlparse

import httpx


class SourceServiceError(Exception):
    def __init__(self, status: int, code: str) -> None:
        self.status = status
        self.code = code
        super().__init__(f"Source service returned {status}: {code}")


class _SourceTransport:
    """Thin transport; enumeration, retries and deletion stay in the shared engine.

    The token is a source-service operator token. Use this client on your backend,
    not in a browser or with an Antfly database token. Connections accept only
    secret references; the application can instead inject a Factory into Service.
    """

    def __init__(self, base_url: str, token: str, *, transport: httpx.BaseTransport | None = None) -> None:
        url = urlparse(base_url)
        loopback = url.hostname in {"localhost", "127.0.0.1", "::1"}
        if (
            not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or (url.scheme != "https" and not (url.scheme == "http" and loopback))
        ):
            raise ValueError("Source service requires HTTPS or loopback HTTP without URL credentials")
        if not token or "\r" in token or "\n" in token:
            raise ValueError("Invalid source service token")
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {token}"},
            follow_redirects=False,
            timeout=30.0,
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SourcesClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None, *, headers: dict[str, str] | None = None
    ) -> dict[str, Any]:
        response = self._client.request(method, path, json=body, headers=headers)
        if not response.is_success:
            code = "request_failed"
            try:
                value = response.json()
                if isinstance(value, dict) and isinstance(value.get("code"), str):
                    code = value["code"]
            except ValueError:
                pass
            raise SourceServiceError(response.status_code, code)
        value = response.json()
        if not isinstance(value, dict):
            raise ValueError("Invalid source service response")
        return value


class StandaloneSourcesClient(_SourceTransport):
    """Standalone-only credential and legacy profile operations."""

    def put_secret(self, key: str, value: str) -> dict[str, Any]:
        return self._request("PUT", f"/v1/secrets/{quote(key, safe='')}", {"value": value})

    def revoke_secret(self, key: str) -> dict[str, Any]:
        return self._request("DELETE", f"/v1/secrets/{quote(key, safe='')}")

    def profile(self) -> dict[str, Any]:
        return self._request("GET", "/v1/profile")

    def put_connection(self, name: str, connection: dict[str, Any]) -> dict[str, Any]:
        return self._request("PUT", f"/v1/connections/{quote(name, safe='')}", connection)

    def revoke_connection(self, name: str) -> dict[str, Any]:
        return self._request("DELETE", f"/v1/connections/{quote(name, safe='')}")

    def put_source(self, source: dict[str, Any]) -> dict[str, Any]:
        return self._request("PUT", f"/v1/sources/{quote(source['id'], safe='')}", source)

    def remove_source(self, source_id: str, *, purge: bool = False) -> dict[str, Any]:
        """Revoke a source; purge=True queues row deletion for the next sync."""
        suffix = "?purge=true" if purge else ""
        return self._request("DELETE", f"/v1/sources/{quote(source_id, safe='')}{suffix}")

    def sync(self, *, restart: bool = False) -> dict[str, Any]:
        return self._request("POST", "/v1/sync", {"restart": restart})

    def run_purges(self) -> dict[str, Any]:
        """Apply durable purge intents independently of ingestion credentials."""
        return self._request("POST", "/v1/sync", {"purge_only": True})

    def current_job(self) -> dict[str, Any]:
        return self._request("GET", "/v1/jobs")

    def job(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/jobs/{quote(job_id, safe='')}")

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        return self._request("DELETE", f"/v1/jobs/{quote(job_id, safe='')}")


class SourcesClient(_SourceTransport):
    """Common management API; base_url includes the runtime's /sources/v1 mount."""

    def capabilities(self) -> dict[str, Any]:
        return self._request("GET", "/capabilities")

    def sources(self, *, limit: int = 20, cursor: str = "") -> dict[str, Any]:
        return self._request("GET", f"/sources?limit={limit}&cursor={quote(cursor, safe='')}")

    def source(self, source_id: str) -> dict[str, Any]:
        return self._request("GET", f"/sources/{quote(source_id, safe='')}")

    def preview_processing(self, source_id: str, preset: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/sources/{quote(source_id, safe='')}/processing-preview", preset)

    def processing_job(self, source_id: str, request: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"/sources/{quote(source_id, safe='')}/processing-jobs", request)

    def put_source(self, source: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "PUT",
            f"/sources/{quote(source['id'], safe='')}",
            {key: value for key, value in source.items() if key != "search_status"},
        )

    def remove_source(self, source_id: str, *, purge: bool = False) -> dict[str, Any]:
        return self._request("DELETE", f"/sources/{quote(source_id, safe='')}?purge={str(purge).lower()}")

    def runs(self, source_id: str, *, limit: int = 20, cursor: str = "") -> dict[str, Any]:
        return self._request(
            "GET", f"/sources/{quote(source_id, safe='')}/runs?limit={limit}&cursor={quote(cursor, safe='')}"
        )

    def run(self, source_id: str, run_id: str) -> dict[str, Any]:
        return self._request("GET", f"/sources/{quote(source_id, safe='')}/runs/{quote(run_id, safe='')}")

    def start_run(self, source_id: str, key: str, *, restart: bool = False, operation: str = "sync") -> dict[str, Any]:
        return self._request(
            "POST",
            f"/sources/{quote(source_id, safe='')}/runs",
            {"restart": restart, "operation": operation},
            headers={"Idempotency-Key": key},
        )

    def cancel_run(self, source_id: str, run_id: str) -> dict[str, Any]:
        return self._request("POST", f"/sources/{quote(source_id, safe='')}/runs/{quote(run_id, safe='')}/cancel", {})
