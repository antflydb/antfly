# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""First-party memory operations on the existing authenticated SDK transport."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import quote

if TYPE_CHECKING:
    from .client import AntflyClient


class MemoryOperations:
    def __init__(self, client: AntflyClient) -> None:
        self._client = client

    def create(
        self,
        space: str,
        *,
        idempotency_key: str,
        config: dict[str, Any] | None = None,
        database: str = "default",
        namespace: str = "public",
    ) -> Any:
        if not idempotency_key:
            raise ValueError("memory mutations require idempotency_key")
        return self._client._request(
            "POST",
            "/memory/v1/spaces",
            json={"space": space, "config": config or {}},
            headers={
                "Idempotency-Key": idempotency_key,
                "X-Antfly-Database": database,
                "X-Antfly-Namespace": namespace,
            },
        )

    def execute(
        self,
        space: str,
        action: str,
        *,
        body: dict[str, Any] | None = None,
        id: str = "",
        idempotency_key: str = "",
        database: str = "default",
        namespace: str = "public",
    ) -> Any:
        """Use the same action names as the native embedded memory envelope."""
        routes = {
            "inspect": ("GET", ""),
            "configure": ("PATCH", ""),
            "observe": ("POST", "/observations"),
            "assertions": ("POST", "/assertions"),
            "recall": ("POST", "/recall"),
            "list": ("GET", "/memories"),
            "get": ("GET", "/memories/" + quote(id, safe="")),
            "history": ("GET", "/memories/" + quote(id, safe="") + "/history"),
            "correct": ("POST", "/corrections"),
            "forget": ("POST", "/forget"),
            "outcome": ("POST", "/outcomes"),
            "operation": ("GET", "/operations/" + quote(id, safe="")),
            "advance": ("POST", "/operations/" + quote(id, safe="") if id else "/maintenance"),
            "entities": ("GET", "/entities"),
            "resolve_entity": ("GET", "/entities/" + quote(id, safe="")),
            "curate_entity": ("POST", "/entity-corrections"),
            "configuration": ("GET", "/configurations/" + quote(id, safe="")),
            "evidence": ("GET", "/evidence/" + quote(id, safe="")),
            "stage_embeddings": ("POST", "/embeddings"),
        }
        if action not in routes:
            raise ValueError("unknown native memory action")
        method, suffix = routes[action]
        if method != "GET" and action != "recall" and not idempotency_key:
            raise ValueError("memory mutations require idempotency_key")
        headers = {"X-Antfly-Database": database, "X-Antfly-Namespace": namespace}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        kwargs: dict[str, Any] = {"headers": headers}
        kwargs["params" if method == "GET" else "json"] = body or {}
        return self._client._request(method, "/memory/v1/spaces/" + quote(space, safe="") + suffix, **kwargs)
