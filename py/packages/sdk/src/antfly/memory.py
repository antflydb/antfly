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
        branch: str = "",
    ) -> Any:
        """Use the same action names as the native embedded memory envelope."""
        routes = {
            "changes": ("POST", "/changes"),
            "lineage": ("GET", "/lineage"),
            "fork_lineage": ("POST", "/forks"),
            "checkpoint_lineage": ("POST", "/checkpoints"),
            "revert_lineage": ("POST", "/reverts"),
            "supersession_candidates": ("POST", "/memories/" + quote(id, safe="") + "/supersession-candidates"),
            "propose_supersession": ("POST", "/supersession-proposals"),
            "supersession_proposals": ("GET", "/supersession-proposals"),
            "supersession_proposal": ("GET", "/supersession-proposals/" + quote(id, safe="")),
            "review_supersession": ("POST", "/supersession-proposals/" + quote(id, safe="") + "/verdict"),
            "put_evaluation_dataset": ("POST", "/evaluation-datasets"),
            "evaluation_dataset": ("GET", "/evaluation-datasets/" + quote(id, safe="")),
            "evaluation_cases": ("GET", "/evaluation-datasets/" + quote(id, safe="") + "/cases"),
            "start_evaluation": ("POST", "/evaluations"),
            "evaluation_results": ("GET", "/evaluations/" + quote(id, safe="") + "/results"),
            "evaluation_run": ("GET", "/evaluations/" + quote(id, safe="")),
            "evaluation_tasks": ("POST", "/evaluations/" + quote(id, safe="") + "/tasks"),
            "evaluation_verdict": ("POST", "/evaluations/" + quote(id, safe="") + "/verdicts"),
            "inspect": ("GET", ""),
            "configure": ("PATCH", ""),
            "observe": ("POST", "/observations"),
            "assertions": ("POST", "/assertions"),
            "recall": ("POST", "/recall"),
            "explain": ("POST", "/memories/" + quote(id, safe="") + "/explain" if id else "/explain"),
            "list": ("GET", "/memories"),
            "get": ("GET", "/memories/" + quote(id, safe="")),
            "history": ("GET", "/memories/" + quote(id, safe="") + "/history"),
            "correct": ("POST", "/corrections"),
            "forget": ("POST", "/forget"),
            "outcome": ("POST", "/outcomes"),
            "operation": ("GET", "/operations/" + quote(id, safe="")),
            "advance": ("POST", "/operations/" + quote(id, safe="") if id else "/maintenance"),
            "entities": ("GET", "/entities"),
            "entity_view": ("POST", "/entities/" + quote(id, safe="") + "/view"),
            "episodes": ("GET", "/episodes"),
            "episode": ("GET", "/episodes/" + quote(id, safe="")),
            "propose_relationship": ("POST", "/relationship-proposals"),
            "relationship_proposal": ("GET", "/relationship-proposals/" + quote(id, safe="")),
            "review_relationship": ("POST", "/relationship-proposals/" + quote(id, safe="") + "/verdict"),
            "entity_candidates": ("POST", "/entities/" + quote(id, safe="") + "/candidates"),
            "propose_entity": ("POST", "/entity-proposals"),
            "entity_proposals": ("GET", "/entity-proposals"),
            "entity_proposal": ("GET", "/entity-proposals/" + quote(id, safe="")),
            "review_entity": ("POST", "/entity-proposals/" + quote(id, safe="") + "/verdict"),
            "resolve_entity": ("GET", "/entities/" + quote(id, safe="")),
            "curate_entity": ("POST", "/entity-corrections"),
            "configuration": ("GET", "/configurations/" + quote(id, safe="")),
            "evidence": ("GET", "/evidence/" + quote(id, safe="")),
            "stage_embeddings": ("POST", "/embeddings"),
        }
        if action not in routes:
            raise ValueError("unknown native memory action")
        method, suffix = routes[action]
        if (
            method != "GET"
            and action
            not in (
                "changes",
                "supersession_candidates",
                "evaluation_tasks",
                "recall",
                "explain",
                "entity_candidates",
                "entity_view",
            )
            and not idempotency_key
        ):
            raise ValueError("memory mutations require idempotency_key")
        headers = {"X-Antfly-Database": database, "X-Antfly-Namespace": namespace, "X-Antfly-Memory-Branch": branch}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        kwargs: dict[str, Any] = {"headers": headers}
        kwargs["params" if method == "GET" else "json"] = body or {}
        return self._client._request(method, "/memory/v1/spaces/" + quote(space, safe="") + suffix, **kwargs)
