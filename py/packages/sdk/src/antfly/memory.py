# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""First-party memory operations on the existing authenticated SDK transport."""

from __future__ import annotations

import hashlib
import uuid
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

if TYPE_CHECKING:
    from .client import AntflyClient


# Non-GET actions that only read; they never take an Idempotency-Key.
_READ_ONLY_POSTS = frozenset(
    {
        "changes",
        "supersession_candidates",
        "evaluation_tasks",
        "recall",
        "brief",
        "consolidate",
        "explain",
        "entity_candidates",
        "entity_view",
    }
)

# Fields accepted by POST /recall (MemoryRecallRequest), besides ``query``.
_RECALL_FIELDS = frozenset(
    {
        "graph",
        "relationship",
        "target_id",
        "missing_relationship",
        "subject",
        "predicate",
        "scope",
        "environment",
        "task",
        "kind",
        "valid_at",
        "known_at",
        "include_history",
        "include_provisional",
        "limit",
        "token_budget",
        "cursor",
        "embedding",
        "embedding_model",
        "utility_weight",
    }
)


def _new_key() -> str:
    return uuid.uuid4().hex


def _compact(**fields: Any) -> dict[str, Any]:
    return {name: value for name, value in fields.items() if value is not None}


class MemoryOperations:
    """Native agent memory API.

    The simple path is ``remember`` -> ``brief``/``recall`` -> ``outcome``. Mutations
    get an auto-generated Idempotency-Key unless one is passed; pass your own when a
    retry must be deduplicated across process restarts.
    """

    def __init__(self, client: AntflyClient) -> None:
        self._client = client

    def create(
        self,
        space: str,
        *,
        idempotency_key: str | None = None,
        config: dict[str, Any] | None = None,
        database: str = "default",
        namespace: str = "public",
    ) -> Any:
        """Create a memory space; returns the operation receipt."""
        idempotency_key = idempotency_key or _new_key()
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
        idempotency_key: str | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Run a native action by its embedded envelope name (the Zig ``Action`` enum).

        Mutations send ``idempotency_key`` or, when omitted, a generated one. Reads
        (GET routes and read-only POSTs such as recall, brief and consolidate) never
        send one. GET bodies are sent as query parameters.
        """
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
            "access": ("POST", "/accesses"),
            "usage": ("GET", "/memories/" + quote(id, safe="") + "/usage"),
            "prepare_consolidation": ("POST", "/consolidation-proposals"),
            "configure": ("PATCH", ""),
            "observe": ("POST", "/observations"),
            "assertions": ("POST", "/assertions"),
            "recall": ("POST", "/recall"),
            "brief": ("POST", "/brief"),
            "consolidate": ("POST", "/consolidation"),
            "pin": ("POST", "/pins"),
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
        if method == "GET" or action in _READ_ONLY_POSTS:
            idempotency_key = None
        else:
            idempotency_key = idempotency_key or _new_key()
        headers = {"X-Antfly-Database": database, "X-Antfly-Namespace": namespace, "X-Antfly-Memory-Branch": branch}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        kwargs: dict[str, Any] = {"headers": headers}
        kwargs["params" if method == "GET" else "json"] = body or {}
        return self._client._request(method, "/memory/v1/spaces/" + quote(space, safe="") + suffix, **kwargs)

    # Convenience wrappers over ``execute``. Each accepts ``database``, ``namespace``
    # and ``branch`` scope keywords; body fields left as ``None`` are omitted so the
    # server applies its defaults.

    def brief(
        self,
        space: str,
        *,
        limit: int | None = None,
        token_budget: int | None = None,
        scope: str | None = None,
        task: str | None = None,
        environment: str | None = None,
        include_provisional: bool | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Load a small, token-bounded context at session start (read-only).

        Returns current, active memories ranked for usefulness (pinned ones always
        included), the subjects available for deeper ``recall``, and guidance.
        Server defaults are ``limit=16`` and ``token_budget=1024``.
        """
        body = _compact(
            scope=scope,
            task=task,
            environment=environment,
            include_provisional=include_provisional,
            limit=limit,
            token_budget=token_budget,
        )
        return self.execute(space, "brief", body=body, database=database, namespace=namespace, branch=branch)

    def recall(
        self,
        space: str,
        *,
        query: str = "",
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
        **filters: Any,
    ) -> Any:
        """Search memory for ``query`` (read-only).

        ``filters`` are MemoryRecallRequest fields such as ``limit``, ``token_budget``,
        ``scope``, ``kind``, ``subject``, ``known_at`` or ``utility_weight`` (0..1,
        how much recorded outcomes boost ranking).
        """
        unknown = sorted(set(filters) - _RECALL_FIELDS)
        if unknown:
            raise TypeError("unknown recall fields: " + ", ".join(unknown))
        body = {"query": query, **_compact(**filters)}
        return self.execute(space, "recall", body=body, database=database, namespace=namespace, branch=branch)

    def remember(
        self,
        space: str,
        text: str,
        *,
        evidence_id: str | None = None,
        source_id: str = "agent",
        assertions: list[dict[str, Any]] | None = None,
        thread_id: str | None = None,
        visibility: str | None = None,
        idempotency_key: str | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Record ``text`` as one evidence item (an observation).

        Pass ``assertions`` to store claims that cite it (or let a space configured
        for model extraction derive them). ``evidence_id`` is generated when omitted;
        evidence IDs name immutable source versions.
        """
        generated_evidence_id = evidence_id is None
        evidence_id = evidence_id or (
            hashlib.sha256(("antfly-memory-remember:" + idempotency_key).encode()).hexdigest()
            if idempotency_key
            else _new_key()
        )
        idempotency_key = idempotency_key or _new_key()
        evidence = _compact(
            id=evidence_id,
            source_id=source_id,
            text=text,
            thread_id=thread_id,
            visibility=visibility,
        )
        body: dict[str, Any] = {"evidence": [evidence]}
        if generated_evidence_id:
            body["scoped_evidence_ids"] = True
        if assertions is not None:
            body["assertions"] = [
                {**assertion, "evidence": assertion.get("evidence", [{"id": evidence_id}])} for assertion in assertions
            ]
        return self.execute(
            space,
            "observe",
            body=body,
            idempotency_key=idempotency_key,
            database=database,
            namespace=namespace,
            branch=branch,
        )

    def outcome(
        self,
        space: str,
        *,
        id: str,
        task_id: str,
        success: bool,
        used_memory_ids: list[str],
        detail: str = "",
        idempotency_key: str | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Report whether a task that used ``used_memory_ids`` succeeded.

        Outcomes feed memory utility, which ranks ``brief`` and ``recall``
        (with ``utility_weight``).
        """
        body = {"id": id, "task_id": task_id, "success": success, "used_memory_ids": used_memory_ids}
        if detail:
            body["detail"] = detail
        return self.execute(
            space,
            "outcome",
            body=body,
            idempotency_key=idempotency_key,
            database=database,
            namespace=namespace,
            branch=branch,
        )

    def consolidate(
        self,
        space: str,
        *,
        limit: int | None = None,
        stale_after_ms: int | None = None,
        cursor: str | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Review memory health (duplicates, conflicts, stale claims) without changing anything.

        Findings carry a ``suggested_action``; apply them with ``correct``, ``pin``
        or ``forget``.
        """
        body = _compact(limit=limit, stale_after_ms=stale_after_ms, cursor=cursor)
        return self.execute(space, "consolidate", body=body, database=database, namespace=namespace, branch=branch)

    def access(self, space: str, memory_ids: list[str], **options: Any) -> Any:
        """Record consumed recall/brief IDs; reuse idempotency_key on retries."""
        return self.execute(space, "access", body={"memory_ids": memory_ids}, **options)

    def usage(self, space: str, id: str, **options: Any) -> Any:
        """Read your access counts, timestamps and retained outcome utility."""
        return self.execute(space, "usage", id=id, **options)

    def prepare_consolidation(self, space: str, *, cursor: str | None = None, limit: int = 32, **options: Any) -> Any:
        """Create review proposals without changing beliefs; continue with review_cursor."""
        return self.execute(space, "prepare_consolidation", body=_compact(cursor=cursor, limit=limit), **options)

    def pin(
        self,
        space: str,
        id: str,
        *,
        reason: str = "",
        idempotency_key: str | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Prioritize memory ``id`` in ``brief`` within its item and byte limits."""
        return self._set_pin(space, id, True, reason, idempotency_key, database, namespace, branch)

    def unpin(
        self,
        space: str,
        id: str,
        *,
        reason: str = "",
        idempotency_key: str | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Remove the pin from memory ``id``."""
        return self._set_pin(space, id, False, reason, idempotency_key, database, namespace, branch)

    def _set_pin(
        self,
        space: str,
        id: str,
        pinned: bool,
        reason: str,
        idempotency_key: str | None,
        database: str,
        namespace: str,
        branch: str,
    ) -> Any:
        body: dict[str, Any] = {"id": id, "pinned": pinned}
        if reason:
            body["reason"] = reason
        return self.execute(
            space,
            "pin",
            body=body,
            idempotency_key=idempotency_key,
            database=database,
            namespace=namespace,
            branch=branch,
        )

    def correct(
        self,
        space: str,
        id: str,
        *,
        expected_revision: int,
        lifecycle: str,
        reason: str,
        stability: str | None = None,
        evidence: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Change a memory's lifecycle (active, disputed, superseded, retracted) or stability."""
        body = _compact(
            id=id,
            expected_revision=expected_revision,
            lifecycle=lifecycle,
            stability=stability,
            reason=reason,
            evidence=evidence,
        )
        return self.execute(
            space,
            "correct",
            body=body,
            idempotency_key=idempotency_key,
            database=database,
            namespace=namespace,
            branch=branch,
        )

    def forget(
        self,
        space: str,
        *,
        evidence_ids: list[str] | None = None,
        assertion_ids: list[str] | None = None,
        source_ids: list[str] | None = None,
        all: bool | None = None,
        idempotency_key: str | None = None,
        database: str = "default",
        namespace: str = "public",
        branch: str = "",
    ) -> Any:
        """Erase evidence, assertions or whole sources (``all=True`` erases the space's memory)."""
        body = _compact(evidence_ids=evidence_ids, assertion_ids=assertion_ids, source_ids=source_ids, all=all)
        return self.execute(
            space,
            "forget",
            body=body,
            idempotency_key=idempotency_key,
            database=database,
            namespace=namespace,
            branch=branch,
        )
