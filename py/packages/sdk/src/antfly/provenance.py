# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Versioned references and trace rows for ordinary Antfly tables.

Helpers only build data; they do not ingest traces or promote them into memory.
"""

from __future__ import annotations

from typing import Any, Literal

ResourceKind = Literal[
    "table",
    "record",
    "relationship",
    "trace",
    "evidence",
    "assertion",
    "entity",
    "outcome",
    "decision",
    "operation",
    "configuration",
]
_RELATIONS = frozenset(
    (
        "mentions",
        "retrieved",
        "used",
        "supported_by",
        "generated",
        "generated_by",
        "approved_by",
        "supersedes",
        "applied_to",
        "governed_by",
        "originated_in",
        "depends_on",
        "motivates",
        "rejected_in_favor_of",
    )
)


def resource_reference(
    *,
    table: str,
    table_id: int,
    id: str,
    revision: int,
    kind: ResourceKind = "record",
    database: str = "default",
    namespace: str = "public",
    digest: str | None = None,
    storage_name: str | None = None,
) -> dict[str, Any]:
    """Pin a table incarnation and exact committed resource revision."""
    if kind not in (
        "table",
        "record",
        "relationship",
        "trace",
        "evidence",
        "assertion",
        "entity",
        "outcome",
        "decision",
        "operation",
        "configuration",
    ):
        raise ValueError("invalid resource kind")
    for number in (table_id, revision):
        if isinstance(number, bool) or not isinstance(number, int) or not 0 < number <= 2**64 - 1:
            raise ValueError("resource identities require exact positive uint64 integers")
    for value in (database, namespace, table, id):
        if not value or len(value.encode("utf-8")) > 256 or any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
            raise ValueError("invalid resource identity")
    if digest is not None and (len(digest) != 64 or any(ch not in "0123456789abcdefABCDEF" for ch in digest)):
        raise ValueError("digest must contain 64 hexadecimal characters")
    result: dict[str, Any] = dict(
        database=database, namespace=namespace, table=table, table_id=table_id, kind=kind, id=id, revision=revision
    )
    if storage_name is not None:
        result["storage_name"] = storage_name
    if digest is not None:
        result["digest"] = digest
    return result


def trace_event(
    *,
    trace_id: str,
    span_id: str,
    name: str,
    started_at: int,
    ended_at: int | None = None,
    parent_span_id: str | None = None,
    status: Literal["running", "succeeded", "failed", "cancelled"] = "running",
    links: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build one version-1 span row for the existing table batch APIs.

    Retrieval, use, and support links remain distinct. A trace origin is not
    automatically factual support for a memory or proof of human approval.
    """
    if not trace_id or not span_id or not name:
        raise ValueError("trace_id, span_id and name are required")
    if status not in ("running", "succeeded", "failed", "cancelled"):
        raise ValueError("invalid trace status")
    if ended_at is not None and ended_at < started_at:
        raise ValueError("span ends before it starts")
    copied = []
    for link in links or []:
        if link.get("relation") not in _RELATIONS:
            raise ValueError("invalid provenance relation")
        copied.append(dict(relation=link["relation"], target=resource_reference(**link["target"])))
    if len(copied) > 256:
        raise ValueError("too many provenance links")
    return dict(
        schema_version=1,
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        name=name,
        started_at=started_at,
        ended_at=ended_at,
        status=status,
        provenance=dict(schema_version=1, links=copied),
    )
