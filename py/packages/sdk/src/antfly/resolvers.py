# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Durable resolver curation resources with typed replay results."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote

from .client_generated.models import (
    EntityCurationResult,
    EntityMergeRequest,
    ResolverList,
    ResolverMergeRequest,
    ResolverMutationResult,
    ResolverNamespaceRequest,
    ResolverRedirectList,
    ResolverRenderRequest,
    ResolverRenderResponse,
    ResolverResolveRequest,
    ResolverResolveResponse,
    ResolverReviewDecision,
    ResolverReviewPage,
    ResolverSummary,
    ResolverSuppressionList,
    ResolverSuppressionRequest,
    ResolverUnmergeRequest,
)

if TYPE_CHECKING:
    from .client import AntflyClient


class ResolverOperations:
    def __init__(self, client: AntflyClient, database: str | None = None, namespace: str | None = None) -> None:
        self._client = client
        self._prefix = (
            "/db/v1"
            if database is None
            else f"/db/v1/databases/{quote(database, safe='')}/namespaces/{quote(namespace or 'public', safe='')}"
        )

    def in_namespace(self, database: str, namespace: str) -> ResolverOperations:
        return ResolverOperations(self._client, database, namespace)

    def list(self, table: str) -> ResolverList:
        value = self._client._request("GET", self._prefix + f"/tables/{quote(table, safe='')}/resolvers")
        return ResolverList.from_dict(value)

    def get(self, table: str, resolver: str) -> ResolverSummary:
        value = self._client._request(
            "GET", self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}"
        )
        return ResolverSummary.from_dict(value)

    def drop(self, table: str, resolver: str) -> ResolverMutationResult:
        value = self._client._request(
            "DELETE", self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}"
        )
        return ResolverMutationResult.from_dict(value)

    def render(self, table: str, resolver: str, request: ResolverRenderRequest) -> ResolverRenderResponse:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}:render",
            json=request.to_dict(),
        )
        return ResolverRenderResponse.from_dict(value)

    def resolve(self, table: str, resolver: str, request: ResolverResolveRequest) -> ResolverResolveResponse:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}:resolve",
            json=request.to_dict(),
        )
        return ResolverResolveResponse.from_dict(value)

    def namespace(self, table: str, resolver: str, request: ResolverNamespaceRequest) -> ResolverMutationResult:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}:namespace",
            json=request.to_dict(),
        )
        return ResolverMutationResult.from_dict(value)

    def redirects(self, table: str, resolver: str) -> ResolverRedirectList:
        value = self._client._request(
            "GET", self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}/redirects"
        )
        return ResolverRedirectList.from_dict(value)

    def merge(self, table: str, resolver: str, request: ResolverMergeRequest) -> ResolverMutationResult:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}/redirects",
            json=request.to_dict(),
        )
        return ResolverMutationResult.from_dict(value)

    def unmerge(
        self, table: str, resolver: str, key: str, request: ResolverUnmergeRequest, *, pin_distinct: bool | None = None
    ) -> ResolverMutationResult:
        value = self._client._request(
            "DELETE",
            self._prefix
            + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}/redirects/{quote(key, safe='')}",
            json=request.to_dict(),
            params={"pin_distinct": str(pin_distinct).lower()} if pin_distinct is not None else {},
        )
        return ResolverMutationResult.from_dict(value)

    def suppressions(self, table: str, resolver: str) -> ResolverSuppressionList:
        value = self._client._request(
            "GET", self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}/suppressions"
        )
        return ResolverSuppressionList.from_dict(value)

    def suppress(self, table: str, resolver: str, request: ResolverSuppressionRequest) -> ResolverMutationResult:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}/suppressions",
            json=request.to_dict(),
        )
        return ResolverMutationResult.from_dict(value)

    def unsuppress(
        self, table: str, resolver: str, key: str, request: ResolverUnmergeRequest
    ) -> ResolverMutationResult:
        value = self._client._request(
            "DELETE",
            self._prefix
            + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}/suppressions/{quote(key, safe='')}",
            json=request.to_dict(),
        )
        return ResolverMutationResult.from_dict(value)

    def reviews(self, table: str, resolver: str, *, cursor: str | None = None, limit: int = 64) -> ResolverReviewPage:
        value = self._client._request(
            "GET",
            self._prefix + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}/reviews",
            params={"limit": limit, **({"cursor": cursor} if cursor is not None else {})},
        )
        return ResolverReviewPage.from_dict(value)

    def review(
        self, table: str, resolver: str, review_id: str, request: ResolverReviewDecision
    ) -> ResolverMutationResult:
        value = self._client._request(
            "POST",
            self._prefix
            + f"/tables/{quote(table, safe='')}/resolvers/{quote(resolver, safe='')}/reviews/{quote(review_id, safe='')}",
            json=request.to_dict(),
        )
        return ResolverMutationResult.from_dict(value)


class EntityOperations:
    def __init__(self, client: AntflyClient, database: str | None = None, namespace: str | None = None) -> None:
        self._client = client
        self._prefix = (
            "/db/v1"
            if database is None
            else f"/db/v1/databases/{quote(database, safe='')}/namespaces/{quote(namespace or 'public', safe='')}"
        )

    def in_namespace(self, database: str, namespace: str) -> EntityOperations:
        return EntityOperations(self._client, database, namespace)

    def state(self, table: str, key: str) -> EntityCurationResult:
        value = self._client._request(
            "GET", self._prefix + f"/tables/{quote(table, safe='')}/entities/{quote(key, safe='')}/redirects"
        )
        return EntityCurationResult.from_dict(value)

    def merge(self, table: str, key: str, request: EntityMergeRequest) -> EntityCurationResult:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/entities/{quote(key, safe='')}:merge",
            json=request.to_dict(),
        )
        return EntityCurationResult.from_dict(value)

    def unmerge(self, table: str, key: str, request: ResolverUnmergeRequest) -> EntityCurationResult:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/entities/{quote(key, safe='')}:unmerge",
            json=request.to_dict(),
        )
        return EntityCurationResult.from_dict(value)

    def suppress(self, table: str, key: str, request: ResolverUnmergeRequest) -> EntityCurationResult:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/entities/{quote(key, safe='')}:suppress",
            json=request.to_dict(),
        )
        return EntityCurationResult.from_dict(value)

    def unsuppress(self, table: str, key: str, request: ResolverUnmergeRequest) -> EntityCurationResult:
        value = self._client._request(
            "POST",
            self._prefix + f"/tables/{quote(table, safe='')}/entities/{quote(key, safe='')}:unsuppress",
            json=request.to_dict(),
        )
        return EntityCurationResult.from_dict(value)
