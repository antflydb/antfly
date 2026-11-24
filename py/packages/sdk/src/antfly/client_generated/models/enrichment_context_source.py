from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.enrichment_context_source_overflow import EnrichmentContextSourceOverflow
from ..models.enrichment_context_source_tracking import EnrichmentContextSourceTracking
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_context_document import EnrichmentContextDocument
    from ..models.enrichment_context_query import EnrichmentContextQuery


T = TypeVar("T", bound="EnrichmentContextSource")


@_attrs_define
class EnrichmentContextSource:
    """One named context source: exactly one of `document` (a referenced row) or `query` (a filter, a graph traversal, or a
    ranked retrieval). `tracking` decides the freshness guarantee. A query whose membership is decided row by row is
    tracked live: subscriptions invalidate dependents exactly and they can be `current`. That covers a `filter_query`
    (optionally with a top-N `order_by`) and one plain bounded `graph_queries` traverse (optionally with a
    `filter_query` narrowing the rows it reaches). A query with any ranking clause (`full_text_search`,
    `semantic_search`, `embeddings`, `indexes`, `merge_config`, `reranker`, `exclusion_query`, `limit`, `cypher`, the
    `text`/`vector` shorthands, or `graph_queries` beyond one plain bounded traverse) depends on the whole corpus and is
    a snapshot: its dependents are `unverified` and rebuild on `refresh_interval` or manually. Registration reports each
    source's effective tracking and, for a snapshot, the clause that made it one.

        Attributes:
            fields (list[str]): Projected fields consumed by the producer (dotted paths). Only these fields, predicate
                fields, and the order field participate in fingerprints and invalidation. `_artifacts.<artifact>[.<path>]` reads
                a row's generated output (asset: parsed JSON or text; chunk: array of chunk payloads; embedding: document
                vector) and may be used in projections and predicates; when the artifact is regenerated, dependents are
                invalidated like for a field change. Admission rejects artifacts that depend on the consuming enrichment
                (directly, through artifact streams, through other context sources, or across tables). Other fields starting
                with `_` are reserved and rejected.
            document (EnrichmentContextDocument | Unset):
            query (EnrichmentContextQuery | Unset): A filter (tracked live) or a ranked retrieval (a snapshot). Any ranking
                clause makes the whole query a public QueryRequest executed by the table's query engine, with
                `{"$source":"/pointer"}` bindings anywhere; `limit` (at most 1000) is then the member count, and paging
                (`offset`, `search_after`, `search_before`), `count`, `aggregations`, and a top-N `order_by` are rejected.
                Distributed tables refuse `semantic_search` (data servers have no query embedder); pass precomputed vectors in
                `embeddings` instead.
            required (bool | Unset): Document sources only: a missing referenced document fails the rebuild explicitly
                instead of rendering null. Default: False.
            max_documents (int | Unset): Query sources: maximum members (for a traversal, rows reached). Exceeding it fails
                explicitly; with `order_by` it is the declared top-N. Default: 100.
            max_candidates (int | Unset): Query sources with residual predicates (range, prefix, match, similar) or
                `order_by`: maximum candidate rows examined after equality filtering. Exceeding it fails explicitly. Default:
                1000.
            max_bytes (int | Unset): Maximum rendered bytes for this source. Default: 1048576.
            overflow (EnrichmentContextSourceOverflow | Unset):  Default: EnrichmentContextSourceOverflow.ERROR.
            tracking (EnrichmentContextSourceTracking | Unset): Defaults to the strongest guarantee the source supports:
                `live` for filters, `snapshot` for ranked queries. `live` registers subscriptions and keeps dependents current.
                `snapshot` registers nothing: dependents publish with freshness `unverified` (never `current`), rebuild on
                `refresh_interval` or manually, and `freshness=require_current` dependency reads refuse them. Declaring `live`
                asserts the guarantee: a query with a ranking clause is then rejected (400 `context_source_requires_snapshot`)
                instead of silently becoming a snapshot. Declaring `snapshot` opts a filter out of live tracking.
            refresh_interval (str | Unset): Snapshot sources only: rebuild interval (at least 1s).
    """

    fields: list[str]
    document: EnrichmentContextDocument | Unset = UNSET
    query: EnrichmentContextQuery | Unset = UNSET
    required: bool | Unset = False
    max_documents: int | Unset = 100
    max_candidates: int | Unset = 1000
    max_bytes: int | Unset = 1048576
    overflow: EnrichmentContextSourceOverflow | Unset = EnrichmentContextSourceOverflow.ERROR
    tracking: EnrichmentContextSourceTracking | Unset = UNSET
    refresh_interval: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query, Unset):
            query = self.query.to_dict()

        required = self.required

        max_documents = self.max_documents

        max_candidates = self.max_candidates

        max_bytes = self.max_bytes

        overflow: str | Unset = UNSET
        if not isinstance(self.overflow, Unset):
            overflow = self.overflow.value

        tracking: str | Unset = UNSET
        if not isinstance(self.tracking, Unset):
            tracking = self.tracking.value

        refresh_interval = self.refresh_interval

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fields": fields,
            }
        )
        if document is not UNSET:
            field_dict["document"] = document
        if query is not UNSET:
            field_dict["query"] = query
        if required is not UNSET:
            field_dict["required"] = required
        if max_documents is not UNSET:
            field_dict["max_documents"] = max_documents
        if max_candidates is not UNSET:
            field_dict["max_candidates"] = max_candidates
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes
        if overflow is not UNSET:
            field_dict["overflow"] = overflow
        if tracking is not UNSET:
            field_dict["tracking"] = tracking
        if refresh_interval is not UNSET:
            field_dict["refresh_interval"] = refresh_interval

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_context_document import EnrichmentContextDocument
        from ..models.enrichment_context_query import EnrichmentContextQuery

        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields"))

        _document = d.pop("document", UNSET)
        document: EnrichmentContextDocument | Unset
        if isinstance(_document, Unset):
            document = UNSET
        else:
            document = EnrichmentContextDocument.from_dict(_document)

        _query = d.pop("query", UNSET)
        query: EnrichmentContextQuery | Unset
        if isinstance(_query, Unset):
            query = UNSET
        else:
            query = EnrichmentContextQuery.from_dict(_query)

        required = d.pop("required", UNSET)

        max_documents = d.pop("max_documents", UNSET)

        max_candidates = d.pop("max_candidates", UNSET)

        max_bytes = d.pop("max_bytes", UNSET)

        _overflow = d.pop("overflow", UNSET)
        overflow: EnrichmentContextSourceOverflow | Unset
        if isinstance(_overflow, Unset):
            overflow = UNSET
        else:
            overflow = EnrichmentContextSourceOverflow(_overflow)

        _tracking = d.pop("tracking", UNSET)
        tracking: EnrichmentContextSourceTracking | Unset
        if isinstance(_tracking, Unset):
            tracking = UNSET
        else:
            tracking = EnrichmentContextSourceTracking(_tracking)

        refresh_interval = d.pop("refresh_interval", UNSET)

        enrichment_context_source = cls(
            fields=fields,
            document=document,
            query=query,
            required=required,
            max_documents=max_documents,
            max_candidates=max_candidates,
            max_bytes=max_bytes,
            overflow=overflow,
            tracking=tracking,
            refresh_interval=refresh_interval,
        )

        return enrichment_context_source
