from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.enrichment_context_source_overflow import EnrichmentContextSourceOverflow
from ..models.enrichment_context_source_tracking import EnrichmentContextSourceTracking
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_context_document import EnrichmentContextDocument
    from ..models.enrichment_context_graph import EnrichmentContextGraph
    from ..models.enrichment_context_query import EnrichmentContextQuery
    from ..models.enrichment_context_search import EnrichmentContextSearch


T = TypeVar("T", bound="EnrichmentContextSource")


@_attrs_define
class EnrichmentContextSource:
    """One named context source: exactly one of `document` (a referenced row), `query` (the matching set of a parameterized
    filter, including the empty set), `graph` (bounded traversal over directly written edges), or `search` (ranked
    retrieval, snapshot only). Live sources are invalidated exactly: every admitted predicate decides a row's membership
    from that row alone. Ranked retrieval depends on the whole corpus and is only available as an explicitly unverified
    snapshot.

        Attributes:
            fields (list[str]): Projected fields consumed by the producer (dotted paths). Only these fields, predicate
                fields, and the order field participate in fingerprints and invalidation. Fields starting with `_` are reserved
                and rejected.
            document (EnrichmentContextDocument | Unset):
            query (EnrichmentContextQuery | Unset):
            graph (EnrichmentContextGraph | Unset): Bounded breadth-first traversal over explicitly written edges of a graph
                index. Field- and artifact-derived (generated) edges are never traversed by live context. Members render with
                depth, edge type, and direction.
            search (EnrichmentContextSearch | Unset): Ranked retrieval (requires `tracking: snapshot`): either full-text
                relevance (`text` + `text_field`) or dense k-NN (`vector` binding + `vector_index`). Members render in rank
                order.
            required (bool | Unset): Document sources only: a missing referenced document fails the rebuild explicitly
                instead of rendering null. Default: False.
            max_documents (int | Unset): Query and graph sources: maximum members. Exceeding it fails explicitly; with
                `order_by` it is the declared top-N. Default: 100.
            max_candidates (int | Unset): Query sources with residual predicates (range, prefix, match, similar) or
                `order_by`: maximum candidate rows examined after equality filtering. Exceeding it fails explicitly. Default:
                1000.
            max_bytes (int | Unset): Maximum rendered bytes for this source. Default: 1048576.
            overflow (EnrichmentContextSourceOverflow | Unset):  Default: EnrichmentContextSourceOverflow.ERROR.
            tracking (EnrichmentContextSourceTracking | Unset): `live` registers subscriptions and keeps dependents current.
                `snapshot` registers nothing: dependents publish with freshness `unverified` (never `current`), rebuild on
                `refresh_interval` or manually, and `require_current` reads refuse them. Required for `search`. Default:
                EnrichmentContextSourceTracking.LIVE.
            refresh_interval (str | Unset): Snapshot sources only: rebuild interval (at least 1s).
    """

    fields: list[str]
    document: EnrichmentContextDocument | Unset = UNSET
    query: EnrichmentContextQuery | Unset = UNSET
    graph: EnrichmentContextGraph | Unset = UNSET
    search: EnrichmentContextSearch | Unset = UNSET
    required: bool | Unset = False
    max_documents: int | Unset = 100
    max_candidates: int | Unset = 1000
    max_bytes: int | Unset = 1048576
    overflow: EnrichmentContextSourceOverflow | Unset = EnrichmentContextSourceOverflow.ERROR
    tracking: EnrichmentContextSourceTracking | Unset = EnrichmentContextSourceTracking.LIVE
    refresh_interval: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document, Unset):
            document = self.document.to_dict()

        query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query, Unset):
            query = self.query.to_dict()

        graph: dict[str, Any] | Unset = UNSET
        if not isinstance(self.graph, Unset):
            graph = self.graph.to_dict()

        search: dict[str, Any] | Unset = UNSET
        if not isinstance(self.search, Unset):
            search = self.search.to_dict()

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
        if graph is not UNSET:
            field_dict["graph"] = graph
        if search is not UNSET:
            field_dict["search"] = search
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
        from ..models.enrichment_context_graph import EnrichmentContextGraph
        from ..models.enrichment_context_query import EnrichmentContextQuery
        from ..models.enrichment_context_search import EnrichmentContextSearch

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

        _graph = d.pop("graph", UNSET)
        graph: EnrichmentContextGraph | Unset
        if isinstance(_graph, Unset):
            graph = UNSET
        else:
            graph = EnrichmentContextGraph.from_dict(_graph)

        _search = d.pop("search", UNSET)
        search: EnrichmentContextSearch | Unset
        if isinstance(_search, Unset):
            search = UNSET
        else:
            search = EnrichmentContextSearch.from_dict(_search)

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
            graph=graph,
            search=search,
            required=required,
            max_documents=max_documents,
            max_candidates=max_candidates,
            max_bytes=max_bytes,
            overflow=overflow,
            tracking=tracking,
            refresh_interval=refresh_interval,
        )

        return enrichment_context_source
