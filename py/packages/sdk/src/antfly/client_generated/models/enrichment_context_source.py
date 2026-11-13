from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.enrichment_context_source_overflow import EnrichmentContextSourceOverflow
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_context_document import EnrichmentContextDocument
    from ..models.enrichment_context_query import EnrichmentContextQuery


T = TypeVar("T", bound="EnrichmentContextSource")


@_attrs_define
class EnrichmentContextSource:
    """One named context source: exactly one of `document` (a referenced row) or `query` (the bounded matching set of a
    parameterized equality query, including the empty set).

        Attributes:
            fields (list[str]): Projected fields consumed by the producer (dotted paths). Only these fields participate in
                the input fingerprint and invalidation. Fields starting with `_` are reserved and rejected.
            document (EnrichmentContextDocument | Unset):
            query (EnrichmentContextQuery | Unset):
            required (bool | Unset): Document sources only: a missing referenced document fails the rebuild explicitly
                instead of rendering null. Default: False.
            max_documents (int | Unset): Query sources only: maximum matching members. Exceeding it fails explicitly;
                truncated context is never published. Default: 100.
            max_bytes (int | Unset): Maximum rendered bytes for this source. Default: 1048576.
            overflow (EnrichmentContextSourceOverflow | Unset):  Default: EnrichmentContextSourceOverflow.ERROR.
    """

    fields: list[str]
    document: EnrichmentContextDocument | Unset = UNSET
    query: EnrichmentContextQuery | Unset = UNSET
    required: bool | Unset = False
    max_documents: int | Unset = 100
    max_bytes: int | Unset = 1048576
    overflow: EnrichmentContextSourceOverflow | Unset = EnrichmentContextSourceOverflow.ERROR

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

        max_bytes = self.max_bytes

        overflow: str | Unset = UNSET
        if not isinstance(self.overflow, Unset):
            overflow = self.overflow.value

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
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes
        if overflow is not UNSET:
            field_dict["overflow"] = overflow

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

        max_bytes = d.pop("max_bytes", UNSET)

        _overflow = d.pop("overflow", UNSET)
        overflow: EnrichmentContextSourceOverflow | Unset
        if isinstance(_overflow, Unset):
            overflow = UNSET
        else:
            overflow = EnrichmentContextSourceOverflow(_overflow)

        enrichment_context_source = cls(
            fields=fields,
            document=document,
            query=query,
            required=required,
            max_documents=max_documents,
            max_bytes=max_bytes,
            overflow=overflow,
        )

        return enrichment_context_source
