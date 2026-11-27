from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_origin_count import EnrichmentOriginCount


T = TypeVar("T", bound="EnrichmentCounts")


@_attrs_define
class EnrichmentCounts:
    """Enrichment output counts by origin over one page of documents, for
    migration and readiness. Counts are exact for the page; page with
    `next` and sum. They are not a snapshot across ranges.

        Attributes:
            documents (int): Documents counted in this page.
            enrichments (list[EnrichmentOriginCount]):
            next_ (None | str | Unset): Key to resume from, or null when the walk is complete.
            head_version (int | Unset): Serverless only. The published version counted.
            pending_documents (int | Unset): Serverless only. Documents awaiting named enrichment generation (table-wide).
    """

    documents: int
    enrichments: list[EnrichmentOriginCount]
    next_: None | str | Unset = UNSET
    head_version: int | Unset = UNSET
    pending_documents: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        documents = self.documents

        enrichments = []
        for enrichments_item_data in self.enrichments:
            enrichments_item = enrichments_item_data.to_dict()
            enrichments.append(enrichments_item)

        next_: None | str | Unset
        if isinstance(self.next_, Unset):
            next_ = UNSET
        else:
            next_ = self.next_

        head_version = self.head_version

        pending_documents = self.pending_documents

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "documents": documents,
                "enrichments": enrichments,
            }
        )
        if next_ is not UNSET:
            field_dict["next"] = next_
        if head_version is not UNSET:
            field_dict["head_version"] = head_version
        if pending_documents is not UNSET:
            field_dict["pending_documents"] = pending_documents

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_origin_count import EnrichmentOriginCount

        d = dict(src_dict)
        documents = d.pop("documents")

        enrichments = []
        _enrichments = d.pop("enrichments")
        for enrichments_item_data in _enrichments:
            enrichments_item = EnrichmentOriginCount.from_dict(enrichments_item_data)

            enrichments.append(enrichments_item)

        def _parse_next_(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_ = _parse_next_(d.pop("next", UNSET))

        head_version = d.pop("head_version", UNSET)

        pending_documents = d.pop("pending_documents", UNSET)

        enrichment_counts = cls(
            documents=documents,
            enrichments=enrichments,
            next_=next_,
            head_version=head_version,
            pending_documents=pending_documents,
        )

        enrichment_counts.additional_properties = d
        return enrichment_counts

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
