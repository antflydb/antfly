from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.provenance_envelope_links_item import ProvenanceEnvelopeLinksItem


T = TypeVar("T", bound="ProvenanceEnvelope")


@_attrs_define
class ProvenanceEnvelope:
    """
    Attributes:
        schema_version (int | Unset):
        links (list[ProvenanceEnvelopeLinksItem] | Unset):
    """

    schema_version: int | Unset = UNSET
    links: list[ProvenanceEnvelopeLinksItem] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        schema_version = self.schema_version

        links: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.links, Unset):
            links = []
            for links_item_data in self.links:
                links_item = links_item_data.to_dict()
                links.append(links_item)

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version
        if links is not UNSET:
            field_dict["links"] = links

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.provenance_envelope_links_item import ProvenanceEnvelopeLinksItem

        d = dict(src_dict)
        schema_version = d.pop("schema_version", UNSET)

        _links = d.pop("links", UNSET)
        links: list[ProvenanceEnvelopeLinksItem] | Unset = UNSET
        if _links is not UNSET:
            links = []
            for links_item_data in _links:
                links_item = ProvenanceEnvelopeLinksItem.from_dict(links_item_data)

                links.append(links_item)

        provenance_envelope = cls(
            schema_version=schema_version,
            links=links,
        )

        return provenance_envelope
