from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchCitation")


@_attrs_define
class ResearchCitation:
    """
    Attributes:
        marker (str): Marker as written in the report, for example `[E3]`.
        evidence_id (str): Resolved evidence ID.
        section_index (int | Unset): Section containing the marker. -1 is the summary.
        count (int | Unset): Number of occurrences in that section.
    """

    marker: str
    evidence_id: str
    section_index: int | Unset = UNSET
    count: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        marker = self.marker

        evidence_id = self.evidence_id

        section_index = self.section_index

        count = self.count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "marker": marker,
                "evidence_id": evidence_id,
            }
        )
        if section_index is not UNSET:
            field_dict["section_index"] = section_index
        if count is not UNSET:
            field_dict["count"] = count

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        marker = d.pop("marker")

        evidence_id = d.pop("evidence_id")

        section_index = d.pop("section_index", UNSET)

        count = d.pop("count", UNSET)

        research_citation = cls(
            marker=marker,
            evidence_id=evidence_id,
            section_index=section_index,
            count=count,
        )

        research_citation.additional_properties = d
        return research_citation

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
