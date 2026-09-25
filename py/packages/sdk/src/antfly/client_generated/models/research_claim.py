from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchClaim")


@_attrs_define
class ResearchClaim:
    """
    Attributes:
        text (str): One factual claim made by a researcher.
        evidence_ids (list[str] | Unset): Evidence registry IDs that support the claim.
    """

    text: str
    evidence_ids: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        evidence_ids: list[str] | Unset = UNSET
        if not isinstance(self.evidence_ids, Unset):
            evidence_ids = self.evidence_ids

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
            }
        )
        if evidence_ids is not UNSET:
            field_dict["evidence_ids"] = evidence_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text = d.pop("text")

        evidence_ids = cast(list[str], d.pop("evidence_ids", UNSET))

        research_claim = cls(
            text=text,
            evidence_ids=evidence_ids,
        )

        research_claim.additional_properties = d
        return research_claim

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
