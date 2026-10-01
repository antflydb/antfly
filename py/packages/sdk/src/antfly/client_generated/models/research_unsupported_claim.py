from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchUnsupportedClaim")


@_attrs_define
class ResearchUnsupportedClaim:
    """
    Attributes:
        section_index (int | Unset): Section containing the claim.
        text (str | Unset): Claim text.
        evidence_ids (list[str] | Unset): Evidence the claim cited.
        reason (str | Unset): Why the claim is unsupported.
    """

    section_index: int | Unset = UNSET
    text: str | Unset = UNSET
    evidence_ids: list[str] | Unset = UNSET
    reason: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        section_index = self.section_index

        text = self.text

        evidence_ids: list[str] | Unset = UNSET
        if not isinstance(self.evidence_ids, Unset):
            evidence_ids = self.evidence_ids

        reason = self.reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if section_index is not UNSET:
            field_dict["section_index"] = section_index
        if text is not UNSET:
            field_dict["text"] = text
        if evidence_ids is not UNSET:
            field_dict["evidence_ids"] = evidence_ids
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        section_index = d.pop("section_index", UNSET)

        text = d.pop("text", UNSET)

        evidence_ids = cast(list[str], d.pop("evidence_ids", UNSET))

        reason = d.pop("reason", UNSET)

        research_unsupported_claim = cls(
            section_index=section_index,
            text=text,
            evidence_ids=evidence_ids,
            reason=reason,
        )

        research_unsupported_claim.additional_properties = d
        return research_unsupported_claim

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
