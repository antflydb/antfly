from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.research_unsupported_claim import ResearchUnsupportedClaim


T = TypeVar("T", bound="ResearchVerification")


@_attrs_define
class ResearchVerification:
    """
    Attributes:
        checked_sections (int | Unset): Number of sections checked.
        unresolved_markers (list[str] | Unset): Citation markers that did not resolve to evidence and were removed.
        uncited_sections (list[int] | Unset): Sections without any resolvable citation.
        unsupported (list[ResearchUnsupportedClaim] | Unset): Claims the verifier judged unsupported by their cited
            evidence.
        supported_ratio (float | Unset): Share of checked claims judged supported.
    """

    checked_sections: int | Unset = UNSET
    unresolved_markers: list[str] | Unset = UNSET
    uncited_sections: list[int] | Unset = UNSET
    unsupported: list[ResearchUnsupportedClaim] | Unset = UNSET
    supported_ratio: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        checked_sections = self.checked_sections

        unresolved_markers: list[str] | Unset = UNSET
        if not isinstance(self.unresolved_markers, Unset):
            unresolved_markers = self.unresolved_markers

        uncited_sections: list[int] | Unset = UNSET
        if not isinstance(self.uncited_sections, Unset):
            uncited_sections = self.uncited_sections

        unsupported: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.unsupported, Unset):
            unsupported = []
            for unsupported_item_data in self.unsupported:
                unsupported_item = unsupported_item_data.to_dict()
                unsupported.append(unsupported_item)

        supported_ratio = self.supported_ratio

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if checked_sections is not UNSET:
            field_dict["checked_sections"] = checked_sections
        if unresolved_markers is not UNSET:
            field_dict["unresolved_markers"] = unresolved_markers
        if uncited_sections is not UNSET:
            field_dict["uncited_sections"] = uncited_sections
        if unsupported is not UNSET:
            field_dict["unsupported"] = unsupported
        if supported_ratio is not UNSET:
            field_dict["supported_ratio"] = supported_ratio

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.research_unsupported_claim import ResearchUnsupportedClaim

        d = dict(src_dict)
        checked_sections = d.pop("checked_sections", UNSET)

        unresolved_markers = cast(list[str], d.pop("unresolved_markers", UNSET))

        uncited_sections = cast(list[int], d.pop("uncited_sections", UNSET))

        _unsupported = d.pop("unsupported", UNSET)
        unsupported: list[ResearchUnsupportedClaim] | Unset = UNSET
        if _unsupported is not UNSET:
            unsupported = []
            for unsupported_item_data in _unsupported:
                unsupported_item = ResearchUnsupportedClaim.from_dict(unsupported_item_data)

                unsupported.append(unsupported_item)

        supported_ratio = d.pop("supported_ratio", UNSET)

        research_verification = cls(
            checked_sections=checked_sections,
            unresolved_markers=unresolved_markers,
            uncited_sections=uncited_sections,
            unsupported=unsupported,
            supported_ratio=supported_ratio,
        )

        research_verification.additional_properties = d
        return research_verification

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
