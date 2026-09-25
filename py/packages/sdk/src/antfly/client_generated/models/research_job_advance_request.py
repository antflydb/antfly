from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchJobAdvanceRequest")


@_attrs_define
class ResearchJobAdvanceRequest:
    """
    Attributes:
        max_phases (int | Unset): Maximum phases to run in this advance. Each phase checkpoints before the next starts.
            Default: 1.
    """

    max_phases: int | Unset = 1
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        max_phases = self.max_phases

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if max_phases is not UNSET:
            field_dict["max_phases"] = max_phases

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_phases = d.pop("max_phases", UNSET)

        research_job_advance_request = cls(
            max_phases=max_phases,
        )

        research_job_advance_request.additional_properties = d
        return research_job_advance_request

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
