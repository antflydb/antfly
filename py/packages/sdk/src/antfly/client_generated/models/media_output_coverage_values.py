from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="MediaOutputCoverageValues")


@_attrs_define
class MediaOutputCoverageValues:
    """
    Attributes:
        complete (int | Unset):
        failed (int | Unset):
        terminal (int | Unset):
        degraded (int | Unset):
    """

    complete: int | Unset = UNSET
    failed: int | Unset = UNSET
    terminal: int | Unset = UNSET
    degraded: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        complete = self.complete

        failed = self.failed

        terminal = self.terminal

        degraded = self.degraded

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if complete is not UNSET:
            field_dict["complete"] = complete
        if failed is not UNSET:
            field_dict["failed"] = failed
        if terminal is not UNSET:
            field_dict["terminal"] = terminal
        if degraded is not UNSET:
            field_dict["degraded"] = degraded

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        complete = d.pop("complete", UNSET)

        failed = d.pop("failed", UNSET)

        terminal = d.pop("terminal", UNSET)

        degraded = d.pop("degraded", UNSET)

        media_output_coverage_values = cls(
            complete=complete,
            failed=failed,
            terminal=terminal,
            degraded=degraded,
        )

        media_output_coverage_values.additional_properties = d
        return media_output_coverage_values

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
