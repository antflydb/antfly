from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="CloseRetainedDatasetBody")


@_attrs_define
class CloseRetainedDatasetBody:
    """
    Attributes:
        expected_revision (int):
        cancel_activation (bool | Unset): Explicitly cancel an activating summary and discard partial state. Rejected
            for an active summary; omit or set false to close and archive an active summary. Default: False.
    """

    expected_revision: int
    cancel_activation: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        expected_revision = self.expected_revision

        cancel_activation = self.cancel_activation

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "expected_revision": expected_revision,
            }
        )
        if cancel_activation is not UNSET:
            field_dict["cancel_activation"] = cancel_activation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        expected_revision = d.pop("expected_revision")

        cancel_activation = d.pop("cancel_activation", UNSET)

        close_retained_dataset_body = cls(
            expected_revision=expected_revision,
            cancel_activation=cancel_activation,
        )

        close_retained_dataset_body.additional_properties = d
        return close_retained_dataset_body

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
