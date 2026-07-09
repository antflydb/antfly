from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExecutionPolicy")


@_attrs_define
class ExecutionPolicy:
    """Non-semantic execution policy for one producer or index maintenance operation. These fields tune how work is batched
    and do not change generated artifact identity.

        Attributes:
            batch_items (int | Unset): Maximum items to process in one batch for this operation.
            batch_bytes (int | Unset): Approximate maximum source bytes to process in one batch for this operation.
    """

    batch_items: int | Unset = UNSET
    batch_bytes: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        batch_items = self.batch_items

        batch_bytes = self.batch_bytes

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if batch_items is not UNSET:
            field_dict["batch_items"] = batch_items
        if batch_bytes is not UNSET:
            field_dict["batch_bytes"] = batch_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        batch_items = d.pop("batch_items", UNSET)

        batch_bytes = d.pop("batch_bytes", UNSET)

        execution_policy = cls(
            batch_items=batch_items,
            batch_bytes=batch_bytes,
        )

        execution_policy.additional_properties = d
        return execution_policy

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
