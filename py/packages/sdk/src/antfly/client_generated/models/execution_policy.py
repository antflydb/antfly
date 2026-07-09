from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

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

    def to_dict(self) -> dict[str, Any]:
        batch_items = self.batch_items

        batch_bytes = self.batch_bytes

        field_dict: dict[str, Any] = {}

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

        return execution_policy
