from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ConstrainedGenerationBatch")


@_attrs_define
class ConstrainedGenerationBatch:
    """
    Attributes:
        max_items_per_call (int | Unset):  Default: 8.
        max_input_bytes (int | Unset):  Default: 65536.
    """

    max_items_per_call: int | Unset = 8
    max_input_bytes: int | Unset = 65536

    def to_dict(self) -> dict[str, Any]:
        max_items_per_call = self.max_items_per_call

        max_input_bytes = self.max_input_bytes

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_items_per_call is not UNSET:
            field_dict["max_items_per_call"] = max_items_per_call
        if max_input_bytes is not UNSET:
            field_dict["max_input_bytes"] = max_input_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_items_per_call = d.pop("max_items_per_call", UNSET)

        max_input_bytes = d.pop("max_input_bytes", UNSET)

        constrained_generation_batch = cls(
            max_items_per_call=max_items_per_call,
            max_input_bytes=max_input_bytes,
        )

        return constrained_generation_batch
