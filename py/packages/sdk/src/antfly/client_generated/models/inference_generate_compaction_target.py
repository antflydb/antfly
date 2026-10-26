from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="InferenceGenerateCompactionTarget")


@_attrs_define
class InferenceGenerateCompactionTarget:
    """
    Attributes:
        message_index (int):
        part_index (int): Zero-based text part; string message content is part zero.
    """

    message_index: int
    part_index: int

    def to_dict(self) -> dict[str, Any]:
        message_index = self.message_index

        part_index = self.part_index

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "message_index": message_index,
                "part_index": part_index,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        message_index = d.pop("message_index")

        part_index = d.pop("part_index")

        inference_generate_compaction_target = cls(
            message_index=message_index,
            part_index=part_index,
        )

        return inference_generate_compaction_target
