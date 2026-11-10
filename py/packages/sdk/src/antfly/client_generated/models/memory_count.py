from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryCount")


@_attrs_define
class MemoryCount:
    """
    Attributes:
        name (str):
        memories (int):
    """

    name: str
    memories: int

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        memories = self.memories

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "memories": memories,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        memories = d.pop("memories")

        memory_count = cls(
            name=name,
            memories=memories,
        )

        return memory_count
