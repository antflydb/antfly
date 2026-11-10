from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryAccess")


@_attrs_define
class MemoryAccess:
    """
    Attributes:
        memory_ids (list[str]):
    """

    memory_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        memory_ids = self.memory_ids

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "memory_ids": memory_ids,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        memory_ids = cast(list[str], d.pop("memory_ids"))

        memory_access = cls(
            memory_ids=memory_ids,
        )

        return memory_access
