from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_space_change_target_type_0_kind import MemorySpaceChangeTargetType0Kind

T = TypeVar("T", bound="MemorySpaceChangeTargetType0")


@_attrs_define
class MemorySpaceChangeTargetType0:
    """
    Attributes:
        kind (MemorySpaceChangeTargetType0Kind):
        id (str):
        revision (int):
    """

    kind: MemorySpaceChangeTargetType0Kind
    id: str
    revision: int

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        id = self.id

        revision = self.revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "id": id,
                "revision": revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = MemorySpaceChangeTargetType0Kind(d.pop("kind"))

        id = d.pop("id")

        revision = d.pop("revision")

        memory_space_change_target_type_0 = cls(
            kind=kind,
            id=id,
            revision=revision,
        )

        return memory_space_change_target_type_0
