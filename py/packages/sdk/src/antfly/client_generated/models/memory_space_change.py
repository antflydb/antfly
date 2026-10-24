from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_space_change_kind import MemorySpaceChangeKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_space_change_target_type_0 import MemorySpaceChangeTargetType0


T = TypeVar("T", bound="MemorySpaceChange")


@_attrs_define
class MemorySpaceChange:
    """
    Attributes:
        id (str):
        kind (MemorySpaceChangeKind):
        target (MemorySpaceChangeTargetType0 | None):
        revision (int):
        recorded_at (int):
        branch (str | Unset):
    """

    id: str
    kind: MemorySpaceChangeKind
    target: MemorySpaceChangeTargetType0 | None
    revision: int
    recorded_at: int
    branch: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_space_change_target_type_0 import MemorySpaceChangeTargetType0

        id = self.id

        kind = self.kind.value

        target: dict[str, Any] | None
        if isinstance(self.target, MemorySpaceChangeTargetType0):
            target = self.target.to_dict()
        else:
            target = self.target

        revision = self.revision

        recorded_at = self.recorded_at

        branch = self.branch

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "kind": kind,
                "target": target,
                "revision": revision,
                "recorded_at": recorded_at,
            }
        )
        if branch is not UNSET:
            field_dict["branch"] = branch

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_space_change_target_type_0 import MemorySpaceChangeTargetType0

        d = dict(src_dict)
        id = d.pop("id")

        kind = MemorySpaceChangeKind(d.pop("kind"))

        def _parse_target(data: object) -> MemorySpaceChangeTargetType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                target_type_0 = MemorySpaceChangeTargetType0.from_dict(data)

                return target_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemorySpaceChangeTargetType0 | None, data)

        target = _parse_target(d.pop("target"))

        revision = d.pop("revision")

        recorded_at = d.pop("recorded_at")

        branch = d.pop("branch", UNSET)

        memory_space_change = cls(
            id=id,
            kind=kind,
            target=target,
            revision=revision,
            recorded_at=recorded_at,
            branch=branch,
        )

        return memory_space_change
