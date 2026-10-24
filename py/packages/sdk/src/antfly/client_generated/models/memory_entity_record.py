from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity import MemoryEntity


T = TypeVar("T", bound="MemoryEntityRecord")


@_attrs_define
class MemoryEntityRecord:
    """
    Attributes:
        entity (MemoryEntity):
        owner (str):
        revision (int | Unset):  Default: 0.
        recorded_at (int | None | Unset):
    """

    entity: MemoryEntity
    owner: str
    revision: int | Unset = 0
    recorded_at: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        entity = self.entity.to_dict()

        owner = self.owner

        revision = self.revision

        recorded_at: int | None | Unset
        if isinstance(self.recorded_at, Unset):
            recorded_at = UNSET
        else:
            recorded_at = self.recorded_at

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "entity": entity,
                "owner": owner,
            }
        )
        if revision is not UNSET:
            field_dict["revision"] = revision
        if recorded_at is not UNSET:
            field_dict["recorded_at"] = recorded_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity import MemoryEntity

        d = dict(src_dict)
        entity = MemoryEntity.from_dict(d.pop("entity"))

        owner = d.pop("owner")

        revision = d.pop("revision", UNSET)

        def _parse_recorded_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        recorded_at = _parse_recorded_at(d.pop("recorded_at", UNSET))

        memory_entity_record = cls(
            entity=entity,
            owner=owner,
            revision=revision,
            recorded_at=recorded_at,
        )

        return memory_entity_record
