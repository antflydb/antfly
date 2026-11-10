from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_finding_kind import MemoryFindingKind
from ..models.memory_finding_suggested_action import MemoryFindingSuggestedAction
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryFinding")


@_attrs_define
class MemoryFinding:
    """
    Attributes:
        kind (MemoryFindingKind):
        reason (str):
        suggested_action (MemoryFindingSuggestedAction):
        memory_ids (list[str] | Unset):
        entity_id (None | str | Unset):
        target_id (None | str | Unset):
    """

    kind: MemoryFindingKind
    reason: str
    suggested_action: MemoryFindingSuggestedAction
    memory_ids: list[str] | Unset = UNSET
    entity_id: None | str | Unset = UNSET
    target_id: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        reason = self.reason

        suggested_action = self.suggested_action.value

        memory_ids: list[str] | Unset = UNSET
        if not isinstance(self.memory_ids, Unset):
            memory_ids = self.memory_ids

        entity_id: None | str | Unset
        if isinstance(self.entity_id, Unset):
            entity_id = UNSET
        else:
            entity_id = self.entity_id

        target_id: None | str | Unset
        if isinstance(self.target_id, Unset):
            target_id = UNSET
        else:
            target_id = self.target_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "reason": reason,
                "suggested_action": suggested_action,
            }
        )
        if memory_ids is not UNSET:
            field_dict["memory_ids"] = memory_ids
        if entity_id is not UNSET:
            field_dict["entity_id"] = entity_id
        if target_id is not UNSET:
            field_dict["target_id"] = target_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = MemoryFindingKind(d.pop("kind"))

        reason = d.pop("reason")

        suggested_action = MemoryFindingSuggestedAction(d.pop("suggested_action"))

        memory_ids = cast(list[str], d.pop("memory_ids", UNSET))

        def _parse_entity_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        entity_id = _parse_entity_id(d.pop("entity_id", UNSET))

        def _parse_target_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        target_id = _parse_target_id(d.pop("target_id", UNSET))

        memory_finding = cls(
            kind=kind,
            reason=reason,
            suggested_action=suggested_action,
            memory_ids=memory_ids,
            entity_id=entity_id,
            target_id=target_id,
        )

        return memory_finding
