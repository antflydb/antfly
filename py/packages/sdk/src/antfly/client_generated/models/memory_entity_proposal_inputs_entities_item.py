from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryEntityProposalInputsEntitiesItem")


@_attrs_define
class MemoryEntityProposalInputsEntitiesItem:
    """
    Attributes:
        id (str):
        revision (int):
    """

    id: str
    revision: int

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        revision = self.revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "revision": revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        revision = d.pop("revision")

        memory_entity_proposal_inputs_entities_item = cls(
            id=id,
            revision=revision,
        )

        return memory_entity_proposal_inputs_entities_item
