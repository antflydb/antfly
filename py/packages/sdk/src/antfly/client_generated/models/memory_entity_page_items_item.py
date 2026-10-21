from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_page_items_item_entity import MemoryEntityPageItemsItemEntity


T = TypeVar("T", bound="MemoryEntityPageItemsItem")


@_attrs_define
class MemoryEntityPageItemsItem:
    """
    Attributes:
        entity (MemoryEntityPageItemsItemEntity):
        owner (str):
        revision (int | Unset):
    """

    entity: MemoryEntityPageItemsItemEntity
    owner: str
    revision: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        entity = self.entity.to_dict()

        owner = self.owner

        revision = self.revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "entity": entity,
                "owner": owner,
            }
        )
        if revision is not UNSET:
            field_dict["revision"] = revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_page_items_item_entity import MemoryEntityPageItemsItemEntity

        d = dict(src_dict)
        entity = MemoryEntityPageItemsItemEntity.from_dict(d.pop("entity"))

        owner = d.pop("owner")

        revision = d.pop("revision", UNSET)

        memory_entity_page_items_item = cls(
            entity=entity,
            owner=owner,
            revision=revision,
        )

        return memory_entity_page_items_item
