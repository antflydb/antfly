from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_entity_page_items_item_entity_visibility import MemoryEntityPageItemsItemEntityVisibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_page_items_item_entity_evidence_item import MemoryEntityPageItemsItemEntityEvidenceItem


T = TypeVar("T", bound="MemoryEntityPageItemsItemEntity")


@_attrs_define
class MemoryEntityPageItemsItemEntity:
    """
    Attributes:
        id (str):
        name (str):
        visibility (MemoryEntityPageItemsItemEntityVisibility | Unset):  Default:
            MemoryEntityPageItemsItemEntityVisibility.PRIVATE.
        evidence (list[MemoryEntityPageItemsItemEntityEvidenceItem] | Unset):
        type_ (str | Unset):
        aliases (list[str] | Unset):
        distinct_from (list[str] | Unset):
    """

    id: str
    name: str
    visibility: MemoryEntityPageItemsItemEntityVisibility | Unset = MemoryEntityPageItemsItemEntityVisibility.PRIVATE
    evidence: list[MemoryEntityPageItemsItemEntityEvidenceItem] | Unset = UNSET
    type_: str | Unset = UNSET
    aliases: list[str] | Unset = UNSET
    distinct_from: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = []
            for evidence_item_data in self.evidence:
                evidence_item = evidence_item_data.to_dict()
                evidence.append(evidence_item)

        type_ = self.type_

        aliases: list[str] | Unset = UNSET
        if not isinstance(self.aliases, Unset):
            aliases = self.aliases

        distinct_from: list[str] | Unset = UNSET
        if not isinstance(self.distinct_from, Unset):
            distinct_from = self.distinct_from

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "name": name,
            }
        )
        if visibility is not UNSET:
            field_dict["visibility"] = visibility
        if evidence is not UNSET:
            field_dict["evidence"] = evidence
        if type_ is not UNSET:
            field_dict["type"] = type_
        if aliases is not UNSET:
            field_dict["aliases"] = aliases
        if distinct_from is not UNSET:
            field_dict["distinct_from"] = distinct_from

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_page_items_item_entity_evidence_item import (
            MemoryEntityPageItemsItemEntityEvidenceItem,
        )

        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        _visibility = d.pop("visibility", UNSET)
        visibility: MemoryEntityPageItemsItemEntityVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = MemoryEntityPageItemsItemEntityVisibility(_visibility)

        _evidence = d.pop("evidence", UNSET)
        evidence: list[MemoryEntityPageItemsItemEntityEvidenceItem] | Unset = UNSET
        if _evidence is not UNSET:
            evidence = []
            for evidence_item_data in _evidence:
                evidence_item = MemoryEntityPageItemsItemEntityEvidenceItem.from_dict(evidence_item_data)

                evidence.append(evidence_item)

        type_ = d.pop("type", UNSET)

        aliases = cast(list[str], d.pop("aliases", UNSET))

        distinct_from = cast(list[str], d.pop("distinct_from", UNSET))

        memory_entity_page_items_item_entity = cls(
            id=id,
            name=name,
            visibility=visibility,
            evidence=evidence,
            type_=type_,
            aliases=aliases,
            distinct_from=distinct_from,
        )

        return memory_entity_page_items_item_entity
