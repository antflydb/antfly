from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_recognize_object_object import TermiteRecognizeObjectObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_recognize_entity import TermiteRecognizeEntity
    from ..models.termite_relation import TermiteRelation


T = TypeVar("T", bound="TermiteRecognizeObject")


@_attrs_define
class TermiteRecognizeObject:
    """
    Attributes:
        object_ (TermiteRecognizeObjectObject):
        index (int): Original input text index.
        entities (list[TermiteRecognizeEntity]): Entities recognized for this input text.
        relations (list[TermiteRelation] | Unset): Relations recognized for this input text. Only present when using
            a model with 'relations' capability (GLiNER multitask, REBEL).
    """

    object_: TermiteRecognizeObjectObject
    index: int
    entities: list[TermiteRecognizeEntity]
    relations: list[TermiteRelation] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        entities = []
        for entities_item_data in self.entities:
            entities_item = entities_item_data.to_dict()
            entities.append(entities_item)

        relations: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.relations, Unset):
            relations = []
            for relations_item_data in self.relations:
                relations_item = relations_item_data.to_dict()
                relations.append(relations_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "entities": entities,
            }
        )
        if relations is not UNSET:
            field_dict["relations"] = relations

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_recognize_entity import TermiteRecognizeEntity
        from ..models.termite_relation import TermiteRelation

        d = dict(src_dict)
        object_ = TermiteRecognizeObjectObject(d.pop("object"))

        index = d.pop("index")

        entities = []
        _entities = d.pop("entities")
        for entities_item_data in _entities:
            entities_item = TermiteRecognizeEntity.from_dict(entities_item_data)

            entities.append(entities_item)

        _relations = d.pop("relations", UNSET)
        relations: list[TermiteRelation] | Unset = UNSET
        if _relations is not UNSET:
            relations = []
            for relations_item_data in _relations:
                relations_item = TermiteRelation.from_dict(relations_item_data)

                relations.append(relations_item)

        termite_recognize_object = cls(
            object_=object_,
            index=index,
            entities=entities,
            relations=relations,
        )

        termite_recognize_object.additional_properties = d
        return termite_recognize_object

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
