from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.termite_recognize_entity import TermiteRecognizeEntity


T = TypeVar("T", bound="TermiteRelation")


@_attrs_define
class TermiteRelation:
    """
    Attributes:
        head (TermiteRecognizeEntity):
        tail (TermiteRecognizeEntity):
        label (str): The relationship type Example: founded.
        score (float): Confidence score for the relation (0.0 to 1.0) Example: 0.95.
    """

    head: TermiteRecognizeEntity
    tail: TermiteRecognizeEntity
    label: str
    score: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        head = self.head.to_dict()

        tail = self.tail.to_dict()

        label = self.label

        score = self.score

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "head": head,
                "tail": tail,
                "label": label,
                "score": score,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_recognize_entity import TermiteRecognizeEntity

        d = dict(src_dict)
        head = TermiteRecognizeEntity.from_dict(d.pop("head"))

        tail = TermiteRecognizeEntity.from_dict(d.pop("tail"))

        label = d.pop("label")

        score = d.pop("score")

        termite_relation = cls(
            head=head,
            tail=tail,
            label=label,
            score=score,
        )

        termite_relation.additional_properties = d
        return termite_relation

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
