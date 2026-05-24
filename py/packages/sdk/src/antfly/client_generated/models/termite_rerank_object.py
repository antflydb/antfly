from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_rerank_object_object import TermiteRerankObjectObject

T = TypeVar("T", bound="TermiteRerankObject")


@_attrs_define
class TermiteRerankObject:
    """
    Attributes:
        object_ (TermiteRerankObjectObject):
        index (int): Original prompt index.
        score (float): Relevance score for this prompt.
    """

    object_: TermiteRerankObjectObject
    index: int
    score: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        score = self.score

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "score": score,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        object_ = TermiteRerankObjectObject(d.pop("object"))

        index = d.pop("index")

        score = d.pop("score")

        termite_rerank_object = cls(
            object_=object_,
            index=index,
            score=score,
        )

        termite_rerank_object.additional_properties = d
        return termite_rerank_object

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
