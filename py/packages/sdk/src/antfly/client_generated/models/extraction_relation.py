from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_relation_endpoint import ExtractionRelationEndpoint


T = TypeVar("T", bound="ExtractionRelation")


@_attrs_define
class ExtractionRelation:
    """
    Attributes:
        type_ (str):
        source (ExtractionRelationEndpoint | Unset):
        target (ExtractionRelationEndpoint | Unset):
        score (float | Unset):
    """

    type_: str
    source: ExtractionRelationEndpoint | Unset = UNSET
    target: ExtractionRelationEndpoint | Unset = UNSET
    score: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_

        source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.to_dict()

        target: dict[str, Any] | Unset = UNSET
        if not isinstance(self.target, Unset):
            target = self.target.to_dict()

        score = self.score

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if source is not UNSET:
            field_dict["source"] = source
        if target is not UNSET:
            field_dict["target"] = target
        if score is not UNSET:
            field_dict["score"] = score

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_relation_endpoint import ExtractionRelationEndpoint

        d = dict(src_dict)
        type_ = d.pop("type")

        _source = d.pop("source", UNSET)
        source: ExtractionRelationEndpoint | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = ExtractionRelationEndpoint.from_dict(_source)

        _target = d.pop("target", UNSET)
        target: ExtractionRelationEndpoint | Unset
        if isinstance(_target, Unset):
            target = UNSET
        else:
            target = ExtractionRelationEndpoint.from_dict(_target)

        score = d.pop("score", UNSET)

        extraction_relation = cls(
            type_=type_,
            source=source,
            target=target,
            score=score,
        )

        extraction_relation.additional_properties = d
        return extraction_relation

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
