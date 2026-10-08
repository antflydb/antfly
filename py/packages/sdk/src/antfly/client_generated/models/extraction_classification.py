from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionClassification")


@_attrs_define
class ExtractionClassification:
    """
    Attributes:
        name (str):
        label (str):
        score (float | Unset):
        similarity (float | Unset): Raw cosine similarity for embedding classifiers; not a probability.
    """

    name: str
    label: str
    score: float | Unset = UNSET
    similarity: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        label = self.label

        score = self.score

        similarity = self.similarity

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "label": label,
            }
        )
        if score is not UNSET:
            field_dict["score"] = score
        if similarity is not UNSET:
            field_dict["similarity"] = similarity

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        label = d.pop("label")

        score = d.pop("score", UNSET)

        similarity = d.pop("similarity", UNSET)

        extraction_classification = cls(
            name=name,
            label=label,
            score=score,
            similarity=similarity,
        )

        extraction_classification.additional_properties = d
        return extraction_classification

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
