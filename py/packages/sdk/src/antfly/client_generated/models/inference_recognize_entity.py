from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferenceRecognizeEntity")


@_attrs_define
class InferenceRecognizeEntity:
    """
    Attributes:
        text (str): The entity text Example: John Smith.
        label (str): Entity type (PER, ORG, LOC, MISC) Example: PER.
        start (int): Character offset where entity begins
        end (int): Character offset where entity ends (exclusive) Example: 10.
        score (float): Confidence score (0.0 to 1.0) Example: 0.99.
    """

    text: str
    label: str
    start: int
    end: int
    score: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        label = self.label

        start = self.start

        end = self.end

        score = self.score

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "label": label,
                "start": start,
                "end": end,
                "score": score,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        text = d.pop("text")

        label = d.pop("label")

        start = d.pop("start")

        end = d.pop("end")

        score = d.pop("score")

        inference_recognize_entity = cls(
            text=text,
            label=label,
            start=start,
            end=end,
            score=score,
        )

        inference_recognize_entity.additional_properties = d
        return inference_recognize_entity

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
