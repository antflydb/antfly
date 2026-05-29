from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceExtractFieldValue")


@_attrs_define
class InferenceExtractFieldValue:
    """
    Attributes:
        value (str): The extracted text value Example: John Smith.
        score (float | Unset): Confidence score (only present when include_confidence=true) Example: 0.95.
        start (int | Unset): Character offset where value begins (only present when include_spans=true)
        end (int | Unset): Character offset where value ends (only present when include_spans=true) Example: 10.
    """

    value: str
    score: float | Unset = UNSET
    start: int | Unset = UNSET
    end: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        score = self.score

        start = self.start

        end = self.end

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "value": value,
            }
        )
        if score is not UNSET:
            field_dict["score"] = score
        if start is not UNSET:
            field_dict["start"] = start
        if end is not UNSET:
            field_dict["end"] = end

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        value = d.pop("value")

        score = d.pop("score", UNSET)

        start = d.pop("start", UNSET)

        end = d.pop("end", UNSET)

        inference_extract_field_value = cls(
            value=value,
            score=score,
            start=start,
            end=end,
        )

        inference_extract_field_value.additional_properties = d
        return inference_extract_field_value

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
