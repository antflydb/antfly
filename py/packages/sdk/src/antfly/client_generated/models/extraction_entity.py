from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionEntity")


@_attrs_define
class ExtractionEntity:
    """
    Attributes:
        label (str):
        text (str):
        start (int | Unset):
        end (int | Unset):
        score (float | Unset):
    """

    label: str
    text: str
    start: int | Unset = UNSET
    end: int | Unset = UNSET
    score: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        label = self.label

        text = self.text

        start = self.start

        end = self.end

        score = self.score

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "label": label,
                "text": text,
            }
        )
        if start is not UNSET:
            field_dict["start"] = start
        if end is not UNSET:
            field_dict["end"] = end
        if score is not UNSET:
            field_dict["score"] = score

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        label = d.pop("label")

        text = d.pop("text")

        start = d.pop("start", UNSET)

        end = d.pop("end", UNSET)

        score = d.pop("score", UNSET)

        extraction_entity = cls(
            label=label,
            text=text,
            start=start,
            end=end,
            score=score,
        )

        extraction_entity.additional_properties = d
        return extraction_entity

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
