from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="DecisionSimilarity")


@_attrs_define
class DecisionSimilarity:
    """
    Attributes:
        value (str):
        similarity (float):
    """

    value: str
    similarity: float

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        similarity = self.similarity

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "value": value,
                "similarity": similarity,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        value = d.pop("value")

        similarity = d.pop("similarity")

        decision_similarity = cls(
            value=value,
            similarity=similarity,
        )

        return decision_similarity
