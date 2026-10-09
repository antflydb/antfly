from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="DecisionChoiceProbability")


@_attrs_define
class DecisionChoiceProbability:
    """
    Attributes:
        value (str):
        probability (float):
    """

    value: str
    probability: float

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        probability = self.probability

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "value": value,
                "probability": probability,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        value = d.pop("value")

        probability = d.pop("probability")

        decision_choice_probability = cls(
            value=value,
            probability=probability,
        )

        return decision_choice_probability
