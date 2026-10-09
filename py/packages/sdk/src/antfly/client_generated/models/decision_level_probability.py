from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="DecisionLevelProbability")


@_attrs_define
class DecisionLevelProbability:
    """
    Attributes:
        label (str):
        value (int):
        probability (float):
    """

    label: str
    value: int
    probability: float

    def to_dict(self) -> dict[str, Any]:
        label = self.label

        value = self.value

        probability = self.probability

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "label": label,
                "value": value,
                "probability": probability,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        label = d.pop("label")

        value = d.pop("value")

        probability = d.pop("probability")

        decision_level_probability = cls(
            label=label,
            value=value,
            probability=probability,
        )

        return decision_level_probability
