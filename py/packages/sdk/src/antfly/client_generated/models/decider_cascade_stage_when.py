from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="DeciderCascadeStageWhen")


@_attrs_define
class DeciderCascadeStageWhen:
    """Required on every stage except the first, which must omit it. Inclusive band on the previous stage's answer.

    Attributes:
        probability_between (list[float]): Lower and upper bound, lower strictly below upper. The previous stage must
            not be verdict_only.
        question (str | Unset): Measure this question only. Omitted, any question in the band escalates.
        choice (str | Unset): For choice questions, measure this value's probability. Omitted, the selected value's
            probability.
    """

    probability_between: list[float]
    question: str | Unset = UNSET
    choice: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        probability_between = self.probability_between

        question = self.question

        choice = self.choice

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "probability_between": probability_between,
            }
        )
        if question is not UNSET:
            field_dict["question"] = question
        if choice is not UNSET:
            field_dict["choice"] = choice

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        probability_between = cast(list[float], d.pop("probability_between"))

        question = d.pop("question", UNSET)

        choice = d.pop("choice", UNSET)

        decider_cascade_stage_when = cls(
            probability_between=probability_between,
            question=question,
            choice=choice,
        )

        return decider_cascade_stage_when
