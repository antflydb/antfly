from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decider_cascade_budget import DeciderCascadeBudget
    from ..models.decider_cascade_stage import DeciderCascadeStage


T = TypeVar("T", bound="DeciderCascade")


@_attrs_define
class DeciderCascade:
    """Required with decision_method=cascade, which allows no transport, model or rate-limit fields of its own. The first
    stage answers every input. Each later stage runs only for inputs whose previous answer falls inside its probability
    band, and all escalating inputs share one call. The final answer is the last answering stage's. Escalation applies
    to successful answers only; it is not failover. Probabilities are not calibrated across models, so set bands per
    model on labelled data.

        Attributes:
            stages (list[DeciderCascadeStage]):
            budget (DeciderCascadeBudget | Unset):
    """

    stages: list[DeciderCascadeStage]
    budget: DeciderCascadeBudget | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        stages = []
        for stages_item_data in self.stages:
            stages_item = stages_item_data.to_dict()
            stages.append(stages_item)

        budget: dict[str, Any] | Unset = UNSET
        if not isinstance(self.budget, Unset):
            budget = self.budget.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "stages": stages,
            }
        )
        if budget is not UNSET:
            field_dict["budget"] = budget

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decider_cascade_budget import DeciderCascadeBudget
        from ..models.decider_cascade_stage import DeciderCascadeStage

        d = dict(src_dict)
        stages = []
        _stages = d.pop("stages")
        for stages_item_data in _stages:
            stages_item = DeciderCascadeStage.from_dict(stages_item_data)

            stages.append(stages_item)

        _budget = d.pop("budget", UNSET)
        budget: DeciderCascadeBudget | Unset
        if isinstance(_budget, Unset):
            budget = UNSET
        else:
            budget = DeciderCascadeBudget.from_dict(_budget)

        decider_cascade = cls(
            stages=stages,
            budget=budget,
        )

        return decider_cascade
