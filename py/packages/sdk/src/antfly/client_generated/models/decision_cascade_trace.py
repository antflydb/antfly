from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.decision_cascade_stage_trace import DecisionCascadeStageTrace


T = TypeVar("T", bound="DecisionCascadeTrace")


@_attrs_define
class DecisionCascadeTrace:
    """Present when a cascade decider answered. Lists the stages that ran for this input, in order. The top-level model and
    answers come from the answering stage, and usage is the sum over these stages.

        Attributes:
            decider (str): The configured cascade name.
            definition_sha256 (str): Identity of the cascade definition, excluding credentials. Any change to stages, bands,
                failure policy or budget changes it.
            answered_by (str): Name of the stage whose answer is final.
            stages (list[DecisionCascadeStageTrace]):
    """

    decider: str
    definition_sha256: str
    answered_by: str
    stages: list[DecisionCascadeStageTrace]

    def to_dict(self) -> dict[str, Any]:
        decider = self.decider

        definition_sha256 = self.definition_sha256

        answered_by = self.answered_by

        stages = []
        for stages_item_data in self.stages:
            stages_item = stages_item_data.to_dict()
            stages.append(stages_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "decider": decider,
                "definition_sha256": definition_sha256,
                "answered_by": answered_by,
                "stages": stages,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_cascade_stage_trace import DecisionCascadeStageTrace

        d = dict(src_dict)
        decider = d.pop("decider")

        definition_sha256 = d.pop("definition_sha256")

        answered_by = d.pop("answered_by")

        stages = []
        _stages = d.pop("stages")
        for stages_item_data in _stages:
            stages_item = DecisionCascadeStageTrace.from_dict(stages_item_data)

            stages.append(stages_item)

        decision_cascade_trace = cls(
            decider=decider,
            definition_sha256=definition_sha256,
            answered_by=answered_by,
            stages=stages,
        )

        return decision_cascade_trace
