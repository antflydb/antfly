from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_evaluation_judge_input_task import MemoryEvaluationJudgeInputTask
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEvaluationJudgeInput")


@_attrs_define
class MemoryEvaluationJudgeInput:
    """
    Attributes:
        task (MemoryEvaluationJudgeInputTask):
        input_ (Any):
        prompt_revision (str | Unset):
    """

    task: MemoryEvaluationJudgeInputTask
    input_: Any
    prompt_revision: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        task = self.task.value

        input_ = self.input_

        prompt_revision = self.prompt_revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "task": task,
                "input": input_,
            }
        )
        if prompt_revision is not UNSET:
            field_dict["prompt_revision"] = prompt_revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        task = MemoryEvaluationJudgeInputTask(d.pop("task"))

        input_ = d.pop("input")

        prompt_revision = d.pop("prompt_revision", UNSET)

        memory_evaluation_judge_input = cls(
            task=task,
            input_=input_,
            prompt_revision=prompt_revision,
        )

        return memory_evaluation_judge_input
