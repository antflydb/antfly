from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_evaluation_judge_input import MemoryEvaluationJudgeInput


T = TypeVar("T", bound="MemoryEvaluationJudgeTask")


@_attrs_define
class MemoryEvaluationJudgeTask:
    """
    Attributes:
        case_id (str):
        case_revision (int):
        input_fingerprint (str):
        input_ (MemoryEvaluationJudgeInput):
    """

    case_id: str
    case_revision: int
    input_fingerprint: str
    input_: MemoryEvaluationJudgeInput

    def to_dict(self) -> dict[str, Any]:
        case_id = self.case_id

        case_revision = self.case_revision

        input_fingerprint = self.input_fingerprint

        input_ = self.input_.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "case_id": case_id,
                "case_revision": case_revision,
                "input_fingerprint": input_fingerprint,
                "input": input_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_judge_input import MemoryEvaluationJudgeInput

        d = dict(src_dict)
        case_id = d.pop("case_id")

        case_revision = d.pop("case_revision")

        input_fingerprint = d.pop("input_fingerprint")

        input_ = MemoryEvaluationJudgeInput.from_dict(d.pop("input"))

        memory_evaluation_judge_task = cls(
            case_id=case_id,
            case_revision=case_revision,
            input_fingerprint=input_fingerprint,
            input_=input_,
        )

        return memory_evaluation_judge_task
