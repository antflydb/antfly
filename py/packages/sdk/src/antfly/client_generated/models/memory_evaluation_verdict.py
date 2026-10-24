from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_judge_output import MemoryEvaluationJudgeOutput


T = TypeVar("T", bound="MemoryEvaluationVerdict")


@_attrs_define
class MemoryEvaluationVerdict:
    """
    Attributes:
        case_id (str):
        case_revision (int):
        input_fingerprint (str):
        prediction (MemoryEvaluationJudgeOutput):
        invalid_output (bool | Unset):  Default: False.
    """

    case_id: str
    case_revision: int
    input_fingerprint: str
    prediction: MemoryEvaluationJudgeOutput
    invalid_output: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        case_id = self.case_id

        case_revision = self.case_revision

        input_fingerprint = self.input_fingerprint

        prediction = self.prediction.to_dict()

        invalid_output = self.invalid_output

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "case_id": case_id,
                "case_revision": case_revision,
                "input_fingerprint": input_fingerprint,
                "prediction": prediction,
            }
        )
        if invalid_output is not UNSET:
            field_dict["invalid_output"] = invalid_output

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_judge_output import MemoryEvaluationJudgeOutput

        d = dict(src_dict)
        case_id = d.pop("case_id")

        case_revision = d.pop("case_revision")

        input_fingerprint = d.pop("input_fingerprint")

        prediction = MemoryEvaluationJudgeOutput.from_dict(d.pop("prediction"))

        invalid_output = d.pop("invalid_output", UNSET)

        memory_evaluation_verdict = cls(
            case_id=case_id,
            case_revision=case_revision,
            input_fingerprint=input_fingerprint,
            prediction=prediction,
            invalid_output=invalid_output,
        )

        return memory_evaluation_verdict
