from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEvaluationCounts")


@_attrs_define
class MemoryEvaluationCounts:
    """
    Attributes:
        cases (int | Unset):  Default: 0.
        binary_supersession_cases (int | Unset):  Default: 0.
        graded (int | Unset):  Default: 0.
        abstained (int | Unset):  Default: 0.
        invalid_outputs (int | Unset):  Default: 0.
        unavailable (int | Unset):  Default: 0.
        correct (int | Unset):  Default: 0.
        true_positive (int | Unset):  Default: 0.
        false_positive (int | Unset):  Default: 0.
        false_negative (int | Unset):  Default: 0.
        true_negative (int | Unset):  Default: 0.
    """

    cases: int | Unset = 0
    binary_supersession_cases: int | Unset = 0
    graded: int | Unset = 0
    abstained: int | Unset = 0
    invalid_outputs: int | Unset = 0
    unavailable: int | Unset = 0
    correct: int | Unset = 0
    true_positive: int | Unset = 0
    false_positive: int | Unset = 0
    false_negative: int | Unset = 0
    true_negative: int | Unset = 0

    def to_dict(self) -> dict[str, Any]:
        cases = self.cases

        binary_supersession_cases = self.binary_supersession_cases

        graded = self.graded

        abstained = self.abstained

        invalid_outputs = self.invalid_outputs

        unavailable = self.unavailable

        correct = self.correct

        true_positive = self.true_positive

        false_positive = self.false_positive

        false_negative = self.false_negative

        true_negative = self.true_negative

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if cases is not UNSET:
            field_dict["cases"] = cases
        if binary_supersession_cases is not UNSET:
            field_dict["binary_supersession_cases"] = binary_supersession_cases
        if graded is not UNSET:
            field_dict["graded"] = graded
        if abstained is not UNSET:
            field_dict["abstained"] = abstained
        if invalid_outputs is not UNSET:
            field_dict["invalid_outputs"] = invalid_outputs
        if unavailable is not UNSET:
            field_dict["unavailable"] = unavailable
        if correct is not UNSET:
            field_dict["correct"] = correct
        if true_positive is not UNSET:
            field_dict["true_positive"] = true_positive
        if false_positive is not UNSET:
            field_dict["false_positive"] = false_positive
        if false_negative is not UNSET:
            field_dict["false_negative"] = false_negative
        if true_negative is not UNSET:
            field_dict["true_negative"] = true_negative

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        cases = d.pop("cases", UNSET)

        binary_supersession_cases = d.pop("binary_supersession_cases", UNSET)

        graded = d.pop("graded", UNSET)

        abstained = d.pop("abstained", UNSET)

        invalid_outputs = d.pop("invalid_outputs", UNSET)

        unavailable = d.pop("unavailable", UNSET)

        correct = d.pop("correct", UNSET)

        true_positive = d.pop("true_positive", UNSET)

        false_positive = d.pop("false_positive", UNSET)

        false_negative = d.pop("false_negative", UNSET)

        true_negative = d.pop("true_negative", UNSET)

        memory_evaluation_counts = cls(
            cases=cases,
            binary_supersession_cases=binary_supersession_cases,
            graded=graded,
            abstained=abstained,
            invalid_outputs=invalid_outputs,
            unavailable=unavailable,
            correct=correct,
            true_positive=true_positive,
            false_positive=false_positive,
            false_negative=false_negative,
            true_negative=true_negative,
        )

        return memory_evaluation_counts
