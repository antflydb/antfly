from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EvalSummary")


@_attrs_define
class EvalSummary:
    """Aggregate statistics across all evaluators

    Attributes:
        average_score (Union[Unset, float]): Average score across all evaluators
        passed (Union[Unset, int]): Number of evaluators that passed
        failed (Union[Unset, int]): Number of evaluators that failed
        total (Union[Unset, int]): Total number of evaluators run
    """

    average_score: Union[Unset, float] = UNSET
    passed: Union[Unset, int] = UNSET
    failed: Union[Unset, int] = UNSET
    total: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        average_score = self.average_score

        passed = self.passed

        failed = self.failed

        total = self.total

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if average_score is not UNSET:
            field_dict["average_score"] = average_score
        if passed is not UNSET:
            field_dict["passed"] = passed
        if failed is not UNSET:
            field_dict["failed"] = failed
        if total is not UNSET:
            field_dict["total"] = total

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        average_score = d.pop("average_score", UNSET)

        passed = d.pop("passed", UNSET)

        failed = d.pop("failed", UNSET)

        total = d.pop("total", UNSET)

        eval_summary = cls(
            average_score=average_score,
            passed=passed,
            failed=failed,
            total=total,
        )

        eval_summary.additional_properties = d
        return eval_summary

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
