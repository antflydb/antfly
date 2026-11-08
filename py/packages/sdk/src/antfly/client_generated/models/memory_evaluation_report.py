from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_counts import MemoryEvaluationCounts


T = TypeVar("T", bound="MemoryEvaluationReport")


@_attrs_define
class MemoryEvaluationReport:
    """
    Attributes:
        counts (MemoryEvaluationCounts):
        precision (float | None):
        recall (float | None):
        f1 (float | None):
        coverage (float | None):
        accuracy (float | None):
        mean_tokens (float | None | Unset):
    """

    counts: MemoryEvaluationCounts
    precision: float | None
    recall: float | None
    f1: float | None
    coverage: float | None
    accuracy: float | None
    mean_tokens: float | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        counts = self.counts.to_dict()

        precision: float | None
        precision = self.precision

        recall: float | None
        recall = self.recall

        f1: float | None
        f1 = self.f1

        coverage: float | None
        coverage = self.coverage

        accuracy: float | None
        accuracy = self.accuracy

        mean_tokens: float | None | Unset
        if isinstance(self.mean_tokens, Unset):
            mean_tokens = UNSET
        else:
            mean_tokens = self.mean_tokens

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "counts": counts,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "coverage": coverage,
                "accuracy": accuracy,
            }
        )
        if mean_tokens is not UNSET:
            field_dict["mean_tokens"] = mean_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_counts import MemoryEvaluationCounts

        d = dict(src_dict)
        counts = MemoryEvaluationCounts.from_dict(d.pop("counts"))

        def _parse_precision(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        precision = _parse_precision(d.pop("precision"))

        def _parse_recall(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        recall = _parse_recall(d.pop("recall"))

        def _parse_f1(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        f1 = _parse_f1(d.pop("f1"))

        def _parse_coverage(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        coverage = _parse_coverage(d.pop("coverage"))

        def _parse_accuracy(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        accuracy = _parse_accuracy(d.pop("accuracy"))

        def _parse_mean_tokens(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        mean_tokens = _parse_mean_tokens(d.pop("mean_tokens", UNSET))

        memory_evaluation_report = cls(
            counts=counts,
            precision=precision,
            recall=recall,
            f1=f1,
            coverage=coverage,
            accuracy=accuracy,
            mean_tokens=mean_tokens,
        )

        return memory_evaluation_report
