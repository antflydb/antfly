from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

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
    """

    counts: MemoryEvaluationCounts
    precision: float | None
    recall: float | None
    f1: float | None
    coverage: float | None
    accuracy: float | None

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

        memory_evaluation_report = cls(
            counts=counts,
            precision=precision,
            recall=recall,
            f1=f1,
            coverage=coverage,
            accuracy=accuracy,
        )

        return memory_evaluation_report
