from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evaluation_start_mode import MemoryEvaluationStartMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_start_judge_type_0 import MemoryEvaluationStartJudgeType0
    from ..models.memory_evaluation_start_thresholds_type_0 import MemoryEvaluationStartThresholdsType0


T = TypeVar("T", bound="MemoryEvaluationStart")


@_attrs_define
class MemoryEvaluationStart:
    """
    Attributes:
        id (str):
        dataset (str):
        expected_revision (int):
        dataset_revision (int | None | Unset):
        mode (MemoryEvaluationStartMode | Unset):  Default: MemoryEvaluationStartMode.EXTERNAL.
        judge (MemoryEvaluationStartJudgeType0 | None | Unset):
        thresholds (MemoryEvaluationStartThresholdsType0 | None | Unset):
    """

    id: str
    dataset: str
    expected_revision: int
    dataset_revision: int | None | Unset = UNSET
    mode: MemoryEvaluationStartMode | Unset = MemoryEvaluationStartMode.EXTERNAL
    judge: MemoryEvaluationStartJudgeType0 | None | Unset = UNSET
    thresholds: MemoryEvaluationStartThresholdsType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_evaluation_start_judge_type_0 import MemoryEvaluationStartJudgeType0
        from ..models.memory_evaluation_start_thresholds_type_0 import MemoryEvaluationStartThresholdsType0

        id = self.id

        dataset = self.dataset

        expected_revision = self.expected_revision

        dataset_revision: int | None | Unset
        if isinstance(self.dataset_revision, Unset):
            dataset_revision = UNSET
        else:
            dataset_revision = self.dataset_revision

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        judge: dict[str, Any] | None | Unset
        if isinstance(self.judge, Unset):
            judge = UNSET
        elif isinstance(self.judge, MemoryEvaluationStartJudgeType0):
            judge = self.judge.to_dict()
        else:
            judge = self.judge

        thresholds: dict[str, Any] | None | Unset
        if isinstance(self.thresholds, Unset):
            thresholds = UNSET
        elif isinstance(self.thresholds, MemoryEvaluationStartThresholdsType0):
            thresholds = self.thresholds.to_dict()
        else:
            thresholds = self.thresholds

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "dataset": dataset,
                "expected_revision": expected_revision,
            }
        )
        if dataset_revision is not UNSET:
            field_dict["dataset_revision"] = dataset_revision
        if mode is not UNSET:
            field_dict["mode"] = mode
        if judge is not UNSET:
            field_dict["judge"] = judge
        if thresholds is not UNSET:
            field_dict["thresholds"] = thresholds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_start_judge_type_0 import MemoryEvaluationStartJudgeType0
        from ..models.memory_evaluation_start_thresholds_type_0 import MemoryEvaluationStartThresholdsType0

        d = dict(src_dict)
        id = d.pop("id")

        dataset = d.pop("dataset")

        expected_revision = d.pop("expected_revision")

        def _parse_dataset_revision(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        dataset_revision = _parse_dataset_revision(d.pop("dataset_revision", UNSET))

        _mode = d.pop("mode", UNSET)
        mode: MemoryEvaluationStartMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = MemoryEvaluationStartMode(_mode)

        def _parse_judge(data: object) -> MemoryEvaluationStartJudgeType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                judge_type_0 = MemoryEvaluationStartJudgeType0.from_dict(data)

                return judge_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationStartJudgeType0 | None | Unset, data)

        judge = _parse_judge(d.pop("judge", UNSET))

        def _parse_thresholds(data: object) -> MemoryEvaluationStartThresholdsType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                thresholds_type_0 = MemoryEvaluationStartThresholdsType0.from_dict(data)

                return thresholds_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationStartThresholdsType0 | None | Unset, data)

        thresholds = _parse_thresholds(d.pop("thresholds", UNSET))

        memory_evaluation_start = cls(
            id=id,
            dataset=dataset,
            expected_revision=expected_revision,
            dataset_revision=dataset_revision,
            mode=mode,
            judge=judge,
            thresholds=thresholds,
        )

        return memory_evaluation_start
