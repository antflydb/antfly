from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evaluation_run_mode import MemoryEvaluationRunMode
from ..models.memory_evaluation_run_phase import MemoryEvaluationRunPhase
from ..models.memory_evaluation_run_visibility import MemoryEvaluationRunVisibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_counts import MemoryEvaluationCounts
    from ..models.memory_evaluation_run_judge_type_0 import MemoryEvaluationRunJudgeType0
    from ..models.memory_evaluation_run_thresholds_type_0 import MemoryEvaluationRunThresholdsType0


T = TypeVar("T", bound="MemoryEvaluationRun")


@_attrs_define
class MemoryEvaluationRun:
    """
    Attributes:
        id (str):
        dataset (str):
        dataset_revision (int):
        root (None | str):
        case_count (int):
        owner (str):
        branch (str):
        visibility (MemoryEvaluationRunVisibility):
        mode (MemoryEvaluationRunMode):
        knowledge_revision (int):
        config_generation (int):
        config_fingerprint (str):
        source_config_fingerprint (str):
        judge (MemoryEvaluationRunJudgeType0 | None):
        fingerprint (str):
        revision (int):
        recorded_at (int):
        version (int | Unset):  Default: 1.
        thresholds (MemoryEvaluationRunThresholdsType0 | None | Unset):
        prompt_revision (str | Unset):
        phase (MemoryEvaluationRunPhase | Unset):  Default: MemoryEvaluationRunPhase.PENDING.
        error_code (None | str | Unset):
        completed (int | Unset):  Default: 0.
        cursor (None | str | Unset):
        counts (list[MemoryEvaluationCounts] | Unset):
        admission (list[MemoryEvaluationCounts] | Unset):
    """

    id: str
    dataset: str
    dataset_revision: int
    root: None | str
    case_count: int
    owner: str
    branch: str
    visibility: MemoryEvaluationRunVisibility
    mode: MemoryEvaluationRunMode
    knowledge_revision: int
    config_generation: int
    config_fingerprint: str
    source_config_fingerprint: str
    judge: MemoryEvaluationRunJudgeType0 | None
    fingerprint: str
    revision: int
    recorded_at: int
    version: int | Unset = 1
    thresholds: MemoryEvaluationRunThresholdsType0 | None | Unset = UNSET
    prompt_revision: str | Unset = UNSET
    phase: MemoryEvaluationRunPhase | Unset = MemoryEvaluationRunPhase.PENDING
    error_code: None | str | Unset = UNSET
    completed: int | Unset = 0
    cursor: None | str | Unset = UNSET
    counts: list[MemoryEvaluationCounts] | Unset = UNSET
    admission: list[MemoryEvaluationCounts] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_evaluation_run_judge_type_0 import MemoryEvaluationRunJudgeType0
        from ..models.memory_evaluation_run_thresholds_type_0 import MemoryEvaluationRunThresholdsType0

        id = self.id

        dataset = self.dataset

        dataset_revision = self.dataset_revision

        root: None | str
        root = self.root

        case_count = self.case_count

        owner = self.owner

        branch = self.branch

        visibility = self.visibility.value

        mode = self.mode.value

        knowledge_revision = self.knowledge_revision

        config_generation = self.config_generation

        config_fingerprint = self.config_fingerprint

        source_config_fingerprint = self.source_config_fingerprint

        judge: dict[str, Any] | None
        if isinstance(self.judge, MemoryEvaluationRunJudgeType0):
            judge = self.judge.to_dict()
        else:
            judge = self.judge

        fingerprint = self.fingerprint

        revision = self.revision

        recorded_at = self.recorded_at

        version = self.version

        thresholds: dict[str, Any] | None | Unset
        if isinstance(self.thresholds, Unset):
            thresholds = UNSET
        elif isinstance(self.thresholds, MemoryEvaluationRunThresholdsType0):
            thresholds = self.thresholds.to_dict()
        else:
            thresholds = self.thresholds

        prompt_revision = self.prompt_revision

        phase: str | Unset = UNSET
        if not isinstance(self.phase, Unset):
            phase = self.phase.value

        error_code: None | str | Unset
        if isinstance(self.error_code, Unset):
            error_code = UNSET
        else:
            error_code = self.error_code

        completed = self.completed

        cursor: None | str | Unset
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

        counts: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.counts, Unset):
            counts = []
            for counts_item_data in self.counts:
                counts_item = counts_item_data.to_dict()
                counts.append(counts_item)

        admission: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.admission, Unset):
            admission = []
            for admission_item_data in self.admission:
                admission_item = admission_item_data.to_dict()
                admission.append(admission_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "dataset": dataset,
                "dataset_revision": dataset_revision,
                "root": root,
                "case_count": case_count,
                "owner": owner,
                "branch": branch,
                "visibility": visibility,
                "mode": mode,
                "knowledge_revision": knowledge_revision,
                "config_generation": config_generation,
                "config_fingerprint": config_fingerprint,
                "source_config_fingerprint": source_config_fingerprint,
                "judge": judge,
                "fingerprint": fingerprint,
                "revision": revision,
                "recorded_at": recorded_at,
            }
        )
        if version is not UNSET:
            field_dict["version"] = version
        if thresholds is not UNSET:
            field_dict["thresholds"] = thresholds
        if prompt_revision is not UNSET:
            field_dict["prompt_revision"] = prompt_revision
        if phase is not UNSET:
            field_dict["phase"] = phase
        if error_code is not UNSET:
            field_dict["error_code"] = error_code
        if completed is not UNSET:
            field_dict["completed"] = completed
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if counts is not UNSET:
            field_dict["counts"] = counts
        if admission is not UNSET:
            field_dict["admission"] = admission

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_counts import MemoryEvaluationCounts
        from ..models.memory_evaluation_run_judge_type_0 import MemoryEvaluationRunJudgeType0
        from ..models.memory_evaluation_run_thresholds_type_0 import MemoryEvaluationRunThresholdsType0

        d = dict(src_dict)
        id = d.pop("id")

        dataset = d.pop("dataset")

        dataset_revision = d.pop("dataset_revision")

        def _parse_root(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        root = _parse_root(d.pop("root"))

        case_count = d.pop("case_count")

        owner = d.pop("owner")

        branch = d.pop("branch")

        visibility = MemoryEvaluationRunVisibility(d.pop("visibility"))

        mode = MemoryEvaluationRunMode(d.pop("mode"))

        knowledge_revision = d.pop("knowledge_revision")

        config_generation = d.pop("config_generation")

        config_fingerprint = d.pop("config_fingerprint")

        source_config_fingerprint = d.pop("source_config_fingerprint")

        def _parse_judge(data: object) -> MemoryEvaluationRunJudgeType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                judge_type_0 = MemoryEvaluationRunJudgeType0.from_dict(data)

                return judge_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationRunJudgeType0 | None, data)

        judge = _parse_judge(d.pop("judge"))

        fingerprint = d.pop("fingerprint")

        revision = d.pop("revision")

        recorded_at = d.pop("recorded_at")

        version = d.pop("version", UNSET)

        def _parse_thresholds(data: object) -> MemoryEvaluationRunThresholdsType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                thresholds_type_0 = MemoryEvaluationRunThresholdsType0.from_dict(data)

                return thresholds_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationRunThresholdsType0 | None | Unset, data)

        thresholds = _parse_thresholds(d.pop("thresholds", UNSET))

        prompt_revision = d.pop("prompt_revision", UNSET)

        _phase = d.pop("phase", UNSET)
        phase: MemoryEvaluationRunPhase | Unset
        if isinstance(_phase, Unset):
            phase = UNSET
        else:
            phase = MemoryEvaluationRunPhase(_phase)

        def _parse_error_code(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        error_code = _parse_error_code(d.pop("error_code", UNSET))

        completed = d.pop("completed", UNSET)

        def _parse_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cursor = _parse_cursor(d.pop("cursor", UNSET))

        _counts = d.pop("counts", UNSET)
        counts: list[MemoryEvaluationCounts] | Unset = UNSET
        if _counts is not UNSET:
            counts = []
            for counts_item_data in _counts:
                counts_item = MemoryEvaluationCounts.from_dict(counts_item_data)

                counts.append(counts_item)

        _admission = d.pop("admission", UNSET)
        admission: list[MemoryEvaluationCounts] | Unset = UNSET
        if _admission is not UNSET:
            admission = []
            for admission_item_data in _admission:
                admission_item = MemoryEvaluationCounts.from_dict(admission_item_data)

                admission.append(admission_item)

        memory_evaluation_run = cls(
            id=id,
            dataset=dataset,
            dataset_revision=dataset_revision,
            root=root,
            case_count=case_count,
            owner=owner,
            branch=branch,
            visibility=visibility,
            mode=mode,
            knowledge_revision=knowledge_revision,
            config_generation=config_generation,
            config_fingerprint=config_fingerprint,
            source_config_fingerprint=source_config_fingerprint,
            judge=judge,
            fingerprint=fingerprint,
            revision=revision,
            recorded_at=recorded_at,
            version=version,
            thresholds=thresholds,
            prompt_revision=prompt_revision,
            phase=phase,
            error_code=error_code,
            completed=completed,
            cursor=cursor,
            counts=counts,
            admission=admission,
        )

        return memory_evaluation_run
