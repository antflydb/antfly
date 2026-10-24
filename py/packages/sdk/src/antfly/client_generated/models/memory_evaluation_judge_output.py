from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evaluation_judge_output_label import MemoryEvaluationJudgeOutputLabel
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEvaluationJudgeOutput")


@_attrs_define
class MemoryEvaluationJudgeOutput:
    """
    Attributes:
        label (MemoryEvaluationJudgeOutputLabel | Unset):
        ranking (list[str] | None | Unset):
        reason (str | Unset):
    """

    label: MemoryEvaluationJudgeOutputLabel | Unset = UNSET
    ranking: list[str] | None | Unset = UNSET
    reason: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        label: str | Unset = UNSET
        if not isinstance(self.label, Unset):
            label = self.label.value

        ranking: list[str] | None | Unset
        if isinstance(self.ranking, Unset):
            ranking = UNSET
        elif isinstance(self.ranking, list):
            ranking = self.ranking

        else:
            ranking = self.ranking

        reason = self.reason

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if label is not UNSET:
            field_dict["label"] = label
        if ranking is not UNSET:
            field_dict["ranking"] = ranking
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _label = d.pop("label", UNSET)
        label: MemoryEvaluationJudgeOutputLabel | Unset
        if isinstance(_label, Unset):
            label = UNSET
        else:
            label = MemoryEvaluationJudgeOutputLabel(_label)

        def _parse_ranking(data: object) -> list[str] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                ranking_type_0 = cast(list[str], data)

                return ranking_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | None | Unset, data)

        ranking = _parse_ranking(d.pop("ranking", UNSET))

        reason = d.pop("reason", UNSET)

        memory_evaluation_judge_output = cls(
            label=label,
            ranking=ranking,
            reason=reason,
        )

        return memory_evaluation_judge_output
