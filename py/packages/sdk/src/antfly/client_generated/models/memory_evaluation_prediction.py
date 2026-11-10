from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evaluation_prediction_label import MemoryEvaluationPredictionLabel
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEvaluationPrediction")


@_attrs_define
class MemoryEvaluationPrediction:
    """
    Attributes:
        admitted (bool | None | Unset):
        label (MemoryEvaluationPredictionLabel | Unset):
        ranking (list[str] | None | Unset):
        reason (str | Unset):
        token_upper_bound (int | None | Unset):
    """

    admitted: bool | None | Unset = UNSET
    label: MemoryEvaluationPredictionLabel | Unset = UNSET
    ranking: list[str] | None | Unset = UNSET
    reason: str | Unset = UNSET
    token_upper_bound: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        admitted: bool | None | Unset
        if isinstance(self.admitted, Unset):
            admitted = UNSET
        else:
            admitted = self.admitted

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

        token_upper_bound: int | None | Unset
        if isinstance(self.token_upper_bound, Unset):
            token_upper_bound = UNSET
        else:
            token_upper_bound = self.token_upper_bound

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if admitted is not UNSET:
            field_dict["admitted"] = admitted
        if label is not UNSET:
            field_dict["label"] = label
        if ranking is not UNSET:
            field_dict["ranking"] = ranking
        if reason is not UNSET:
            field_dict["reason"] = reason
        if token_upper_bound is not UNSET:
            field_dict["token_upper_bound"] = token_upper_bound

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_admitted(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        admitted = _parse_admitted(d.pop("admitted", UNSET))

        _label = d.pop("label", UNSET)
        label: MemoryEvaluationPredictionLabel | Unset
        if isinstance(_label, Unset):
            label = UNSET
        else:
            label = MemoryEvaluationPredictionLabel(_label)

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

        def _parse_token_upper_bound(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        token_upper_bound = _parse_token_upper_bound(d.pop("token_upper_bound", UNSET))

        memory_evaluation_prediction = cls(
            admitted=admitted,
            label=label,
            ranking=ranking,
            reason=reason,
            token_upper_bound=token_upper_bound,
        )

        return memory_evaluation_prediction
