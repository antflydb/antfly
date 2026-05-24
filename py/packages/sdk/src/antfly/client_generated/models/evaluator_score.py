from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.evaluator_score_metadata import EvaluatorScoreMetadata


T = TypeVar("T", bound="EvaluatorScore")


@_attrs_define
class EvaluatorScore:
    """Result from a single evaluator

    Attributes:
        score (float | Unset): Numeric score (0-1)
        pass_ (bool | Unset): Whether the evaluation passed the threshold
        reason (str | Unset): Human-readable explanation of the result
        metadata (EvaluatorScoreMetadata | Unset): Additional evaluator-specific data
    """

    score: float | Unset = UNSET
    pass_: bool | Unset = UNSET
    reason: str | Unset = UNSET
    metadata: EvaluatorScoreMetadata | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        score = self.score

        pass_ = self.pass_

        reason = self.reason

        metadata: dict[str, Any] | Unset = UNSET
        if not isinstance(self.metadata, Unset):
            metadata = self.metadata.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if score is not UNSET:
            field_dict["score"] = score
        if pass_ is not UNSET:
            field_dict["pass"] = pass_
        if reason is not UNSET:
            field_dict["reason"] = reason
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.evaluator_score_metadata import EvaluatorScoreMetadata

        d = dict(src_dict)
        score = d.pop("score", UNSET)

        pass_ = d.pop("pass", UNSET)

        reason = d.pop("reason", UNSET)

        _metadata = d.pop("metadata", UNSET)
        metadata: EvaluatorScoreMetadata | Unset
        if isinstance(_metadata, Unset):
            metadata = UNSET
        else:
            metadata = EvaluatorScoreMetadata.from_dict(_metadata)

        evaluator_score = cls(
            score=score,
            pass_=pass_,
            reason=reason,
            metadata=metadata,
        )

        evaluator_score.additional_properties = d
        return evaluator_score

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
