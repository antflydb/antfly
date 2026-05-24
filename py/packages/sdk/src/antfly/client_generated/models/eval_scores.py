from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.eval_scores_generation import EvalScoresGeneration
    from ..models.eval_scores_retrieval import EvalScoresRetrieval


T = TypeVar("T", bound="EvalScores")


@_attrs_define
class EvalScores:
    """Scores organized by category

    Attributes:
        retrieval (EvalScoresRetrieval | Unset): Retrieval metric scores (recall, precision, ndcg, etc.)
        generation (EvalScoresGeneration | Unset): Generation quality scores (faithfulness, relevance, etc.)
    """

    retrieval: EvalScoresRetrieval | Unset = UNSET
    generation: EvalScoresGeneration | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        retrieval: dict[str, Any] | Unset = UNSET
        if not isinstance(self.retrieval, Unset):
            retrieval = self.retrieval.to_dict()

        generation: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generation, Unset):
            generation = self.generation.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if retrieval is not UNSET:
            field_dict["retrieval"] = retrieval
        if generation is not UNSET:
            field_dict["generation"] = generation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_scores_generation import EvalScoresGeneration
        from ..models.eval_scores_retrieval import EvalScoresRetrieval

        d = dict(src_dict)
        _retrieval = d.pop("retrieval", UNSET)
        retrieval: EvalScoresRetrieval | Unset
        if isinstance(_retrieval, Unset):
            retrieval = UNSET
        else:
            retrieval = EvalScoresRetrieval.from_dict(_retrieval)

        _generation = d.pop("generation", UNSET)
        generation: EvalScoresGeneration | Unset
        if isinstance(_generation, Unset):
            generation = UNSET
        else:
            generation = EvalScoresGeneration.from_dict(_generation)

        eval_scores = cls(
            retrieval=retrieval,
            generation=generation,
        )

        eval_scores.additional_properties = d
        return eval_scores

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
