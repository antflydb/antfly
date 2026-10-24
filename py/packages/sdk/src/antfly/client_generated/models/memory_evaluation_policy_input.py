from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_candidate_request import MemoryEntityCandidateRequest
    from ..models.memory_recall_request import MemoryRecallRequest
    from ..models.memory_supersession_candidate_request import MemorySupersessionCandidateRequest


T = TypeVar("T", bound="MemoryEvaluationPolicyInput")


@_attrs_define
class MemoryEvaluationPolicyInput:
    """
    Attributes:
        seed (str | Unset):
        target (str | Unset):
        entity_candidates (MemoryEntityCandidateRequest | Unset):
        supersession_candidates (MemorySupersessionCandidateRequest | Unset):
        recall (MemoryRecallRequest | Unset):
    """

    seed: str | Unset = UNSET
    target: str | Unset = UNSET
    entity_candidates: MemoryEntityCandidateRequest | Unset = UNSET
    supersession_candidates: MemorySupersessionCandidateRequest | Unset = UNSET
    recall: MemoryRecallRequest | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        seed = self.seed

        target = self.target

        entity_candidates: dict[str, Any] | Unset = UNSET
        if not isinstance(self.entity_candidates, Unset):
            entity_candidates = self.entity_candidates.to_dict()

        supersession_candidates: dict[str, Any] | Unset = UNSET
        if not isinstance(self.supersession_candidates, Unset):
            supersession_candidates = self.supersession_candidates.to_dict()

        recall: dict[str, Any] | Unset = UNSET
        if not isinstance(self.recall, Unset):
            recall = self.recall.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if seed is not UNSET:
            field_dict["seed"] = seed
        if target is not UNSET:
            field_dict["target"] = target
        if entity_candidates is not UNSET:
            field_dict["entity_candidates"] = entity_candidates
        if supersession_candidates is not UNSET:
            field_dict["supersession_candidates"] = supersession_candidates
        if recall is not UNSET:
            field_dict["recall"] = recall

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_candidate_request import MemoryEntityCandidateRequest
        from ..models.memory_recall_request import MemoryRecallRequest
        from ..models.memory_supersession_candidate_request import MemorySupersessionCandidateRequest

        d = dict(src_dict)
        seed = d.pop("seed", UNSET)

        target = d.pop("target", UNSET)

        _entity_candidates = d.pop("entity_candidates", UNSET)
        entity_candidates: MemoryEntityCandidateRequest | Unset
        if isinstance(_entity_candidates, Unset):
            entity_candidates = UNSET
        else:
            entity_candidates = MemoryEntityCandidateRequest.from_dict(_entity_candidates)

        _supersession_candidates = d.pop("supersession_candidates", UNSET)
        supersession_candidates: MemorySupersessionCandidateRequest | Unset
        if isinstance(_supersession_candidates, Unset):
            supersession_candidates = UNSET
        else:
            supersession_candidates = MemorySupersessionCandidateRequest.from_dict(_supersession_candidates)

        _recall = d.pop("recall", UNSET)
        recall: MemoryRecallRequest | Unset
        if isinstance(_recall, Unset):
            recall = UNSET
        else:
            recall = MemoryRecallRequest.from_dict(_recall)

        memory_evaluation_policy_input = cls(
            seed=seed,
            target=target,
            entity_candidates=entity_candidates,
            supersession_candidates=supersession_candidates,
            recall=recall,
        )

        return memory_evaluation_policy_input
