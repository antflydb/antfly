from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_supersession_candidate_page_retrieval import MemorySupersessionCandidatePageRetrieval
from ..models.memory_supersession_candidate_page_semantic_discovery import (
    MemorySupersessionCandidatePageSemanticDiscovery,
)

if TYPE_CHECKING:
    from ..models.memory_claim_view import MemoryClaimView
    from ..models.memory_supersession_candidate import MemorySupersessionCandidate


T = TypeVar("T", bound="MemorySupersessionCandidatePage")


@_attrs_define
class MemorySupersessionCandidatePage:
    """
    Attributes:
        seed (MemoryClaimView):
        items (list[MemorySupersessionCandidate]):
        retrieval (MemorySupersessionCandidatePageRetrieval):
        semantic_discovery (MemorySupersessionCandidatePageSemanticDiscovery):
        truncated (bool):
        revision (int):
    """

    seed: MemoryClaimView
    items: list[MemorySupersessionCandidate]
    retrieval: MemorySupersessionCandidatePageRetrieval
    semantic_discovery: MemorySupersessionCandidatePageSemanticDiscovery
    truncated: bool
    revision: int

    def to_dict(self) -> dict[str, Any]:
        seed = self.seed.to_dict()

        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        retrieval = self.retrieval.value

        semantic_discovery = self.semantic_discovery.value

        truncated = self.truncated

        revision = self.revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "seed": seed,
                "items": items,
                "retrieval": retrieval,
                "semantic_discovery": semantic_discovery,
                "truncated": truncated,
                "revision": revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_claim_view import MemoryClaimView
        from ..models.memory_supersession_candidate import MemorySupersessionCandidate

        d = dict(src_dict)
        seed = MemoryClaimView.from_dict(d.pop("seed"))

        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemorySupersessionCandidate.from_dict(items_item_data)

            items.append(items_item)

        retrieval = MemorySupersessionCandidatePageRetrieval(d.pop("retrieval"))

        semantic_discovery = MemorySupersessionCandidatePageSemanticDiscovery(d.pop("semantic_discovery"))

        truncated = d.pop("truncated")

        revision = d.pop("revision")

        memory_supersession_candidate_page = cls(
            seed=seed,
            items=items,
            retrieval=retrieval,
            semantic_discovery=semantic_discovery,
            truncated=truncated,
            revision=revision,
        )

        return memory_supersession_candidate_page
