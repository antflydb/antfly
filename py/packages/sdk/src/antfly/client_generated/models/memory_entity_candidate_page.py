from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_entity_candidate_page_basis import MemoryEntityCandidatePageBasis
from ..models.memory_entity_candidate_page_kind import MemoryEntityCandidatePageKind
from ..models.memory_entity_candidate_page_retrieval import MemoryEntityCandidatePageRetrieval
from ..models.memory_entity_candidate_page_semantic_discovery import MemoryEntityCandidatePageSemanticDiscovery
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_candidate import MemoryEntityCandidate
    from ..models.memory_entity_record import MemoryEntityRecord
    from ..models.memory_entity_view import MemoryEntityView


T = TypeVar("T", bound="MemoryEntityCandidatePage")


@_attrs_define
class MemoryEntityCandidatePage:
    """
    Attributes:
        seed (MemoryEntityRecord):
        seed_view (MemoryEntityView):
        kind (MemoryEntityCandidatePageKind):
        relationship_predicates (list[str]):
        items (list[MemoryEntityCandidate]):
        revision (int):
        config_generation (int):
        next_cursor (None | str):
        complete (bool):
        omitted (int):
        retrieval (MemoryEntityCandidatePageRetrieval):
        semantic_discovery (MemoryEntityCandidatePageSemanticDiscovery | Unset):  Default:
            MemoryEntityCandidatePageSemanticDiscovery.NOT_INDEXED.
        basis (MemoryEntityCandidatePageBasis | Unset):  Default:
            MemoryEntityCandidatePageBasis.NAME_MATCHES_DESCRIPTION_TIEBREAKER_NOT_IDENTITY_OR_CALIBRATED_CONFIDENCE.
    """

    seed: MemoryEntityRecord
    seed_view: MemoryEntityView
    kind: MemoryEntityCandidatePageKind
    relationship_predicates: list[str]
    items: list[MemoryEntityCandidate]
    revision: int
    config_generation: int
    next_cursor: None | str
    complete: bool
    omitted: int
    retrieval: MemoryEntityCandidatePageRetrieval
    semantic_discovery: MemoryEntityCandidatePageSemanticDiscovery | Unset = (
        MemoryEntityCandidatePageSemanticDiscovery.NOT_INDEXED
    )
    basis: MemoryEntityCandidatePageBasis | Unset = (
        MemoryEntityCandidatePageBasis.NAME_MATCHES_DESCRIPTION_TIEBREAKER_NOT_IDENTITY_OR_CALIBRATED_CONFIDENCE
    )

    def to_dict(self) -> dict[str, Any]:
        seed = self.seed.to_dict()

        seed_view = self.seed_view.to_dict()

        kind = self.kind.value

        relationship_predicates = self.relationship_predicates

        items = []
        for items_item_data in self.items:
            items_item = items_item_data.to_dict()
            items.append(items_item)

        revision = self.revision

        config_generation = self.config_generation

        next_cursor: None | str
        next_cursor = self.next_cursor

        complete = self.complete

        omitted = self.omitted

        retrieval = self.retrieval.value

        semantic_discovery: str | Unset = UNSET
        if not isinstance(self.semantic_discovery, Unset):
            semantic_discovery = self.semantic_discovery.value

        basis: str | Unset = UNSET
        if not isinstance(self.basis, Unset):
            basis = self.basis.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "seed": seed,
                "seed_view": seed_view,
                "kind": kind,
                "relationship_predicates": relationship_predicates,
                "items": items,
                "revision": revision,
                "config_generation": config_generation,
                "next_cursor": next_cursor,
                "complete": complete,
                "omitted": omitted,
                "retrieval": retrieval,
            }
        )
        if semantic_discovery is not UNSET:
            field_dict["semantic_discovery"] = semantic_discovery
        if basis is not UNSET:
            field_dict["basis"] = basis

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_candidate import MemoryEntityCandidate
        from ..models.memory_entity_record import MemoryEntityRecord
        from ..models.memory_entity_view import MemoryEntityView

        d = dict(src_dict)
        seed = MemoryEntityRecord.from_dict(d.pop("seed"))

        seed_view = MemoryEntityView.from_dict(d.pop("seed_view"))

        kind = MemoryEntityCandidatePageKind(d.pop("kind"))

        relationship_predicates = cast(list[str], d.pop("relationship_predicates"))

        items = []
        _items = d.pop("items")
        for items_item_data in _items:
            items_item = MemoryEntityCandidate.from_dict(items_item_data)

            items.append(items_item)

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        def _parse_next_cursor(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor"))

        complete = d.pop("complete")

        omitted = d.pop("omitted")

        retrieval = MemoryEntityCandidatePageRetrieval(d.pop("retrieval"))

        _semantic_discovery = d.pop("semantic_discovery", UNSET)
        semantic_discovery: MemoryEntityCandidatePageSemanticDiscovery | Unset
        if isinstance(_semantic_discovery, Unset):
            semantic_discovery = UNSET
        else:
            semantic_discovery = MemoryEntityCandidatePageSemanticDiscovery(_semantic_discovery)

        _basis = d.pop("basis", UNSET)
        basis: MemoryEntityCandidatePageBasis | Unset
        if isinstance(_basis, Unset):
            basis = UNSET
        else:
            basis = MemoryEntityCandidatePageBasis(_basis)

        memory_entity_candidate_page = cls(
            seed=seed,
            seed_view=seed_view,
            kind=kind,
            relationship_predicates=relationship_predicates,
            items=items,
            revision=revision,
            config_generation=config_generation,
            next_cursor=next_cursor,
            complete=complete,
            omitted=omitted,
            retrieval=retrieval,
            semantic_discovery=semantic_discovery,
            basis=basis,
        )

        return memory_entity_candidate_page
