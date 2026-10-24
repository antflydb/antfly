from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_record import MemoryEntityRecord
    from ..models.memory_entity_view import MemoryEntityView


T = TypeVar("T", bound="MemoryEntityCandidate")


@_attrs_define
class MemoryEntityCandidate:
    """
    Attributes:
        entity (MemoryEntityRecord):
        view (MemoryEntityView):
        match_score (float):
        description_score (float):
        predicates (list[str]):
        name_similarity (float | None | Unset):
        view_similarity (float | None | Unset):
        semantic_discovered (bool | Unset):  Default: False.
    """

    entity: MemoryEntityRecord
    view: MemoryEntityView
    match_score: float
    description_score: float
    predicates: list[str]
    name_similarity: float | None | Unset = UNSET
    view_similarity: float | None | Unset = UNSET
    semantic_discovered: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        entity = self.entity.to_dict()

        view = self.view.to_dict()

        match_score = self.match_score

        description_score = self.description_score

        predicates = self.predicates

        name_similarity: float | None | Unset
        if isinstance(self.name_similarity, Unset):
            name_similarity = UNSET
        else:
            name_similarity = self.name_similarity

        view_similarity: float | None | Unset
        if isinstance(self.view_similarity, Unset):
            view_similarity = UNSET
        else:
            view_similarity = self.view_similarity

        semantic_discovered = self.semantic_discovered

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "entity": entity,
                "view": view,
                "match_score": match_score,
                "description_score": description_score,
                "predicates": predicates,
            }
        )
        if name_similarity is not UNSET:
            field_dict["name_similarity"] = name_similarity
        if view_similarity is not UNSET:
            field_dict["view_similarity"] = view_similarity
        if semantic_discovered is not UNSET:
            field_dict["semantic_discovered"] = semantic_discovered

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_record import MemoryEntityRecord
        from ..models.memory_entity_view import MemoryEntityView

        d = dict(src_dict)
        entity = MemoryEntityRecord.from_dict(d.pop("entity"))

        view = MemoryEntityView.from_dict(d.pop("view"))

        match_score = d.pop("match_score")

        description_score = d.pop("description_score")

        predicates = cast(list[str], d.pop("predicates"))

        def _parse_name_similarity(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        name_similarity = _parse_name_similarity(d.pop("name_similarity", UNSET))

        def _parse_view_similarity(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        view_similarity = _parse_view_similarity(d.pop("view_similarity", UNSET))

        semantic_discovered = d.pop("semantic_discovered", UNSET)

        memory_entity_candidate = cls(
            entity=entity,
            view=view,
            match_score=match_score,
            description_score=description_score,
            predicates=predicates,
            name_similarity=name_similarity,
            view_similarity=view_similarity,
            semantic_discovered=semantic_discovered,
        )

        return memory_entity_candidate
