from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

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
    """

    entity: MemoryEntityRecord
    view: MemoryEntityView
    match_score: float
    description_score: float
    predicates: list[str]

    def to_dict(self) -> dict[str, Any]:
        entity = self.entity.to_dict()

        view = self.view.to_dict()

        match_score = self.match_score

        description_score = self.description_score

        predicates = self.predicates

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

        memory_entity_candidate = cls(
            entity=entity,
            view=view,
            match_score=match_score,
            description_score=description_score,
            predicates=predicates,
        )

        return memory_entity_candidate
