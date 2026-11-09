from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_brief_item_kind import MemoryBriefItemKind
from ..models.memory_brief_item_stability import MemoryBriefItemStability

if TYPE_CHECKING:
    from ..models.memory_utility import MemoryUtility


T = TypeVar("T", bound="MemoryBriefItem")


@_attrs_define
class MemoryBriefItem:
    """
    Attributes:
        id (str):
        statement (str):
        kind (MemoryBriefItemKind):
        subject (str):
        predicate (str):
        scope (str):
        stability (MemoryBriefItemStability):
        valid_from (int | None):
        pinned (bool):
        score (float):
        utility (MemoryUtility):
    """

    id: str
    statement: str
    kind: MemoryBriefItemKind
    subject: str
    predicate: str
    scope: str
    stability: MemoryBriefItemStability
    valid_from: int | None
    pinned: bool
    score: float
    utility: MemoryUtility

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        statement = self.statement

        kind = self.kind.value

        subject = self.subject

        predicate = self.predicate

        scope = self.scope

        stability = self.stability.value

        valid_from: int | None
        valid_from = self.valid_from

        pinned = self.pinned

        score = self.score

        utility = self.utility.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "statement": statement,
                "kind": kind,
                "subject": subject,
                "predicate": predicate,
                "scope": scope,
                "stability": stability,
                "valid_from": valid_from,
                "pinned": pinned,
                "score": score,
                "utility": utility,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_utility import MemoryUtility

        d = dict(src_dict)
        id = d.pop("id")

        statement = d.pop("statement")

        kind = MemoryBriefItemKind(d.pop("kind"))

        subject = d.pop("subject")

        predicate = d.pop("predicate")

        scope = d.pop("scope")

        stability = MemoryBriefItemStability(d.pop("stability"))

        def _parse_valid_from(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        valid_from = _parse_valid_from(d.pop("valid_from"))

        pinned = d.pop("pinned")

        score = d.pop("score")

        utility = MemoryUtility.from_dict(d.pop("utility"))

        memory_brief_item = cls(
            id=id,
            statement=statement,
            kind=kind,
            subject=subject,
            predicate=predicate,
            scope=scope,
            stability=stability,
            valid_from=valid_from,
            pinned=pinned,
            score=score,
            utility=utility,
        )

        return memory_brief_item
