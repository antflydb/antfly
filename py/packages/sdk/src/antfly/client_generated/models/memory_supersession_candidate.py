from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_claim_view import MemoryClaimView


T = TypeVar("T", bound="MemorySupersessionCandidate")


@_attrs_define
class MemorySupersessionCandidate:
    """
    Attributes:
        view (MemoryClaimView):
        lexical_score (float):
        semantic_similarity (float | None):
    """

    view: MemoryClaimView
    lexical_score: float
    semantic_similarity: float | None

    def to_dict(self) -> dict[str, Any]:
        view = self.view.to_dict()

        lexical_score = self.lexical_score

        semantic_similarity: float | None
        semantic_similarity = self.semantic_similarity

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "view": view,
                "lexical_score": lexical_score,
                "semantic_similarity": semantic_similarity,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_claim_view import MemoryClaimView

        d = dict(src_dict)
        view = MemoryClaimView.from_dict(d.pop("view"))

        lexical_score = d.pop("lexical_score")

        def _parse_semantic_similarity(data: object) -> float | None:
            if data is None:
                return data
            return cast(float | None, data)

        semantic_similarity = _parse_semantic_similarity(d.pop("semantic_similarity"))

        memory_supersession_candidate = cls(
            view=view,
            lexical_score=lexical_score,
            semantic_similarity=semantic_similarity,
        )

        return memory_supersession_candidate
