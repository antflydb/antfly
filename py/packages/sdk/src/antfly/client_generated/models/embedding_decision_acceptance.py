from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="EmbeddingDecisionAcceptance")


@_attrs_define
class EmbeddingDecisionAcceptance:
    """Question-specific acceptance policy for embedding similarity. Trained decision models reject it.

    Attributes:
        calibration_id (str | Unset): Public API identifier for a server-side fitted threshold artifact at
            model/calibrations/{id}.json. The server verifies qualification and the exact asset identity, prompt profile,
            dimensions, renderer and this question's label/prototype set. Mutually exclusive with manual thresholds; never
            produces probabilities.
        min_similarity (float | Unset): For a single choice, abstain when the highest cosine similarity is below this
            value. It is a similarity threshold, not an accuracy or probability estimate.
        min_margin (float | Unset): For a single choice, require this gap between the highest and second-highest cosine
            similarities. For multi_choice, require every score to be at least this distance from its own selection
            threshold; otherwise abstain.
    """

    calibration_id: str | Unset = UNSET
    min_similarity: float | Unset = UNSET
    min_margin: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        calibration_id = self.calibration_id

        min_similarity = self.min_similarity

        min_margin = self.min_margin

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if calibration_id is not UNSET:
            field_dict["calibration_id"] = calibration_id
        if min_similarity is not UNSET:
            field_dict["min_similarity"] = min_similarity
        if min_margin is not UNSET:
            field_dict["min_margin"] = min_margin

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        calibration_id = d.pop("calibration_id", UNSET)

        min_similarity = d.pop("min_similarity", UNSET)

        min_margin = d.pop("min_margin", UNSET)

        embedding_decision_acceptance = cls(
            calibration_id=calibration_id,
            min_similarity=min_similarity,
            min_margin=min_margin,
        )

        return embedding_decision_acceptance
