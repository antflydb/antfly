from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointEntity")


@_attrs_define
class ExtractionJointEntity:
    """
    Attributes:
        description (str | Unset):
        threshold (float | Unset): Finite centered-logit decisions require a threshold strictly between zero and one.
        candidate_threshold (float | Unset):
        max_candidates (int | Unset):
        allow_nested (bool | Unset):
    """

    description: str | Unset = UNSET
    threshold: float | Unset = UNSET
    candidate_threshold: float | Unset = UNSET
    max_candidates: int | Unset = UNSET
    allow_nested: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        description = self.description

        threshold = self.threshold

        candidate_threshold = self.candidate_threshold

        max_candidates = self.max_candidates

        allow_nested = self.allow_nested

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if description is not UNSET:
            field_dict["description"] = description
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if candidate_threshold is not UNSET:
            field_dict["candidate_threshold"] = candidate_threshold
        if max_candidates is not UNSET:
            field_dict["max_candidates"] = max_candidates
        if allow_nested is not UNSET:
            field_dict["allow_nested"] = allow_nested

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        description = d.pop("description", UNSET)

        threshold = d.pop("threshold", UNSET)

        candidate_threshold = d.pop("candidate_threshold", UNSET)

        max_candidates = d.pop("max_candidates", UNSET)

        allow_nested = d.pop("allow_nested", UNSET)

        extraction_joint_entity = cls(
            description=description,
            threshold=threshold,
            candidate_threshold=candidate_threshold,
            max_candidates=max_candidates,
            allow_nested=allow_nested,
        )

        return extraction_joint_entity
