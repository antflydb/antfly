from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceEmbeddingDecisionCategory")


@_attrs_define
class InferenceEmbeddingDecisionCategory:
    """Embedding similarity categories only. The normalized centroid of examples replaces the description prototype;
    examples use the input renderer. Trained decision models reject this form.

        Attributes:
            examples (list[str]):
            description (str | Unset):
    """

    examples: list[str]
    description: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        examples = self.examples

        description = self.description

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "examples": examples,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        examples = cast(list[str], d.pop("examples"))

        description = d.pop("description", UNSET)

        inference_embedding_decision_category = cls(
            examples=examples,
            description=description,
        )

        return inference_embedding_decision_category
