from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="DecisionChoice")


@_attrs_define
class DecisionChoice:
    """Local decision models use string choice identifiers. Embedding category examples replace the description with their
    normalized centroid; trained models reject examples.

        Attributes:
            value (str):
            description (str | Unset):
            examples (list[str] | Unset):
    """

    value: str
    description: str | Unset = UNSET
    examples: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        description = self.description

        examples: list[str] | Unset = UNSET
        if not isinstance(self.examples, Unset):
            examples = self.examples

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "value": value,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if examples is not UNSET:
            field_dict["examples"] = examples

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        value = d.pop("value")

        description = d.pop("description", UNSET)

        examples = cast(list[str], d.pop("examples", UNSET))

        decision_choice = cls(
            value=value,
            description=description,
            examples=examples,
        )

        return decision_choice
