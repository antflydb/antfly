from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="DecisionLevel")


@_attrs_define
class DecisionLevel:
    """Ordered score level, from lowest to highest. Results assign zero-based numeric values to these levels.

    Attributes:
        label (str):
        description (str | Unset):
    """

    label: str
    description: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        label = self.label

        description = self.description

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "label": label,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        label = d.pop("label")

        description = d.pop("description", UNSET)

        decision_level = cls(
            label=label,
            description=description,
        )

        return decision_level
