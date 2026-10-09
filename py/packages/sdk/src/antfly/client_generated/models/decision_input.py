from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="DecisionInput")


@_attrs_define
class DecisionInput:
    """
    Attributes:
        input_ (str):
        id (str | Unset):
    """

    input_: str
    id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        input_ = self.input_

        id = self.id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "input": input_,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        input_ = d.pop("input")

        id = d.pop("id", UNSET)

        decision_input = cls(
            input_=input_,
            id=id,
        )

        return decision_input
