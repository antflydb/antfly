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
        context_index (int | Unset): Index into request contexts. Requires context_version 1. Effective input is
            context, newline, then input; the 1 MiB input limit applies after expansion.
    """

    input_: str
    id: str | Unset = UNSET
    context_index: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        input_ = self.input_

        id = self.id

        context_index = self.context_index

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "input": input_,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if context_index is not UNSET:
            field_dict["context_index"] = context_index

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        input_ = d.pop("input")

        id = d.pop("id", UNSET)

        context_index = d.pop("context_index", UNSET)

        decision_input = cls(
            input_=input_,
            id=id,
            context_index=context_index,
        )

        return decision_input
