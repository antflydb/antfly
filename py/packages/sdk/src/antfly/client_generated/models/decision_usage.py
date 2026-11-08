from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="DecisionUsage")


@_attrs_define
class DecisionUsage:
    """
    Attributes:
        input_tokens (int): Encoded tokens including repeated input text for split model tasks.
        output_tokens (int): Zero for classifiers, which generate no tokens.
        estimated (bool | Unset): True for estimated usage, including apportioned shared generated-batch costs and
            embedding-based estimates. Generated cascades divide shared totals evenly, assigning integer remainders in input
            order; their request aggregate remains measured.
    """

    input_tokens: int
    output_tokens: int
    estimated: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        input_tokens = self.input_tokens

        output_tokens = self.output_tokens

        estimated = self.estimated

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        )
        if estimated is not UNSET:
            field_dict["estimated"] = estimated

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        input_tokens = d.pop("input_tokens")

        output_tokens = d.pop("output_tokens")

        estimated = d.pop("estimated", UNSET)

        decision_usage = cls(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated=estimated,
        )

        return decision_usage
