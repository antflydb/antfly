from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferenceDecideResponseUsage")


@_attrs_define
class InferenceDecideResponseUsage:
    """
    Attributes:
        input_tokens (int): Encoded prompt tokens consumed by the executor, including repeated state text for split
            GLiNER tasks.
        output_tokens (int): Zero for classifier executors, which emit no generated tokens.
    """

    input_tokens: int
    output_tokens: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        input_tokens = self.input_tokens

        output_tokens = self.output_tokens

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        input_tokens = d.pop("input_tokens")

        output_tokens = d.pop("output_tokens")

        inference_decide_response_usage = cls(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

        inference_decide_response_usage.additional_properties = d
        return inference_decide_response_usage

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
