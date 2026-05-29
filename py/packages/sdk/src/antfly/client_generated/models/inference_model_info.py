from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceModelInfo")


@_attrs_define
class InferenceModelInfo:
    """Information about a model including its capabilities

    Attributes:
        capabilities (list[str] | Unset): List of capabilities this model supports (omitted when empty). For rerankers,
            `late_interaction` or `colbert` selects native MaxSim token scoring.
        inputs (list[str] | Unset): List of input modalities this model accepts, such as `text`, `image`, or `audio`
    """

    capabilities: list[str] | Unset = UNSET
    inputs: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        capabilities: list[str] | Unset = UNSET
        if not isinstance(self.capabilities, Unset):
            capabilities = self.capabilities

        inputs: list[str] | Unset = UNSET
        if not isinstance(self.inputs, Unset):
            inputs = self.inputs

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if capabilities is not UNSET:
            field_dict["capabilities"] = capabilities
        if inputs is not UNSET:
            field_dict["inputs"] = inputs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        capabilities = cast(list[str], d.pop("capabilities", UNSET))

        inputs = cast(list[str], d.pop("inputs", UNSET))

        inference_model_info = cls(
            capabilities=capabilities,
            inputs=inputs,
        )

        inference_model_info.additional_properties = d
        return inference_model_info

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
