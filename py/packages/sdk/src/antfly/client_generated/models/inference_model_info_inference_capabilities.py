from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceModelInfoInferenceCapabilities")


@_attrs_define
class InferenceModelInfoInferenceCapabilities:
    """Resolved executor/task facts; generation bits are false when the selected executor does not implement them.

    Attributes:
        version (int | Unset):
        task (str | Unset):
        generation_grammar_v1 (bool | Unset):
        generation_token_logprobs_v1 (bool | Unset):
    """

    version: int | Unset = UNSET
    task: str | Unset = UNSET
    generation_grammar_v1: bool | Unset = UNSET
    generation_token_logprobs_v1: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        version = self.version

        task = self.task

        generation_grammar_v1 = self.generation_grammar_v1

        generation_token_logprobs_v1 = self.generation_token_logprobs_v1

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if version is not UNSET:
            field_dict["version"] = version
        if task is not UNSET:
            field_dict["task"] = task
        if generation_grammar_v1 is not UNSET:
            field_dict["generation_grammar_v1"] = generation_grammar_v1
        if generation_token_logprobs_v1 is not UNSET:
            field_dict["generation_token_logprobs_v1"] = generation_token_logprobs_v1

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        version = d.pop("version", UNSET)

        task = d.pop("task", UNSET)

        generation_grammar_v1 = d.pop("generation_grammar_v1", UNSET)

        generation_token_logprobs_v1 = d.pop("generation_token_logprobs_v1", UNSET)

        inference_model_info_inference_capabilities = cls(
            version=version,
            task=task,
            generation_grammar_v1=generation_grammar_v1,
            generation_token_logprobs_v1=generation_token_logprobs_v1,
        )

        inference_model_info_inference_capabilities.additional_properties = d
        return inference_model_info_inference_capabilities

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
