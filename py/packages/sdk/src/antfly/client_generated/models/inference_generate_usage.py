from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceGenerateUsage")


@_attrs_define
class InferenceGenerateUsage:
    """
    Attributes:
        prompt_tokens (int): Number of tokens in the prompt
        completion_tokens (int): Number of tokens in the completion
        total_tokens (int): Total tokens used (prompt + completion)
        model_calls (int | Unset): Cumulative logical child invocations for a document run.
        physical_model_calls (int | Unset): Physical child operation invocations, separate from logical composite calls.
        rendered_pixels (int | Unset): Cumulative rendered or decoded pixels charged by document work.
        output_bytes (int | Unset): Cumulative document text bytes charged before envelope serialization.
        completed_pages (int | Unset): Selected pages completed successfully.
        visited_pages (int | Unset): Selected pages visited, including unresolved work.
        cached_prompt_tokens (int | Unset): Prompt tokens served from inference-native prefix KV cache
    """

    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model_calls: int | Unset = UNSET
    physical_model_calls: int | Unset = UNSET
    rendered_pixels: int | Unset = UNSET
    output_bytes: int | Unset = UNSET
    completed_pages: int | Unset = UNSET
    visited_pages: int | Unset = UNSET
    cached_prompt_tokens: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        prompt_tokens = self.prompt_tokens

        completion_tokens = self.completion_tokens

        total_tokens = self.total_tokens

        model_calls = self.model_calls

        physical_model_calls = self.physical_model_calls

        rendered_pixels = self.rendered_pixels

        output_bytes = self.output_bytes

        completed_pages = self.completed_pages

        visited_pages = self.visited_pages

        cached_prompt_tokens = self.cached_prompt_tokens

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
            }
        )
        if model_calls is not UNSET:
            field_dict["model_calls"] = model_calls
        if physical_model_calls is not UNSET:
            field_dict["physical_model_calls"] = physical_model_calls
        if rendered_pixels is not UNSET:
            field_dict["rendered_pixels"] = rendered_pixels
        if output_bytes is not UNSET:
            field_dict["output_bytes"] = output_bytes
        if completed_pages is not UNSET:
            field_dict["completed_pages"] = completed_pages
        if visited_pages is not UNSET:
            field_dict["visited_pages"] = visited_pages
        if cached_prompt_tokens is not UNSET:
            field_dict["cached_prompt_tokens"] = cached_prompt_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        prompt_tokens = d.pop("prompt_tokens")

        completion_tokens = d.pop("completion_tokens")

        total_tokens = d.pop("total_tokens")

        model_calls = d.pop("model_calls", UNSET)

        physical_model_calls = d.pop("physical_model_calls", UNSET)

        rendered_pixels = d.pop("rendered_pixels", UNSET)

        output_bytes = d.pop("output_bytes", UNSET)

        completed_pages = d.pop("completed_pages", UNSET)

        visited_pages = d.pop("visited_pages", UNSET)

        cached_prompt_tokens = d.pop("cached_prompt_tokens", UNSET)

        inference_generate_usage = cls(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            model_calls=model_calls,
            physical_model_calls=physical_model_calls,
            rendered_pixels=rendered_pixels,
            output_bytes=output_bytes,
            completed_pages=completed_pages,
            visited_pages=visited_pages,
            cached_prompt_tokens=cached_prompt_tokens,
        )

        inference_generate_usage.additional_properties = d
        return inference_generate_usage

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
