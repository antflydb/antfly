from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemorillaGenerateUsage")


@_attrs_define
class MemorillaGenerateUsage:
    """
    Attributes:
        encoder_query_tokens (int):
        prompt_tokens (int):
        completion_tokens (int):
        total_prompt_tokens (int | Unset): Source-verified mode only. Decoder prompt tokens across memory, independent
            source answering and verification phases, regardless of the final answer path.
        total_completion_tokens (int | Unset): Source-verified mode only. Actual decoder output tokens across every
            phase, including discarded candidates and verifier labels.
        verification_calls (int | Unset):
        source_fallback_used (bool | Unset): Source-verified mode only. True when the final selection uses the source-
            answer or abstention path, false for memory_verified. Independent source-answer work occurs even for
            memory_verified and is included in total token usage.
    """

    encoder_query_tokens: int
    prompt_tokens: int
    completion_tokens: int
    total_prompt_tokens: int | Unset = UNSET
    total_completion_tokens: int | Unset = UNSET
    verification_calls: int | Unset = UNSET
    source_fallback_used: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        encoder_query_tokens = self.encoder_query_tokens

        prompt_tokens = self.prompt_tokens

        completion_tokens = self.completion_tokens

        total_prompt_tokens = self.total_prompt_tokens

        total_completion_tokens = self.total_completion_tokens

        verification_calls = self.verification_calls

        source_fallback_used = self.source_fallback_used

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "encoder_query_tokens": encoder_query_tokens,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
            }
        )
        if total_prompt_tokens is not UNSET:
            field_dict["total_prompt_tokens"] = total_prompt_tokens
        if total_completion_tokens is not UNSET:
            field_dict["total_completion_tokens"] = total_completion_tokens
        if verification_calls is not UNSET:
            field_dict["verification_calls"] = verification_calls
        if source_fallback_used is not UNSET:
            field_dict["source_fallback_used"] = source_fallback_used

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        encoder_query_tokens = d.pop("encoder_query_tokens")

        prompt_tokens = d.pop("prompt_tokens")

        completion_tokens = d.pop("completion_tokens")

        total_prompt_tokens = d.pop("total_prompt_tokens", UNSET)

        total_completion_tokens = d.pop("total_completion_tokens", UNSET)

        verification_calls = d.pop("verification_calls", UNSET)

        source_fallback_used = d.pop("source_fallback_used", UNSET)

        memorilla_generate_usage = cls(
            encoder_query_tokens=encoder_query_tokens,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_prompt_tokens=total_prompt_tokens,
            total_completion_tokens=total_completion_tokens,
            verification_calls=verification_calls,
            source_fallback_used=source_fallback_used,
        )

        return memorilla_generate_usage
