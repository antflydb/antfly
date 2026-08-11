from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceGenerateChatTemplateKwargs")


@_attrs_define
class InferenceGenerateChatTemplateKwargs:
    """Typed options passed to the model's chat template.

    Attributes:
        enable_thinking (bool | Unset): Controls templates that support an `enable_thinking` variable. False asks the
            template to open a public/final response channel directly. Omitted preserves the
            model template's default behavior.
    """

    enable_thinking: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        enable_thinking = self.enable_thinking

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if enable_thinking is not UNSET:
            field_dict["enable_thinking"] = enable_thinking

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enable_thinking = d.pop("enable_thinking", UNSET)

        inference_generate_chat_template_kwargs = cls(
            enable_thinking=enable_thinking,
        )

        return inference_generate_chat_template_kwargs
