from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chat_tools_config import ChatToolsConfig


T = TypeVar("T", bound="RetrievalStepConfig")


@_attrs_define
class RetrievalStepConfig:
    """Configuration for the retrieval step. Retrieval tools are constrained by
    the top-level request tools policy when both are present.

        Attributes:
            tools (ChatToolsConfig | Unset): Configuration for retrieval agent tools.

                If `enabled_tools` is empty/omitted, retrieval agents default to all retrieval tools
                available for the request. Explicit retrieval policies should use semantic_search
                for vector retrieval.

                For models that don't support native tool calling (e.g., Ollama),
                a prompt-based fallback is used with structured output parsing.
    """

    tools: ChatToolsConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        tools: dict[str, Any] | Unset = UNSET
        if not isinstance(self.tools, Unset):
            tools = self.tools.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if tools is not UNSET:
            field_dict["tools"] = tools

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chat_tools_config import ChatToolsConfig

        d = dict(src_dict)
        _tools = d.pop("tools", UNSET)
        tools: ChatToolsConfig | Unset
        if isinstance(_tools, Unset):
            tools = UNSET
        else:
            tools = ChatToolsConfig.from_dict(_tools)

        retrieval_step_config = cls(
            tools=tools,
        )

        retrieval_step_config.additional_properties = d
        return retrieval_step_config

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
