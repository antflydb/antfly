from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_context_prepare_request_mode import InferenceContextPrepareRequestMode
from ..models.inference_tool_choice_type_0 import InferenceToolChoiceType0
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_chat_message import InferenceChatMessage
    from ..models.inference_context_checkpoint import InferenceContextCheckpoint
    from ..models.inference_generate_chat_template_kwargs import InferenceGenerateChatTemplateKwargs
    from ..models.inference_tool import InferenceTool
    from ..models.inference_tool_choice_type_1 import InferenceToolChoiceType1


T = TypeVar("T", bound="InferenceContextPrepareRequest")


@_attrs_define
class InferenceContextPrepareRequest:
    """
    Attributes:
        model (str):
        messages (list[InferenceChatMessage]):
        tools (list[InferenceTool] | Unset):
        tool_choice (InferenceToolChoiceType0 | InferenceToolChoiceType1 | Unset): Controls how the model uses tools.
            Options:
            - "auto": Model decides whether to call a tool (default)
            - "none": Model will not call any tools
            - "required": Model must call at least one tool
            - object: Force a specific function to be called
        chat_template_kwargs (InferenceGenerateChatTemplateKwargs | Unset): Typed options passed to the model's chat
            template.
        max_tokens (int | Unset): Output-token reservation used to calculate the input budget. Default: 256.
        mode (InferenceContextPrepareRequestMode | Unset):  Default: InferenceContextPrepareRequestMode.INSPECT.
        checkpoint (InferenceContextCheckpoint | Unset):
        max_input_tokens (int | Unset):
        keep_recent_turns (int | Unset):  Default: 2.
    """

    model: str
    messages: list[InferenceChatMessage]
    tools: list[InferenceTool] | Unset = UNSET
    tool_choice: InferenceToolChoiceType0 | InferenceToolChoiceType1 | Unset = UNSET
    chat_template_kwargs: InferenceGenerateChatTemplateKwargs | Unset = UNSET
    max_tokens: int | Unset = 256
    mode: InferenceContextPrepareRequestMode | Unset = InferenceContextPrepareRequestMode.INSPECT
    checkpoint: InferenceContextCheckpoint | Unset = UNSET
    max_input_tokens: int | Unset = UNSET
    keep_recent_turns: int | Unset = 2

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        messages = []
        for messages_item_data in self.messages:
            messages_item = messages_item_data.to_dict()
            messages.append(messages_item)

        tools: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tools, Unset):
            tools = []
            for tools_item_data in self.tools:
                tools_item = tools_item_data.to_dict()
                tools.append(tools_item)

        tool_choice: dict[str, Any] | str | Unset
        if isinstance(self.tool_choice, Unset):
            tool_choice = UNSET
        elif isinstance(self.tool_choice, InferenceToolChoiceType0):
            tool_choice = self.tool_choice.value
        else:
            tool_choice = self.tool_choice.to_dict()

        chat_template_kwargs: dict[str, Any] | Unset = UNSET
        if not isinstance(self.chat_template_kwargs, Unset):
            chat_template_kwargs = self.chat_template_kwargs.to_dict()

        max_tokens = self.max_tokens

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        checkpoint: dict[str, Any] | Unset = UNSET
        if not isinstance(self.checkpoint, Unset):
            checkpoint = self.checkpoint.to_dict()

        max_input_tokens = self.max_input_tokens

        keep_recent_turns = self.keep_recent_turns

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "messages": messages,
            }
        )
        if tools is not UNSET:
            field_dict["tools"] = tools
        if tool_choice is not UNSET:
            field_dict["tool_choice"] = tool_choice
        if chat_template_kwargs is not UNSET:
            field_dict["chat_template_kwargs"] = chat_template_kwargs
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if mode is not UNSET:
            field_dict["mode"] = mode
        if checkpoint is not UNSET:
            field_dict["checkpoint"] = checkpoint
        if max_input_tokens is not UNSET:
            field_dict["max_input_tokens"] = max_input_tokens
        if keep_recent_turns is not UNSET:
            field_dict["keep_recent_turns"] = keep_recent_turns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_chat_message import InferenceChatMessage
        from ..models.inference_context_checkpoint import InferenceContextCheckpoint
        from ..models.inference_generate_chat_template_kwargs import InferenceGenerateChatTemplateKwargs
        from ..models.inference_tool import InferenceTool
        from ..models.inference_tool_choice_type_1 import InferenceToolChoiceType1

        d = dict(src_dict)
        model = d.pop("model")

        messages = []
        _messages = d.pop("messages")
        for messages_item_data in _messages:
            messages_item = InferenceChatMessage.from_dict(messages_item_data)

            messages.append(messages_item)

        _tools = d.pop("tools", UNSET)
        tools: list[InferenceTool] | Unset = UNSET
        if _tools is not UNSET:
            tools = []
            for tools_item_data in _tools:
                tools_item = InferenceTool.from_dict(tools_item_data)

                tools.append(tools_item)

        def _parse_tool_choice(data: object) -> InferenceToolChoiceType0 | InferenceToolChoiceType1 | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_inference_tool_choice_type_0 = InferenceToolChoiceType0(data)

                return componentsschemas_inference_tool_choice_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_inference_tool_choice_type_1 = InferenceToolChoiceType1.from_dict(data)

            return componentsschemas_inference_tool_choice_type_1

        tool_choice = _parse_tool_choice(d.pop("tool_choice", UNSET))

        _chat_template_kwargs = d.pop("chat_template_kwargs", UNSET)
        chat_template_kwargs: InferenceGenerateChatTemplateKwargs | Unset
        if isinstance(_chat_template_kwargs, Unset):
            chat_template_kwargs = UNSET
        else:
            chat_template_kwargs = InferenceGenerateChatTemplateKwargs.from_dict(_chat_template_kwargs)

        max_tokens = d.pop("max_tokens", UNSET)

        _mode = d.pop("mode", UNSET)
        mode: InferenceContextPrepareRequestMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = InferenceContextPrepareRequestMode(_mode)

        _checkpoint = d.pop("checkpoint", UNSET)
        checkpoint: InferenceContextCheckpoint | Unset
        if isinstance(_checkpoint, Unset):
            checkpoint = UNSET
        else:
            checkpoint = InferenceContextCheckpoint.from_dict(_checkpoint)

        max_input_tokens = d.pop("max_input_tokens", UNSET)

        keep_recent_turns = d.pop("keep_recent_turns", UNSET)

        inference_context_prepare_request = cls(
            model=model,
            messages=messages,
            tools=tools,
            tool_choice=tool_choice,
            chat_template_kwargs=chat_template_kwargs,
            max_tokens=max_tokens,
            mode=mode,
            checkpoint=checkpoint,
            max_input_tokens=max_input_tokens,
            keep_recent_turns=keep_recent_turns,
        )

        return inference_context_prepare_request
