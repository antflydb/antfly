from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_chat_message import InferenceChatMessage
    from ..models.inference_context_checkpoint import InferenceContextCheckpoint
    from ..models.inference_context_diagnostics import InferenceContextDiagnostics
    from ..models.inference_generate_usage import InferenceGenerateUsage


T = TypeVar("T", bound="InferenceContextPrepareResponse")


@_attrs_define
class InferenceContextPrepareResponse:
    """
    Attributes:
        messages (list[InferenceChatMessage]): Messages ready to send in a subsequent generation request. Original
            instruction messages are retained byte-for-byte even when they precede a
            checkpoint-covered archive; clients send these returned instructions with
            the active suffix and checkpoint.
        context (InferenceContextDiagnostics):
        usage (InferenceGenerateUsage):
        checkpoint (InferenceContextCheckpoint | Unset):
    """

    messages: list[InferenceChatMessage]
    context: InferenceContextDiagnostics
    usage: InferenceGenerateUsage
    checkpoint: InferenceContextCheckpoint | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        messages = []
        for messages_item_data in self.messages:
            messages_item = messages_item_data.to_dict()
            messages.append(messages_item)

        context = self.context.to_dict()

        usage = self.usage.to_dict()

        checkpoint: dict[str, Any] | Unset = UNSET
        if not isinstance(self.checkpoint, Unset):
            checkpoint = self.checkpoint.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "messages": messages,
                "context": context,
                "usage": usage,
            }
        )
        if checkpoint is not UNSET:
            field_dict["checkpoint"] = checkpoint

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_chat_message import InferenceChatMessage
        from ..models.inference_context_checkpoint import InferenceContextCheckpoint
        from ..models.inference_context_diagnostics import InferenceContextDiagnostics
        from ..models.inference_generate_usage import InferenceGenerateUsage

        d = dict(src_dict)
        messages = []
        _messages = d.pop("messages")
        for messages_item_data in _messages:
            messages_item = InferenceChatMessage.from_dict(messages_item_data)

            messages.append(messages_item)

        context = InferenceContextDiagnostics.from_dict(d.pop("context"))

        usage = InferenceGenerateUsage.from_dict(d.pop("usage"))

        _checkpoint = d.pop("checkpoint", UNSET)
        checkpoint: InferenceContextCheckpoint | Unset
        if isinstance(_checkpoint, Unset):
            checkpoint = UNSET
        else:
            checkpoint = InferenceContextCheckpoint.from_dict(_checkpoint)

        inference_context_prepare_response = cls(
            messages=messages,
            context=context,
            usage=usage,
            checkpoint=checkpoint,
        )

        return inference_context_prepare_response
