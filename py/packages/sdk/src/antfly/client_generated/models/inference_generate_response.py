from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_generate_response_object import InferenceGenerateResponseObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_context_checkpoint import InferenceContextCheckpoint
    from ..models.inference_context_diagnostics import InferenceContextDiagnostics
    from ..models.inference_generate_choice import InferenceGenerateChoice
    from ..models.inference_generate_speculation_status import InferenceGenerateSpeculationStatus
    from ..models.inference_generate_usage import InferenceGenerateUsage


T = TypeVar("T", bound="InferenceGenerateResponse")


@_attrs_define
class InferenceGenerateResponse:
    """OpenAI-compatible chat completion response

    Attributes:
        id (str): A unique identifier for the chat completion Example: chatcmpl-abc123.
        object_ (InferenceGenerateResponseObject): The object type, always "chat.completion"
        created (int): Unix timestamp (seconds) when the completion was created Example: 1704123456.
        model (str): Model used for generation
        choices (list[InferenceGenerateChoice]): List of completion choices (currently always 1)
        usage (InferenceGenerateUsage):
        speculation (InferenceGenerateSpeculationStatus | None | Unset):
        context (InferenceContextDiagnostics | Unset):
        checkpoint (InferenceContextCheckpoint | Unset):
    """

    id: str
    object_: InferenceGenerateResponseObject
    created: int
    model: str
    choices: list[InferenceGenerateChoice]
    usage: InferenceGenerateUsage
    speculation: InferenceGenerateSpeculationStatus | None | Unset = UNSET
    context: InferenceContextDiagnostics | Unset = UNSET
    checkpoint: InferenceContextCheckpoint | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.inference_generate_speculation_status import InferenceGenerateSpeculationStatus

        id = self.id

        object_ = self.object_.value

        created = self.created

        model = self.model

        choices = []
        for choices_item_data in self.choices:
            choices_item = choices_item_data.to_dict()
            choices.append(choices_item)

        usage = self.usage.to_dict()

        speculation: dict[str, Any] | None | Unset
        if isinstance(self.speculation, Unset):
            speculation = UNSET
        elif isinstance(self.speculation, InferenceGenerateSpeculationStatus):
            speculation = self.speculation.to_dict()
        else:
            speculation = self.speculation

        context: dict[str, Any] | Unset = UNSET
        if not isinstance(self.context, Unset):
            context = self.context.to_dict()

        checkpoint: dict[str, Any] | Unset = UNSET
        if not isinstance(self.checkpoint, Unset):
            checkpoint = self.checkpoint.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "object": object_,
                "created": created,
                "model": model,
                "choices": choices,
                "usage": usage,
            }
        )
        if speculation is not UNSET:
            field_dict["speculation"] = speculation
        if context is not UNSET:
            field_dict["context"] = context
        if checkpoint is not UNSET:
            field_dict["checkpoint"] = checkpoint

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_context_checkpoint import InferenceContextCheckpoint
        from ..models.inference_context_diagnostics import InferenceContextDiagnostics
        from ..models.inference_generate_choice import InferenceGenerateChoice
        from ..models.inference_generate_speculation_status import InferenceGenerateSpeculationStatus
        from ..models.inference_generate_usage import InferenceGenerateUsage

        d = dict(src_dict)
        id = d.pop("id")

        object_ = InferenceGenerateResponseObject(d.pop("object"))

        created = d.pop("created")

        model = d.pop("model")

        choices = []
        _choices = d.pop("choices")
        for choices_item_data in _choices:
            choices_item = InferenceGenerateChoice.from_dict(choices_item_data)

            choices.append(choices_item)

        usage = InferenceGenerateUsage.from_dict(d.pop("usage"))

        def _parse_speculation(data: object) -> InferenceGenerateSpeculationStatus | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                speculation_type_1 = InferenceGenerateSpeculationStatus.from_dict(data)

                return speculation_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(InferenceGenerateSpeculationStatus | None | Unset, data)

        speculation = _parse_speculation(d.pop("speculation", UNSET))

        _context = d.pop("context", UNSET)
        context: InferenceContextDiagnostics | Unset
        if isinstance(_context, Unset):
            context = UNSET
        else:
            context = InferenceContextDiagnostics.from_dict(_context)

        _checkpoint = d.pop("checkpoint", UNSET)
        checkpoint: InferenceContextCheckpoint | Unset
        if isinstance(_checkpoint, Unset):
            checkpoint = UNSET
        else:
            checkpoint = InferenceContextCheckpoint.from_dict(_checkpoint)

        inference_generate_response = cls(
            id=id,
            object_=object_,
            created=created,
            model=model,
            choices=choices,
            usage=usage,
            speculation=speculation,
            context=context,
            checkpoint=checkpoint,
        )

        inference_generate_response.additional_properties = d
        return inference_generate_response

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
