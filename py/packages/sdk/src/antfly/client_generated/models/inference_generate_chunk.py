from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_generate_chunk_object import InferenceGenerateChunkObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_generate_chunk_choice import InferenceGenerateChunkChoice
    from ..models.inference_generate_speculation_status import InferenceGenerateSpeculationStatus
    from ..models.inference_generate_usage import InferenceGenerateUsage


T = TypeVar("T", bound="InferenceGenerateChunk")


@_attrs_define
class InferenceGenerateChunk:
    """Streaming generation chunk (SSE event data)

    Attributes:
        id (str):
        object_ (InferenceGenerateChunkObject):
        created (int):
        model (str):
        choices (list[InferenceGenerateChunkChoice]):
        usage (InferenceGenerateUsage | None | Unset): Authoritative token accounting. When
            `stream_options.include_usage` is true,
            streaming responses emit this only on the final chunk, after the finish chunk
            and before the `[DONE]` sentinel; that chunk has an empty `choices` array.
        speculation (InferenceGenerateSpeculationStatus | None | Unset):
    """

    id: str
    object_: InferenceGenerateChunkObject
    created: int
    model: str
    choices: list[InferenceGenerateChunkChoice]
    usage: InferenceGenerateUsage | None | Unset = UNSET
    speculation: InferenceGenerateSpeculationStatus | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.inference_generate_speculation_status import InferenceGenerateSpeculationStatus
        from ..models.inference_generate_usage import InferenceGenerateUsage

        id = self.id

        object_ = self.object_.value

        created = self.created

        model = self.model

        choices = []
        for choices_item_data in self.choices:
            choices_item = choices_item_data.to_dict()
            choices.append(choices_item)

        usage: dict[str, Any] | None | Unset
        if isinstance(self.usage, Unset):
            usage = UNSET
        elif isinstance(self.usage, InferenceGenerateUsage):
            usage = self.usage.to_dict()
        else:
            usage = self.usage

        speculation: dict[str, Any] | None | Unset
        if isinstance(self.speculation, Unset):
            speculation = UNSET
        elif isinstance(self.speculation, InferenceGenerateSpeculationStatus):
            speculation = self.speculation.to_dict()
        else:
            speculation = self.speculation

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "object": object_,
                "created": created,
                "model": model,
                "choices": choices,
            }
        )
        if usage is not UNSET:
            field_dict["usage"] = usage
        if speculation is not UNSET:
            field_dict["speculation"] = speculation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_generate_chunk_choice import InferenceGenerateChunkChoice
        from ..models.inference_generate_speculation_status import InferenceGenerateSpeculationStatus
        from ..models.inference_generate_usage import InferenceGenerateUsage

        d = dict(src_dict)
        id = d.pop("id")

        object_ = InferenceGenerateChunkObject(d.pop("object"))

        created = d.pop("created")

        model = d.pop("model")

        choices = []
        _choices = d.pop("choices")
        for choices_item_data in _choices:
            choices_item = InferenceGenerateChunkChoice.from_dict(choices_item_data)

            choices.append(choices_item)

        def _parse_usage(data: object) -> InferenceGenerateUsage | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                usage_type_1 = InferenceGenerateUsage.from_dict(data)

                return usage_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(InferenceGenerateUsage | None | Unset, data)

        usage = _parse_usage(d.pop("usage", UNSET))

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

        inference_generate_chunk = cls(
            id=id,
            object_=object_,
            created=created,
            model=model,
            choices=choices,
            usage=usage,
            speculation=speculation,
        )

        inference_generate_chunk.additional_properties = d
        return inference_generate_chunk

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
