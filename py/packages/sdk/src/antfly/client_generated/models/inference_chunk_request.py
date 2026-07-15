from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_chunk_config import InferenceChunkConfig
    from ..models.media_content_part import MediaContentPart
    from ..models.text_content_part import TextContentPart


T = TypeVar("T", bound="InferenceChunkRequest")


@_attrs_define
class InferenceChunkRequest:
    """
    Attributes:
        input_ (MediaContentPart | str | TextContentPart): Input content to chunk. Supports two formats:
            - Text string: `"This is a long document..."`
            - ContentPart: `{"type": "media", "data": "<base64>", "mime_type": "audio/wav"}`
            - ContentPart: `{"type": "text", "text": "..."}`
        config (InferenceChunkConfig | Unset): Configuration for chunking requests to Antfly inference.
            Combines shared text options with inference-specific audio/VAD options.
    """

    input_: MediaContentPart | str | TextContentPart
    config: InferenceChunkConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        input_: dict[str, Any] | str
        if isinstance(self.input_, TextContentPart):
            input_ = self.input_.to_dict()
        elif isinstance(self.input_, MediaContentPart):
            input_ = self.input_.to_dict()
        else:
            input_ = self.input_

        config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "input": input_,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_chunk_config import InferenceChunkConfig
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        d = dict(src_dict)

        def _parse_input_(data: object) -> MediaContentPart | str | TextContentPart:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_inference_chunk_content_part_type_0 = TextContentPart.from_dict(data)

                return componentsschemas_inference_chunk_content_part_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_inference_chunk_content_part_type_1 = MediaContentPart.from_dict(data)

                return componentsschemas_inference_chunk_content_part_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MediaContentPart | str | TextContentPart, data)

        input_ = _parse_input_(d.pop("input"))

        _config = d.pop("config", UNSET)
        config: InferenceChunkConfig | Unset
        if isinstance(_config, Unset):
            config = UNSET
        else:
            config = InferenceChunkConfig.from_dict(_config)

        inference_chunk_request = cls(
            input_=input_,
            config=config,
        )

        inference_chunk_request.additional_properties = d
        return inference_chunk_request

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
