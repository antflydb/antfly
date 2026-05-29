from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.image_url_content_part import ImageURLContentPart
    from ..models.media_content_part import MediaContentPart
    from ..models.termite_chunk_config import TermiteChunkConfig
    from ..models.text_content_part import TextContentPart


T = TypeVar("T", bound="TermiteChunkRequest")


@_attrs_define
class TermiteChunkRequest:
    """
    Attributes:
        input_ (ImageURLContentPart | MediaContentPart | str | TextContentPart | Unset): Input content to chunk.
            Supports two formats:
            - Text string: `"This is a long document..."` (backward compatible)
            - ContentPart: `{"type": "media", "data": "<base64>", "mime_type": "audio/wav"}`
            - ContentPart: `{"type": "text", "text": "..."}`
        text (str | Unset): DEPRECATED: Use 'input' instead. Text to chunk. Example: This is a long document that needs
            to be split into smaller chunks....
        config (TermiteChunkConfig | Unset): Configuration for chunking requests to Termite API.
            Combines shared text options with Termite-specific audio/VAD options.
    """

    input_: ImageURLContentPart | MediaContentPart | str | TextContentPart | Unset = UNSET
    text: str | Unset = UNSET
    config: TermiteChunkConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        input_: dict[str, Any] | str | Unset
        if isinstance(self.input_, Unset):
            input_ = UNSET
        elif isinstance(self.input_, TextContentPart):
            input_ = self.input_.to_dict()
        elif isinstance(self.input_, ImageURLContentPart):
            input_ = self.input_.to_dict()
        elif isinstance(self.input_, MediaContentPart):
            input_ = self.input_.to_dict()
        else:
            input_ = self.input_

        text = self.text

        config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if input_ is not UNSET:
            field_dict["input"] = input_
        if text is not UNSET:
            field_dict["text"] = text
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.media_content_part import MediaContentPart
        from ..models.termite_chunk_config import TermiteChunkConfig
        from ..models.text_content_part import TextContentPart

        d = dict(src_dict)

        def _parse_input_(data: object) -> ImageURLContentPart | MediaContentPart | str | TextContentPart | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_termite_content_part_type_0 = TextContentPart.from_dict(data)

                return componentsschemas_termite_content_part_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_termite_content_part_type_1 = ImageURLContentPart.from_dict(data)

                return componentsschemas_termite_content_part_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_termite_content_part_type_2 = MediaContentPart.from_dict(data)

                return componentsschemas_termite_content_part_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ImageURLContentPart | MediaContentPart | str | TextContentPart | Unset, data)

        input_ = _parse_input_(d.pop("input", UNSET))

        text = d.pop("text", UNSET)

        _config = d.pop("config", UNSET)
        config: TermiteChunkConfig | Unset
        if isinstance(_config, Unset):
            config = UNSET
        else:
            config = TermiteChunkConfig.from_dict(_config)

        termite_chunk_request = cls(
            input_=input_,
            text=text,
            config=config,
        )

        termite_chunk_request.additional_properties = d
        return termite_chunk_request

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
