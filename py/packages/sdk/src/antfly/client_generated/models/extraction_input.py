from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_input_metadata import ExtractionInputMetadata
    from ..models.extraction_token import ExtractionToken
    from ..models.image_url_content_part import ImageURLContentPart
    from ..models.media_content_part import MediaContentPart
    from ..models.text_content_part import TextContentPart


T = TypeVar("T", bound="ExtractionInput")


@_attrs_define
class ExtractionInput:
    """
    Attributes:
        content (list[ImageURLContentPart | MediaContentPart | TextContentPart] | str): OpenAI-compatible message
            content: either text or an array of content parts.
        id (str | Unset):
        tokens (list[ExtractionToken] | Unset):
        metadata (ExtractionInputMetadata | Unset):
    """

    content: list[ImageURLContentPart | MediaContentPart | TextContentPart] | str
    id: str | Unset = UNSET
    tokens: list[ExtractionToken] | Unset = UNSET
    metadata: ExtractionInputMetadata | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.text_content_part import TextContentPart

        content: list[dict[str, Any]] | str
        if isinstance(self.content, list):
            content = []
            for componentsschemas_chat_message_content_type_1_item_data in self.content:
                componentsschemas_chat_message_content_type_1_item: dict[str, Any]
                if isinstance(componentsschemas_chat_message_content_type_1_item_data, TextContentPart):
                    componentsschemas_chat_message_content_type_1_item = (
                        componentsschemas_chat_message_content_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_chat_message_content_type_1_item_data, ImageURLContentPart):
                    componentsschemas_chat_message_content_type_1_item = (
                        componentsschemas_chat_message_content_type_1_item_data.to_dict()
                    )
                else:
                    componentsschemas_chat_message_content_type_1_item = (
                        componentsschemas_chat_message_content_type_1_item_data.to_dict()
                    )

                content.append(componentsschemas_chat_message_content_type_1_item)

        else:
            content = self.content

        id = self.id

        tokens: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tokens, Unset):
            tokens = []
            for tokens_item_data in self.tokens:
                tokens_item = tokens_item_data.to_dict()
                tokens.append(tokens_item)

        metadata: dict[str, Any] | Unset = UNSET
        if not isinstance(self.metadata, Unset):
            metadata = self.metadata.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "content": content,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if tokens is not UNSET:
            field_dict["tokens"] = tokens
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_input_metadata import ExtractionInputMetadata
        from ..models.extraction_token import ExtractionToken
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        d = dict(src_dict)

        def _parse_content(data: object) -> list[ImageURLContentPart | MediaContentPart | TextContentPart] | str:
            try:
                if not isinstance(data, list):
                    raise TypeError()
                componentsschemas_chat_message_content_type_1 = []
                _componentsschemas_chat_message_content_type_1 = data
                for (
                    componentsschemas_chat_message_content_type_1_item_data
                ) in _componentsschemas_chat_message_content_type_1:

                    def _parse_componentsschemas_chat_message_content_type_1_item(
                        data: object,
                    ) -> ImageURLContentPart | MediaContentPart | TextContentPart:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_content_part_type_0 = TextContentPart.from_dict(data)

                            return componentsschemas_content_part_type_0
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_content_part_type_1 = ImageURLContentPart.from_dict(data)

                            return componentsschemas_content_part_type_1
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_content_part_type_2 = MediaContentPart.from_dict(data)

                        return componentsschemas_content_part_type_2

                    componentsschemas_chat_message_content_type_1_item = (
                        _parse_componentsschemas_chat_message_content_type_1_item(
                            componentsschemas_chat_message_content_type_1_item_data
                        )
                    )

                    componentsschemas_chat_message_content_type_1.append(
                        componentsschemas_chat_message_content_type_1_item
                    )

                return componentsschemas_chat_message_content_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[ImageURLContentPart | MediaContentPart | TextContentPart] | str, data)

        content = _parse_content(d.pop("content"))

        id = d.pop("id", UNSET)

        _tokens = d.pop("tokens", UNSET)
        tokens: list[ExtractionToken] | Unset = UNSET
        if _tokens is not UNSET:
            tokens = []
            for tokens_item_data in _tokens:
                tokens_item = ExtractionToken.from_dict(tokens_item_data)

                tokens.append(tokens_item)

        _metadata = d.pop("metadata", UNSET)
        metadata: ExtractionInputMetadata | Unset
        if isinstance(_metadata, Unset):
            metadata = UNSET
        else:
            metadata = ExtractionInputMetadata.from_dict(_metadata)

        extraction_input = cls(
            content=content,
            id=id,
            tokens=tokens,
            metadata=metadata,
        )

        extraction_input.additional_properties = d
        return extraction_input

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
