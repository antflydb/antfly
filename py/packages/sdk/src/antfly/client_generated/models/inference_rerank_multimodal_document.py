from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.image_url_content_part import ImageURLContentPart
    from ..models.media_content_part import MediaContentPart
    from ..models.text_content_part import TextContentPart


T = TypeVar("T", bound="InferenceRerankMultimodalDocument")


@_attrs_define
class InferenceRerankMultimodalDocument:
    """
    Attributes:
        content (list[ImageURLContentPart | MediaContentPart | TextContentPart] | str): Message content. Supports two
            formats:
            - Simple string: "Hello, how are you?"
            - Array of content parts: [{"type": "text", "text": "Hello"}]
        id (str | Unset): Optional caller-provided document identifier
    """

    content: list[ImageURLContentPart | MediaContentPart | TextContentPart] | str
    id: str | Unset = UNSET
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

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "content": content,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
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

        inference_rerank_multimodal_document = cls(
            content=content,
            id=id,
        )

        inference_rerank_multimodal_document.additional_properties = d
        return inference_rerank_multimodal_document

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
