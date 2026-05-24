from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_image_url_content_part import TermiteImageURLContentPart
    from ..models.termite_media_content_part import TermiteMediaContentPart
    from ..models.termite_text_content_part import TermiteTextContentPart


T = TypeVar("T", bound="TermiteRerankMultimodalDocument")


@_attrs_define
class TermiteRerankMultimodalDocument:
    """
    Attributes:
        content (list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str): Message
            content. Supports two formats:
            - Simple string: "Hello, how are you?"
            - Array of content parts (OpenAI multimodal format): [{"type": "text", "text": "Hello"}]
        id (str | Unset): Optional caller-provided document identifier
    """

    content: list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str
    id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.termite_image_url_content_part import TermiteImageURLContentPart
        from ..models.termite_text_content_part import TermiteTextContentPart

        content: list[dict[str, Any]] | str
        if isinstance(self.content, list):
            content = []
            for componentsschemas_termite_chat_message_content_type_1_item_data in self.content:
                componentsschemas_termite_chat_message_content_type_1_item: dict[str, Any]
                if isinstance(componentsschemas_termite_chat_message_content_type_1_item_data, TermiteTextContentPart):
                    componentsschemas_termite_chat_message_content_type_1_item = (
                        componentsschemas_termite_chat_message_content_type_1_item_data.to_dict()
                    )
                elif isinstance(
                    componentsschemas_termite_chat_message_content_type_1_item_data, TermiteImageURLContentPart
                ):
                    componentsschemas_termite_chat_message_content_type_1_item = (
                        componentsschemas_termite_chat_message_content_type_1_item_data.to_dict()
                    )
                else:
                    componentsschemas_termite_chat_message_content_type_1_item = (
                        componentsschemas_termite_chat_message_content_type_1_item_data.to_dict()
                    )

                content.append(componentsschemas_termite_chat_message_content_type_1_item)

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
        from ..models.termite_image_url_content_part import TermiteImageURLContentPart
        from ..models.termite_media_content_part import TermiteMediaContentPart
        from ..models.termite_text_content_part import TermiteTextContentPart

        d = dict(src_dict)

        def _parse_content(
            data: object,
        ) -> list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str:
            try:
                if not isinstance(data, list):
                    raise TypeError()
                componentsschemas_termite_chat_message_content_type_1 = []
                _componentsschemas_termite_chat_message_content_type_1 = data
                for (
                    componentsschemas_termite_chat_message_content_type_1_item_data
                ) in _componentsschemas_termite_chat_message_content_type_1:

                    def _parse_componentsschemas_termite_chat_message_content_type_1_item(
                        data: object,
                    ) -> TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_termite_content_part_type_0 = TermiteTextContentPart.from_dict(data)

                            return componentsschemas_termite_content_part_type_0
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_termite_content_part_type_1 = TermiteImageURLContentPart.from_dict(data)

                            return componentsschemas_termite_content_part_type_1
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_termite_content_part_type_2 = TermiteMediaContentPart.from_dict(data)

                        return componentsschemas_termite_content_part_type_2

                    componentsschemas_termite_chat_message_content_type_1_item = (
                        _parse_componentsschemas_termite_chat_message_content_type_1_item(
                            componentsschemas_termite_chat_message_content_type_1_item_data
                        )
                    )

                    componentsschemas_termite_chat_message_content_type_1.append(
                        componentsschemas_termite_chat_message_content_type_1_item
                    )

                return componentsschemas_termite_chat_message_content_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str, data)

        content = _parse_content(d.pop("content"))

        id = d.pop("id", UNSET)

        termite_rerank_multimodal_document = cls(
            content=content,
            id=id,
        )

        termite_rerank_multimodal_document.additional_properties = d
        return termite_rerank_multimodal_document

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
