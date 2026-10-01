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


T = TypeVar("T", bound="InferenceRerankRequest")


@_attrs_define
class InferenceRerankRequest:
    """
    Attributes:
        model (str): Name of reranking model from models_dir/rerankers/ Example: BAAI/bge-reranker-v2-m3.
        query (str): Search query for relevance scoring Example: machine learning applications.
        documents (list[list[ImageURLContentPart | MediaContentPart | TextContentPart] | str] | Unset): Documents to
            rerank. Each entry is a string or an array of text and image
            content parts. Exactly one of `documents` and `prompts` is required.
             Example: ['Introduction to machine learning...', [{'type': 'text', 'text': 'Quarterly invoice'}, {'type':
            'image_url', 'image_url': {'url': 'data:image/png;base64,iVBORw0KGgo...'}}]].
        prompts (list[str] | Unset): Deprecated text-only form of `documents`. Accepted so older clients keep
            working; send `documents` instead. Exactly one of `documents` and `prompts`
            is required.
    """

    model: str
    query: str
    documents: list[list[ImageURLContentPart | MediaContentPart | TextContentPart] | str] | Unset = UNSET
    prompts: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.text_content_part import TextContentPart

        model = self.model

        query = self.query

        documents: list[list[dict[str, Any]] | str] | Unset = UNSET
        if not isinstance(self.documents, Unset):
            documents = []
            for documents_item_data in self.documents:
                documents_item: list[dict[str, Any]] | str
                if isinstance(documents_item_data, list):
                    documents_item = []
                    for componentsschemas_chat_message_content_type_1_item_data in documents_item_data:
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

                        documents_item.append(componentsschemas_chat_message_content_type_1_item)

                else:
                    documents_item = documents_item_data
                documents.append(documents_item)

        prompts: list[str] | Unset = UNSET
        if not isinstance(self.prompts, Unset):
            prompts = self.prompts

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "query": query,
            }
        )
        if documents is not UNSET:
            field_dict["documents"] = documents
        if prompts is not UNSET:
            field_dict["prompts"] = prompts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        d = dict(src_dict)
        model = d.pop("model")

        query = d.pop("query")

        _documents = d.pop("documents", UNSET)
        documents: list[list[ImageURLContentPart | MediaContentPart | TextContentPart] | str] | Unset = UNSET
        if _documents is not UNSET:
            documents = []
            for documents_item_data in _documents:

                def _parse_documents_item(
                    data: object,
                ) -> list[ImageURLContentPart | MediaContentPart | TextContentPart] | str:
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

                documents_item = _parse_documents_item(documents_item_data)

                documents.append(documents_item)

        prompts = cast(list[str], d.pop("prompts", UNSET))

        inference_rerank_request = cls(
            model=model,
            query=query,
            documents=documents,
            prompts=prompts,
        )

        inference_rerank_request.additional_properties = d
        return inference_rerank_request

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
