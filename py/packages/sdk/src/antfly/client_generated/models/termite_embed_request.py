from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_embed_request_encoding_format import TermiteEmbedRequestEncodingFormat
from ..models.termite_embed_request_input_type import TermiteEmbedRequestInputType
from ..models.termite_embed_request_task_type import TermiteEmbedRequestTaskType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_image_url_content_part import TermiteImageURLContentPart
    from ..models.termite_media_content_part import TermiteMediaContentPart
    from ..models.termite_text_content_part import TermiteTextContentPart


T = TypeVar("T", bound="TermiteEmbedRequest")


@_attrs_define
class TermiteEmbedRequest:
    """OpenAI-compatible embedding request with Termite multimodal content-part extension

    Attributes:
        model (str): Model name to use for embedding generation
        input_ (list[str] | list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str):
            Input content to embed.
            Supports:
            - a single string
            - an array of strings
            - an array of OpenAI-style content parts for multimodal embedding
        encoding_format (TermiteEmbedRequestEncodingFormat | Unset): Encoding format for the embeddings (only "float"
            supported) Default: TermiteEmbedRequestEncodingFormat.FLOAT.
        dimensions (int | Unset): Optional truncation size for dense embeddings. Must be a positive integer no larger
            than the model embedding size. Not supported for sparse models.
        task_type (TermiteEmbedRequestTaskType | Unset): Optional embedding task type using Google embedding task-type
            names. For Jina v5 text embeddings, query-side tasks use the query prefix and RETRIEVAL_DOCUMENT uses the
            document prefix.
        input_type (TermiteEmbedRequestInputType | Unset): Deprecated compatibility alias for task_type.
            search_query/query map to RETRIEVAL_QUERY; search_document/document map to RETRIEVAL_DOCUMENT; classification
            and clustering map to their Google task_type equivalents.
    """

    model: str
    input_: list[str] | list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str
    encoding_format: TermiteEmbedRequestEncodingFormat | Unset = TermiteEmbedRequestEncodingFormat.FLOAT
    dimensions: int | Unset = UNSET
    task_type: TermiteEmbedRequestTaskType | Unset = UNSET
    input_type: TermiteEmbedRequestInputType | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.termite_image_url_content_part import TermiteImageURLContentPart
        from ..models.termite_text_content_part import TermiteTextContentPart

        model = self.model

        input_: list[dict[str, Any]] | list[str] | str
        if isinstance(self.input_, list):
            input_ = self.input_

        elif isinstance(self.input_, list):
            input_ = []
            for input_type_2_item_data in self.input_:
                input_type_2_item: dict[str, Any]
                if isinstance(input_type_2_item_data, TermiteTextContentPart):
                    input_type_2_item = input_type_2_item_data.to_dict()
                elif isinstance(input_type_2_item_data, TermiteImageURLContentPart):
                    input_type_2_item = input_type_2_item_data.to_dict()
                else:
                    input_type_2_item = input_type_2_item_data.to_dict()

                input_.append(input_type_2_item)

        else:
            input_ = self.input_

        encoding_format: str | Unset = UNSET
        if not isinstance(self.encoding_format, Unset):
            encoding_format = self.encoding_format.value

        dimensions = self.dimensions

        task_type: str | Unset = UNSET
        if not isinstance(self.task_type, Unset):
            task_type = self.task_type.value

        input_type: str | Unset = UNSET
        if not isinstance(self.input_type, Unset):
            input_type = self.input_type.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "input": input_,
            }
        )
        if encoding_format is not UNSET:
            field_dict["encoding_format"] = encoding_format
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if task_type is not UNSET:
            field_dict["task_type"] = task_type
        if input_type is not UNSET:
            field_dict["input_type"] = input_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_image_url_content_part import TermiteImageURLContentPart
        from ..models.termite_media_content_part import TermiteMediaContentPart
        from ..models.termite_text_content_part import TermiteTextContentPart

        d = dict(src_dict)
        model = d.pop("model")

        def _parse_input_(
            data: object,
        ) -> list[str] | list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str:
            try:
                if not isinstance(data, list):
                    raise TypeError()
                input_type_1 = cast(list[str], data)

                return input_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, list):
                    raise TypeError()
                input_type_2 = []
                _input_type_2 = data
                for input_type_2_item_data in _input_type_2:

                    def _parse_input_type_2_item(
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

                    input_type_2_item = _parse_input_type_2_item(input_type_2_item_data)

                    input_type_2.append(input_type_2_item)

                return input_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(
                list[str] | list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str,
                data,
            )

        input_ = _parse_input_(d.pop("input"))

        _encoding_format = d.pop("encoding_format", UNSET)
        encoding_format: TermiteEmbedRequestEncodingFormat | Unset
        if isinstance(_encoding_format, Unset):
            encoding_format = UNSET
        else:
            encoding_format = TermiteEmbedRequestEncodingFormat(_encoding_format)

        dimensions = d.pop("dimensions", UNSET)

        _task_type = d.pop("task_type", UNSET)
        task_type: TermiteEmbedRequestTaskType | Unset
        if isinstance(_task_type, Unset):
            task_type = UNSET
        else:
            task_type = TermiteEmbedRequestTaskType(_task_type)

        _input_type = d.pop("input_type", UNSET)
        input_type: TermiteEmbedRequestInputType | Unset
        if isinstance(_input_type, Unset):
            input_type = UNSET
        else:
            input_type = TermiteEmbedRequestInputType(_input_type)

        termite_embed_request = cls(
            model=model,
            input_=input_,
            encoding_format=encoding_format,
            dimensions=dimensions,
            task_type=task_type,
            input_type=input_type,
        )

        termite_embed_request.additional_properties = d
        return termite_embed_request

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
