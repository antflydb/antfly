from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cohere_embedder_config_input_type import CohereEmbedderConfigInputType
from ..models.cohere_embedder_config_truncate import CohereEmbedderConfigTruncate
from ..types import UNSET, Unset

T = TypeVar("T", bound="CohereEmbedderConfig")


@_attrs_define
class CohereEmbedderConfig:
    """Configuration for the Cohere embedding provider.

    API key via `api_key` field or `COHERE_API_KEY` environment variable.

    **Example Models:** embed-english-v3.0 (default, 1024 dims), embed-multilingual-v3.0

    **Docs:** https://docs.cohere.com/reference/embed

        Example:
            {'provider': 'cohere', 'model': 'embed-english-v3.0', 'input_type': 'search_document'}

        Attributes:
            model (str): The name of the Cohere embedding model to use. Default: 'embed-english-v3.0'. Example: embed-
                english-v3.0.
            api_key (Union[Unset, str]): The Cohere API key. Can also be set via COHERE_API_KEY environment variable.
            input_type (Union[Unset, CohereEmbedderConfigInputType]): Specifies the type of input for optimized embeddings.
                Default: CohereEmbedderConfigInputType.SEARCH_DOCUMENT.
            truncate (Union[Unset, CohereEmbedderConfigTruncate]): How to handle inputs longer than the max token length.
                Default: CohereEmbedderConfigTruncate.END.
    """

    model: str = "embed-english-v3.0"
    api_key: Union[Unset, str] = UNSET
    input_type: Union[Unset, CohereEmbedderConfigInputType] = CohereEmbedderConfigInputType.SEARCH_DOCUMENT
    truncate: Union[Unset, CohereEmbedderConfigTruncate] = CohereEmbedderConfigTruncate.END
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_key = self.api_key

        input_type: Union[Unset, str] = UNSET
        if not isinstance(self.input_type, Unset):
            input_type = self.input_type.value

        truncate: Union[Unset, str] = UNSET
        if not isinstance(self.truncate, Unset):
            truncate = self.truncate.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if input_type is not UNSET:
            field_dict["input_type"] = input_type
        if truncate is not UNSET:
            field_dict["truncate"] = truncate

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        api_key = d.pop("api_key", UNSET)

        _input_type = d.pop("input_type", UNSET)
        input_type: Union[Unset, CohereEmbedderConfigInputType]
        if isinstance(_input_type, Unset):
            input_type = UNSET
        else:
            input_type = CohereEmbedderConfigInputType(_input_type)

        _truncate = d.pop("truncate", UNSET)
        truncate: Union[Unset, CohereEmbedderConfigTruncate]
        if isinstance(_truncate, Unset):
            truncate = UNSET
        else:
            truncate = CohereEmbedderConfigTruncate(_truncate)

        cohere_embedder_config = cls(
            model=model,
            api_key=api_key,
            input_type=input_type,
            truncate=truncate,
        )

        cohere_embedder_config.additional_properties = d
        return cohere_embedder_config

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
