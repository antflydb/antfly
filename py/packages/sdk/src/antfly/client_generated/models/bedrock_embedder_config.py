from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="BedrockEmbedderConfig")


@_attrs_define
class BedrockEmbedderConfig:
    """Configuration for the AWS Bedrock embedding provider.

    Uses the AWS credential chain: environment variables, web identity, shared credentials, ECS task roles, and EC2
    instance roles.

    **Example Models:** cohere.embed-v4, amazon.titan-embed-text-v2:0

    **Docs:** https://docs.aws.amazon.com/bedrock/latest/userguide/models-supported.html

        Example:
            {'provider': 'bedrock', 'model': 'cohere.embed-v4', 'region': 'us-east-1'}

        Attributes:
            model (str): The Bedrock model ID to use (e.g., 'cohere.embed-v4', 'amazon.titan-embed-text-v2:0'). Example:
                cohere.embed-v4.
            region (str | Unset): The AWS region for the Bedrock service (e.g., 'us-east-1'). Example: us-east-1.
            dimension (int | Unset): Output dimension for Bedrock embedding models that support configurable dimensions.
            dimensions (int | Unset): Alias for output dimension when using OpenAI-compatible configuration fields.
            input_type (str | Unset): Cohere Bedrock input type, such as search_document, search_query, classification, or
                clustering.
            truncate (str | Unset): Cohere Bedrock truncate behavior.
            strip_new_lines (bool | Unset): Whether to strip new lines from the input text before embedding. Default: False.
            batch_size (int | Unset): The batch size for embedding requests to optimize throughput. Default: 1.
    """

    model: str
    region: str | Unset = UNSET
    dimension: int | Unset = UNSET
    dimensions: int | Unset = UNSET
    input_type: str | Unset = UNSET
    truncate: str | Unset = UNSET
    strip_new_lines: bool | Unset = False
    batch_size: int | Unset = 1
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        region = self.region

        dimension = self.dimension

        dimensions = self.dimensions

        input_type = self.input_type

        truncate = self.truncate

        strip_new_lines = self.strip_new_lines

        batch_size = self.batch_size

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if region is not UNSET:
            field_dict["region"] = region
        if dimension is not UNSET:
            field_dict["dimension"] = dimension
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if input_type is not UNSET:
            field_dict["input_type"] = input_type
        if truncate is not UNSET:
            field_dict["truncate"] = truncate
        if strip_new_lines is not UNSET:
            field_dict["strip_new_lines"] = strip_new_lines
        if batch_size is not UNSET:
            field_dict["batch_size"] = batch_size

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        region = d.pop("region", UNSET)

        dimension = d.pop("dimension", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        input_type = d.pop("input_type", UNSET)

        truncate = d.pop("truncate", UNSET)

        strip_new_lines = d.pop("strip_new_lines", UNSET)

        batch_size = d.pop("batch_size", UNSET)

        bedrock_embedder_config = cls(
            model=model,
            region=region,
            dimension=dimension,
            dimensions=dimensions,
            input_type=input_type,
            truncate=truncate,
            strip_new_lines=strip_new_lines,
            batch_size=batch_size,
        )

        bedrock_embedder_config.additional_properties = d
        return bedrock_embedder_config

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
