from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="BedrockEmbedderConfig")


@_attrs_define
class BedrockEmbedderConfig:
    """Configuration for the AWS Bedrock embedding provider.

    Uses AWS credentials from environment or IAM roles.

    **Example Models:** cohere.embed-english-v4, amazon.titan-embed-text-v2:0

    **Docs:** https://docs.aws.amazon.com/bedrock/latest/userguide/models-supported.html

        Example:
            {'provider': 'bedrock', 'model': 'cohere.embed-english-v4', 'region': 'us-east-1'}

        Attributes:
            model (str): The Bedrock model ID to use (e.g., 'cohere.embed-english-v4', 'amazon.titan-embed-text-v2:0').
                Example: cohere.embed-english-v4.
            region (Union[Unset, str]): The AWS region for the Bedrock service (e.g., 'us-east-1'). Example: us-east-1.
            strip_new_lines (Union[Unset, bool]): Whether to strip new lines from the input text before embedding. Default:
                False.
            batch_size (Union[Unset, int]): The batch size for embedding requests to optimize throughput. Default: 1.
    """

    model: str
    region: Union[Unset, str] = UNSET
    strip_new_lines: Union[Unset, bool] = False
    batch_size: Union[Unset, int] = 1
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        region = self.region

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

        strip_new_lines = d.pop("strip_new_lines", UNSET)

        batch_size = d.pop("batch_size", UNSET)

        bedrock_embedder_config = cls(
            model=model,
            region=region,
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
