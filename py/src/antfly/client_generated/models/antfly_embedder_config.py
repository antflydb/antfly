from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="AntflyEmbedderConfig")


@_attrs_define
class AntflyEmbedderConfig:
    """Configuration for the built-in Antfly embedding provider.

    Uses an embedded INT8-quantized all-MiniLM-L6-v2 ONNX model bundled directly
    in the binary. No external service, API key, or model download required.

    **Model:** all-MiniLM-L6-v2 (384 dimensions, text-only)

    **Features:**
    - Zero configuration — works out of the box
    - No network access required
    - Pure Go inference via GoMLX

        Example:
            {'provider': 'antfly'}

    """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        antfly_embedder_config = cls()

        antfly_embedder_config.additional_properties = d
        return antfly_embedder_config

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
