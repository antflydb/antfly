from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="EmbeddingType3")


@_attrs_define
class EmbeddingType3:
    """Packed sparse embedding with base64-encoded indices and values.
    Same semantics as the sparse object format but more compact on the wire.

        Attributes:
            packed_indices (str): Base64-encoded little-endian uint32 bytes for sparse indices
            packed_values (str): Base64-encoded little-endian float32 bytes for sparse values
    """

    packed_indices: str
    packed_values: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        packed_indices = self.packed_indices

        packed_values = self.packed_values

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "packed_indices": packed_indices,
                "packed_values": packed_values,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        packed_indices = d.pop("packed_indices")

        packed_values = d.pop("packed_values")

        embedding_type_3 = cls(
            packed_indices=packed_indices,
            packed_values=packed_values,
        )

        embedding_type_3.additional_properties = d
        return embedding_type_3

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
