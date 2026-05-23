from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TermiteSparseVector")


@_attrs_define
class TermiteSparseVector:
    """A sparse vector with parallel index/value arrays, sorted by index ascending

    Attributes:
        indices (list[int]): Token IDs from the model vocabulary (sorted ascending)
        values (list[float]): Corresponding weights for each index (always positive)
    """

    indices: list[int]
    values: list[float]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        indices = self.indices

        values = self.values

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "indices": indices,
                "values": values,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        indices = cast(list[int], d.pop("indices"))

        values = cast(list[float], d.pop("values"))

        termite_sparse_vector = cls(
            indices=indices,
            values=values,
        )

        termite_sparse_vector.additional_properties = d
        return termite_sparse_vector

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
