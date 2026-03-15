from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="FieldStatistics")


@_attrs_define
class FieldStatistics:
    """Statistics about a specific field.

    Attributes:
        cardinality (Union[Unset, int]): Approximate number of unique values (via HyperLogLog).
        null_count (Union[Unset, int]): Number of rows with null values for this field.
        min_value (Union[Unset, Any]): Minimum value for numeric/date fields.
        max_value (Union[Unset, Any]): Maximum value for numeric/date fields.
        avg_size (Union[Unset, int]): Average size in bytes for variable-length fields.
    """

    cardinality: Union[Unset, int] = UNSET
    null_count: Union[Unset, int] = UNSET
    min_value: Union[Unset, Any] = UNSET
    max_value: Union[Unset, Any] = UNSET
    avg_size: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        cardinality = self.cardinality

        null_count = self.null_count

        min_value = self.min_value

        max_value = self.max_value

        avg_size = self.avg_size

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if cardinality is not UNSET:
            field_dict["cardinality"] = cardinality
        if null_count is not UNSET:
            field_dict["null_count"] = null_count
        if min_value is not UNSET:
            field_dict["min_value"] = min_value
        if max_value is not UNSET:
            field_dict["max_value"] = max_value
        if avg_size is not UNSET:
            field_dict["avg_size"] = avg_size

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        cardinality = d.pop("cardinality", UNSET)

        null_count = d.pop("null_count", UNSET)

        min_value = d.pop("min_value", UNSET)

        max_value = d.pop("max_value", UNSET)

        avg_size = d.pop("avg_size", UNSET)

        field_statistics = cls(
            cardinality=cardinality,
            null_count=null_count,
            min_value=min_value,
            max_value=max_value,
            avg_size=avg_size,
        )

        field_statistics.additional_properties = d
        return field_statistics

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
