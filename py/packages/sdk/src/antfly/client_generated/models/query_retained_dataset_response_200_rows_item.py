from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="QueryRetainedDatasetResponse200RowsItem")


@_attrs_define
class QueryRetainedDatasetResponse200RowsItem:
    """
    Attributes:
        bucket_start_ns (str):
        dimensions (list[Any]):
        values (list[Any]):
    """

    bucket_start_ns: str
    dimensions: list[Any]
    values: list[Any]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        bucket_start_ns = self.bucket_start_ns

        dimensions = self.dimensions

        values = self.values

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "bucket_start_ns": bucket_start_ns,
                "dimensions": dimensions,
                "values": values,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        bucket_start_ns = d.pop("bucket_start_ns")

        dimensions = cast(list[Any], d.pop("dimensions"))

        values = cast(list[Any], d.pop("values"))

        query_retained_dataset_response_200_rows_item = cls(
            bucket_start_ns=bucket_start_ns,
            dimensions=dimensions,
            values=values,
        )

        query_retained_dataset_response_200_rows_item.additional_properties = d
        return query_retained_dataset_response_200_rows_item

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
