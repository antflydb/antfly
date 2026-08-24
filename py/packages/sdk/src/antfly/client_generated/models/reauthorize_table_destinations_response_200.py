from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.reauthorize_table_destinations_response_200_status import ReauthorizeTableDestinationsResponse200Status

T = TypeVar("T", bound="ReauthorizeTableDestinationsResponse200")


@_attrs_define
class ReauthorizeTableDestinationsResponse200:
    """
    Attributes:
        table (str):
        status (ReauthorizeTableDestinationsResponse200Status):
    """

    table: str
    status: ReauthorizeTableDestinationsResponse200Status
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        status = self.status.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table")

        status = ReauthorizeTableDestinationsResponse200Status(d.pop("status"))

        reauthorize_table_destinations_response_200 = cls(
            table=table,
            status=status,
        )

        reauthorize_table_destinations_response_200.additional_properties = d
        return reauthorize_table_destinations_response_200

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
