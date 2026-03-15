from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cluster_restore_response_status import ClusterRestoreResponseStatus

if TYPE_CHECKING:
    from ..models.table_restore_status import TableRestoreStatus


T = TypeVar("T", bound="ClusterRestoreResponse")


@_attrs_define
class ClusterRestoreResponse:
    """
    Attributes:
        tables (list['TableRestoreStatus']): Status of each table restore
        status (ClusterRestoreResponseStatus): Overall restore status Example: triggered.
    """

    tables: list["TableRestoreStatus"]
    status: ClusterRestoreResponseStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        tables = []
        for tables_item_data in self.tables:
            tables_item = tables_item_data.to_dict()
            tables.append(tables_item)

        status = self.status.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "tables": tables,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.table_restore_status import TableRestoreStatus

        d = dict(src_dict)
        tables = []
        _tables = d.pop("tables")
        for tables_item_data in _tables:
            tables_item = TableRestoreStatus.from_dict(tables_item_data)

            tables.append(tables_item)

        status = ClusterRestoreResponseStatus(d.pop("status"))

        cluster_restore_response = cls(
            tables=tables,
            status=status,
        )

        cluster_restore_response.additional_properties = d
        return cluster_restore_response

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
