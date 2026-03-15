from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cluster_backup_response_status import ClusterBackupResponseStatus

if TYPE_CHECKING:
    from ..models.table_backup_status import TableBackupStatus


T = TypeVar("T", bound="ClusterBackupResponse")


@_attrs_define
class ClusterBackupResponse:
    """
    Attributes:
        backup_id (str): The backup identifier Example: cluster-backup-2025-01-15.
        tables (list['TableBackupStatus']): Status of each table backup
        status (ClusterBackupResponseStatus): Overall backup status Example: completed.
    """

    backup_id: str
    tables: list["TableBackupStatus"]
    status: ClusterBackupResponseStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        tables = []
        for tables_item_data in self.tables:
            tables_item = tables_item_data.to_dict()
            tables.append(tables_item)

        status = self.status.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "backup_id": backup_id,
                "tables": tables,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.table_backup_status import TableBackupStatus

        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        tables = []
        _tables = d.pop("tables")
        for tables_item_data in _tables:
            tables_item = TableBackupStatus.from_dict(tables_item_data)

            tables.append(tables_item)

        status = ClusterBackupResponseStatus(d.pop("status"))

        cluster_backup_response = cls(
            backup_id=backup_id,
            tables=tables,
            status=status,
        )

        cluster_backup_response.additional_properties = d
        return cluster_backup_response

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
