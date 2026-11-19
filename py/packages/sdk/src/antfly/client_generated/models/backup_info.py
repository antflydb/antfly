from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.backup_info_format import BackupInfoFormat
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.backup_info_table_selectors_item import BackupInfoTableSelectorsItem


T = TypeVar("T", bound="BackupInfo")


@_attrs_define
class BackupInfo:
    """
    Attributes:
        backup_id (str): The backup identifier Example: cluster-backup-2025-01-15.
        timestamp (datetime.datetime): When the backup was created Example: 2025-01-15T10:30:00Z.
        tables (list[str]): Tables included in the backup Example: ['users', 'products'].
        location (str): Storage location of the backup Example: s3://mybucket/antfly-backups/cluster/2025-01-15.
        table_selectors (list[BackupInfoTableSelectorsItem] | Unset): Exact restore selectors paired with display names,
            in the same order as tables. Use selector in ClusterRestoreRequest.table_names to disambiguate identical display
            names.
        table_details_omitted (bool | Unset): True when the listing returned only backup metadata because table details
            exceeded the page size.
        antfly_version (str | Unset): Antfly version that created the backup Example: v1.0.0.
        format_ (BackupInfoFormat | Unset): Backup format used Example: portable.
    """

    backup_id: str
    timestamp: datetime.datetime
    tables: list[str]
    location: str
    table_selectors: list[BackupInfoTableSelectorsItem] | Unset = UNSET
    table_details_omitted: bool | Unset = UNSET
    antfly_version: str | Unset = UNSET
    format_: BackupInfoFormat | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        backup_id = self.backup_id

        timestamp = self.timestamp.isoformat()

        tables = self.tables

        location = self.location

        table_selectors: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.table_selectors, Unset):
            table_selectors = []
            for table_selectors_item_data in self.table_selectors:
                table_selectors_item = table_selectors_item_data.to_dict()
                table_selectors.append(table_selectors_item)

        table_details_omitted = self.table_details_omitted

        antfly_version = self.antfly_version

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "backup_id": backup_id,
                "timestamp": timestamp,
                "tables": tables,
                "location": location,
            }
        )
        if table_selectors is not UNSET:
            field_dict["table_selectors"] = table_selectors
        if table_details_omitted is not UNSET:
            field_dict["table_details_omitted"] = table_details_omitted
        if antfly_version is not UNSET:
            field_dict["antfly_version"] = antfly_version
        if format_ is not UNSET:
            field_dict["format"] = format_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.backup_info_table_selectors_item import BackupInfoTableSelectorsItem

        d = dict(src_dict)
        backup_id = d.pop("backup_id")

        timestamp = isoparse(d.pop("timestamp"))

        tables = cast(list[str], d.pop("tables"))

        location = d.pop("location")

        _table_selectors = d.pop("table_selectors", UNSET)
        table_selectors: list[BackupInfoTableSelectorsItem] | Unset = UNSET
        if _table_selectors is not UNSET:
            table_selectors = []
            for table_selectors_item_data in _table_selectors:
                table_selectors_item = BackupInfoTableSelectorsItem.from_dict(table_selectors_item_data)

                table_selectors.append(table_selectors_item)

        table_details_omitted = d.pop("table_details_omitted", UNSET)

        antfly_version = d.pop("antfly_version", UNSET)

        _format_ = d.pop("format", UNSET)
        format_: BackupInfoFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = BackupInfoFormat(_format_)

        backup_info = cls(
            backup_id=backup_id,
            timestamp=timestamp,
            tables=tables,
            location=location,
            table_selectors=table_selectors,
            table_details_omitted=table_details_omitted,
            antfly_version=antfly_version,
            format_=format_,
        )

        backup_info.additional_properties = d
        return backup_info

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
