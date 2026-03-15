from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.table_restore_status_status import TableRestoreStatusStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="TableRestoreStatus")


@_attrs_define
class TableRestoreStatus:
    """
    Attributes:
        name (str): Table name Example: users.
        status (TableRestoreStatusStatus): Restore status for this table Example: triggered.
        error (Union[Unset, str]): Error message if restore failed
    """

    name: str
    status: TableRestoreStatusStatus
    error: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        status = self.status.value

        error = self.error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "status": status,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        status = TableRestoreStatusStatus(d.pop("status"))

        error = d.pop("error", UNSET)

        table_restore_status = cls(
            name=name,
            status=status,
            error=error,
        )

        table_restore_status.additional_properties = d
        return table_restore_status

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
