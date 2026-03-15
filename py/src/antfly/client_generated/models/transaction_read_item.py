from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TransactionReadItem")


@_attrs_define
class TransactionReadItem:
    """A key that was read as part of an OCC transaction, along with the version
    observed at read time. Used to detect conflicts at commit time.

        Attributes:
            table (str): Table name the key belongs to
            key (str): Document key that was read
            version (str): Version token observed at read time (from X-Antfly-Version header).
                Use "0" to assert the key did not exist at read time.
    """

    table: str
    key: str
    version: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        key = self.key

        version = self.version

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "key": key,
                "version": version,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table")

        key = d.pop("key")

        version = d.pop("version")

        transaction_read_item = cls(
            table=table,
            key=key,
            version=version,
        )

        transaction_read_item.additional_properties = d
        return transaction_read_item

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
