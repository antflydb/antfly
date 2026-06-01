from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TransactionStageReadSnapshot")


@_attrs_define
class TransactionStageReadSnapshot:
    """
    Attributes:
        table (str):
        key (str):
        version (str):
        document (Any):
    """

    table: str
    key: str
    version: str
    document: Any
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        key = self.key

        version = self.version

        document = self.document

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "key": key,
                "version": version,
                "document": document,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table")

        key = d.pop("key")

        version = d.pop("version")

        document = d.pop("document")

        transaction_stage_read_snapshot = cls(
            table=table,
            key=key,
            version=version,
            document=document,
        )

        transaction_stage_read_snapshot.additional_properties = d
        return transaction_stage_read_snapshot

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
