from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.transaction_stage_write_request_document import TransactionStageWriteRequestDocument


T = TypeVar("T", bound="TransactionStageWriteRequest")


@_attrs_define
class TransactionStageWriteRequest:
    """
    Attributes:
        table (str):
        key (str):
        document (TransactionStageWriteRequestDocument):
    """

    table: str
    key: str
    document: TransactionStageWriteRequestDocument
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        key = self.key

        document = self.document.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "key": key,
                "document": document,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.transaction_stage_write_request_document import TransactionStageWriteRequestDocument

        d = dict(src_dict)
        table = d.pop("table")

        key = d.pop("key")

        document = TransactionStageWriteRequestDocument.from_dict(d.pop("document"))

        transaction_stage_write_request = cls(
            table=table,
            key=key,
            document=document,
        )

        transaction_stage_write_request.additional_properties = d
        return transaction_stage_write_request

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
