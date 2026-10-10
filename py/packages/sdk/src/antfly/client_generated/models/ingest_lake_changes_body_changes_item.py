from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.ingest_lake_changes_body_changes_item_op import IngestLakeChangesBodyChangesItemOp

if TYPE_CHECKING:
    from ..models.ingest_lake_changes_body_changes_item_row import IngestLakeChangesBodyChangesItemRow


T = TypeVar("T", bound="IngestLakeChangesBodyChangesItem")


@_attrs_define
class IngestLakeChangesBodyChangesItem:
    """
    Attributes:
        op (IngestLakeChangesBodyChangesItemOp):
        row (IngestLakeChangesBodyChangesItemRow):
    """

    op: IngestLakeChangesBodyChangesItemOp
    row: IngestLakeChangesBodyChangesItemRow
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        op = self.op.value

        row = self.row.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "op": op,
                "row": row,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ingest_lake_changes_body_changes_item_row import IngestLakeChangesBodyChangesItemRow

        d = dict(src_dict)
        op = IngestLakeChangesBodyChangesItemOp(d.pop("op"))

        row = IngestLakeChangesBodyChangesItemRow.from_dict(d.pop("row"))

        ingest_lake_changes_body_changes_item = cls(
            op=op,
            row=row,
        )

        ingest_lake_changes_body_changes_item.additional_properties = d
        return ingest_lake_changes_body_changes_item

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
