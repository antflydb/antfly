from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.relational_row_row import RelationalRowRow


T = TypeVar("T", bound="RelationalRow")


@_attrs_define
class RelationalRow:
    """
    Attributes:
        field_id (str):
        row (RelationalRowRow):
        version (str): Exact row version for mutation preconditions, encoded as decimal text.
        schema_version (int): Active pinned schema epoch, not the historical physical row layout.
    """

    field_id: str
    row: RelationalRowRow
    version: str
    schema_version: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_id = self.field_id

        row = self.row.to_dict()

        version = self.version

        schema_version = self.schema_version

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "_id": field_id,
                "row": row,
                "version": version,
                "schema_version": schema_version,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_row_row import RelationalRowRow

        d = dict(src_dict)
        field_id = d.pop("_id")

        row = RelationalRowRow.from_dict(d.pop("row"))

        version = d.pop("version")

        schema_version = d.pop("schema_version")

        relational_row = cls(
            field_id=field_id,
            row=row,
            version=version,
            schema_version=schema_version,
        )

        relational_row.additional_properties = d
        return relational_row

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
