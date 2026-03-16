from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.table_migration_state import TableMigrationState

if TYPE_CHECKING:
    from ..models.table_schema import TableSchema


T = TypeVar("T", bound="TableMigration")


@_attrs_define
class TableMigration:
    """Describes an in-progress schema migration. The table serves reads from read_schema while rebuilding full-text
    indexes for the new schema.

        Attributes:
            state (TableMigrationState):
            read_schema (TableSchema): Schema definition for a table with multiple document types
    """

    state: TableMigrationState
    read_schema: "TableSchema"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        state = self.state.value

        read_schema = self.read_schema.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "state": state,
                "read_schema": read_schema,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.table_schema import TableSchema

        d = dict(src_dict)
        state = TableMigrationState(d.pop("state"))

        read_schema = TableSchema.from_dict(d.pop("read_schema"))

        table_migration = cls(
            state=state,
            read_schema=read_schema,
        )

        table_migration.additional_properties = d
        return table_migration

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
