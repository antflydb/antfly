from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sql_named_enum_column_schema_type import SQLNamedEnumColumnSchemaType
from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLNamedEnumColumnSchema")


@_attrs_define
class SQLNamedEnumColumnSchema:
    """A schema-bound enum column. JSON values are exact labels; SQL NULL is distinct from every label, including the empty
    string.

        Attributes:
            type_ (SQLNamedEnumColumnSchemaType):
            x_antfly_sql_type_id (int): Resolved catalog type ID. The table's named_types directory supplies its immutable
                executable definition.
            nullable (bool | Unset):  Default: False.
    """

    type_: SQLNamedEnumColumnSchemaType
    x_antfly_sql_type_id: int
    nullable: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        x_antfly_sql_type_id = self.x_antfly_sql_type_id

        nullable = self.nullable

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "x-antfly-sql-type-id": x_antfly_sql_type_id,
            }
        )
        if nullable is not UNSET:
            field_dict["nullable"] = nullable

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = SQLNamedEnumColumnSchemaType(d.pop("type"))

        x_antfly_sql_type_id = d.pop("x-antfly-sql-type-id")

        nullable = d.pop("nullable", UNSET)

        sql_named_enum_column_schema = cls(
            type_=type_,
            x_antfly_sql_type_id=x_antfly_sql_type_id,
            nullable=nullable,
        )

        sql_named_enum_column_schema.additional_properties = d
        return sql_named_enum_column_schema

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
