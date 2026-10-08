from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sql_array_column_schema_type import SQLArrayColumnSchemaType
from ..models.sql_array_element_type import SQLArrayElementType
from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLArrayColumnSchema")


@_attrs_define
class SQLArrayColumnSchema:
    """JSON Schema property declaration for one typed SQL-array column in a
    relational table. Use it as a root property of DocumentSchema.schema.
    The element identity is mandatory; a JSON Schema `array` remains a JSON
    column and is never inferred to be a SQL array. Column values use the
    lossless SQLArrayValue envelope: dimensions with lower bounds, flat
    row-major values and explicit SQL NULL flags. Integer elements are
    decimal strings, even when small. JSONB null and SQL NULL are distinct.
    Float elements acquire their declared width before validation and
    storage. Outer null represents a SQL NULL array when nullable is true.
    Additional JSON Schema constraints apply to this envelope, not to
    PostgreSQL array subscripts. SQL array index keys and SQL DDL activation
    are not implied by accepting this storage schema.

        Attributes:
            type_ (SQLArrayColumnSchemaType):
            x_antfly_sql_type (SQLArrayElementType): Bound SQL scalar or array-element identity, including numeric widths
                and exact NUMERIC. Never inferred from JSON value shape.
            nullable (bool | Unset):  Default: False.
            description (str | Unset):
    """

    type_: SQLArrayColumnSchemaType
    x_antfly_sql_type: SQLArrayElementType
    nullable: bool | Unset = False
    description: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        x_antfly_sql_type = self.x_antfly_sql_type.value

        nullable = self.nullable

        description = self.description

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "x-antfly-sql-type": x_antfly_sql_type,
            }
        )
        if nullable is not UNSET:
            field_dict["nullable"] = nullable
        if description is not UNSET:
            field_dict["description"] = description

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = SQLArrayColumnSchemaType(d.pop("type"))

        x_antfly_sql_type = SQLArrayElementType(d.pop("x-antfly-sql-type"))

        nullable = d.pop("nullable", UNSET)

        description = d.pop("description", UNSET)

        sql_array_column_schema = cls(
            type_=type_,
            x_antfly_sql_type=x_antfly_sql_type,
            nullable=nullable,
            description=description,
        )

        sql_array_column_schema.additional_properties = d
        return sql_array_column_schema

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
