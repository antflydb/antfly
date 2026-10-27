from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sql_range_column_schema_type import SQLRangeColumnSchemaType
from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLRangeColumnSchema")


@_attrs_define
class SQLRangeColumnSchema:
    """A relational PostgreSQL range column. JSON ingress and SQL results use
    PostgreSQL range text. The declared subtype supplies identity even for
    NULL, empty and unbounded values. Packed rows retain canonical binary
    endpoints, inclusive/exclusive flags and exact NUMERIC precision.
    Empty ranges, omitted bounds, subtype infinities and SQL NULL are
    distinct. DATE ranges canonicalize finite endpoints to [lower,upper).
    Timestamp endpoints use the execution's UTC zone. Requires schema
    reader capability version 35.

        Attributes:
            type_ (SQLRangeColumnSchemaType):
            nullable (bool | Unset):  Default: False.
            description (str | Unset):
    """

    type_: SQLRangeColumnSchemaType
    nullable: bool | Unset = False
    description: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        nullable = self.nullable

        description = self.description

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
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
        type_ = SQLRangeColumnSchemaType(d.pop("type"))

        nullable = d.pop("nullable", UNSET)

        description = d.pop("description", UNSET)

        sql_range_column_schema = cls(
            type_=type_,
            nullable=nullable,
            description=description,
        )

        sql_range_column_schema.additional_properties = d
        return sql_range_column_schema

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
