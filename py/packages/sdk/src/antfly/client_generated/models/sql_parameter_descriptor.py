from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_array_element_type import SQLArrayElementType
from ..models.sql_column_type import SQLColumnType
from ..models.sql_temporal_kind import SQLTemporalKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_named_type_identity import SQLNamedTypeIdentity


T = TypeVar("T", bound="SQLParameterDescriptor")


@_attrs_define
class SQLParameterDescriptor:
    """Immutable positional input contract. Element identity also preserves primitive widths; array inputs require it.
    Unknown slots have no SQL constraint. Bytea arguments are PostgreSQL bytea input strings; use hex text (a backslash
    followed by x and hexadecimal octets) for arbitrary binary data. SQL NULL remains a null cell.

        Attributes:
            type_ (SQLColumnType): Logical SQL result type. Integer values and numbers with element_type numeric are decimal
                strings to preserve exact precision in every client. Bytea results use PostgreSQL hex text (a backslash followed
                by x and lowercase hex digits) in JSON and opaque bytes in PostgreSQL binary format.
            nullable (bool):
            named_type (SQLNamedTypeIdentity | Unset): Server-resolved immutable SQL type identity. Origin survives restore
                remapping of local IDs and wire OIDs.
            temporal_kind (SQLTemporalKind | Unset): Exact temporal identity for datetime-family SQL values. Omission means
                timestamp_tz. Intervals preserve separate calendar months, days, and microseconds and use PostgreSQL interval
                text in JSON results.
            element_type (SQLArrayElementType | Unset): Bound SQL scalar or array-element identity, including numeric widths
                and exact NUMERIC. Never inferred from JSON value shape.
    """

    type_: SQLColumnType
    nullable: bool
    named_type: SQLNamedTypeIdentity | Unset = UNSET
    temporal_kind: SQLTemporalKind | Unset = UNSET
    element_type: SQLArrayElementType | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        nullable = self.nullable

        named_type: dict[str, Any] | Unset = UNSET
        if not isinstance(self.named_type, Unset):
            named_type = self.named_type.to_dict()

        temporal_kind: str | Unset = UNSET
        if not isinstance(self.temporal_kind, Unset):
            temporal_kind = self.temporal_kind.value

        element_type: str | Unset = UNSET
        if not isinstance(self.element_type, Unset):
            element_type = self.element_type.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "nullable": nullable,
            }
        )
        if named_type is not UNSET:
            field_dict["named_type"] = named_type
        if temporal_kind is not UNSET:
            field_dict["temporal_kind"] = temporal_kind
        if element_type is not UNSET:
            field_dict["element_type"] = element_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_named_type_identity import SQLNamedTypeIdentity

        d = dict(src_dict)
        type_ = SQLColumnType(d.pop("type"))

        nullable = d.pop("nullable")

        _named_type = d.pop("named_type", UNSET)
        named_type: SQLNamedTypeIdentity | Unset
        if isinstance(_named_type, Unset):
            named_type = UNSET
        else:
            named_type = SQLNamedTypeIdentity.from_dict(_named_type)

        _temporal_kind = d.pop("temporal_kind", UNSET)
        temporal_kind: SQLTemporalKind | Unset
        if isinstance(_temporal_kind, Unset):
            temporal_kind = UNSET
        else:
            temporal_kind = SQLTemporalKind(_temporal_kind)

        _element_type = d.pop("element_type", UNSET)
        element_type: SQLArrayElementType | Unset
        if isinstance(_element_type, Unset):
            element_type = UNSET
        else:
            element_type = SQLArrayElementType(_element_type)

        sql_parameter_descriptor = cls(
            type_=type_,
            nullable=nullable,
            named_type=named_type,
            temporal_kind=temporal_kind,
            element_type=element_type,
        )

        return sql_parameter_descriptor
