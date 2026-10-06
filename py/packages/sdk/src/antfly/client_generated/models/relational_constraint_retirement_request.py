from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.table_schema import TableSchema


T = TypeVar("T", bound="RelationalConstraintRetirementRequest")


@_attrs_define
class RelationalConstraintRetirementRequest:
    """Supply exactly one of target_schema or drop=true. A target schema may
    only remove UNIQUE/FK definitions; all other schema properties must
    remain unchanged. Its version is assigned by the server. Retirement
    fences primary mutations while existing reference and claim records
    are drained. External foreign keys referencing removed definitions
    must be retired first.

        Attributes:
            schema_version (int):
            target_schema (TableSchema | Unset): Schema definition for a table with multiple document types
            drop (bool | Unset): Prepare for explicit table deletion; this operation does not delete the table. Default:
                False.
    """

    schema_version: int
    target_schema: TableSchema | Unset = UNSET
    drop: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        schema_version = self.schema_version

        target_schema: dict[str, Any] | Unset = UNSET
        if not isinstance(self.target_schema, Unset):
            target_schema = self.target_schema.to_dict()

        drop = self.drop

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "schema_version": schema_version,
            }
        )
        if target_schema is not UNSET:
            field_dict["target_schema"] = target_schema
        if drop is not UNSET:
            field_dict["drop"] = drop

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.table_schema import TableSchema

        d = dict(src_dict)
        schema_version = d.pop("schema_version")

        _target_schema = d.pop("target_schema", UNSET)
        target_schema: TableSchema | Unset
        if isinstance(_target_schema, Unset):
            target_schema = UNSET
        else:
            target_schema = TableSchema.from_dict(_target_schema)

        drop = d.pop("drop", UNSET)

        relational_constraint_retirement_request = cls(
            schema_version=schema_version,
            target_schema=target_schema,
            drop=drop,
        )

        return relational_constraint_retirement_request
