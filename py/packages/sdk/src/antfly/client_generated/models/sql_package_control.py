from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_package_export import SQLPackageExport


T = TypeVar("T", bound="SQLPackageControl")


@_attrs_define
class SQLPackageControl:
    """Database-scoped SQL installation over the existing owned package registry. Requires embedded_db scope, SHA256-bound
    core WASM artifacts, no other install objects/shapes and no host capabilities. Familiar package names imply no
    upstream extension payload.

        Attributes:
            default_version (str): Initial package-level default intent used only when the first SQL version is registered.
                Later defaults are explicit digest-bound operator promotions; version names are opaque.
            exports (list[SQLPackageExport]):
            schema (None | str | Unset): Optional fixed installation namespace; requires relocatable false.
            updates_from (list[str] | Unset): Predecessor versions supported by this version's declarative typed-export
                transition, separate from REST update scripts.
    """

    default_version: str
    exports: list[SQLPackageExport]
    schema: None | str | Unset = UNSET
    updates_from: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        default_version = self.default_version

        exports = []
        for exports_item_data in self.exports:
            exports_item = exports_item_data.to_dict()
            exports.append(exports_item)

        schema: None | str | Unset
        if isinstance(self.schema, Unset):
            schema = UNSET
        else:
            schema = self.schema

        updates_from: list[str] | Unset = UNSET
        if not isinstance(self.updates_from, Unset):
            updates_from = self.updates_from

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "default_version": default_version,
                "exports": exports,
            }
        )
        if schema is not UNSET:
            field_dict["schema"] = schema
        if updates_from is not UNSET:
            field_dict["updates_from"] = updates_from

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_package_export import SQLPackageExport

        d = dict(src_dict)
        default_version = d.pop("default_version")

        exports = []
        _exports = d.pop("exports")
        for exports_item_data in _exports:
            exports_item = SQLPackageExport.from_dict(exports_item_data)

            exports.append(exports_item)

        def _parse_schema(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        schema = _parse_schema(d.pop("schema", UNSET))

        updates_from = cast(list[str], d.pop("updates_from", UNSET))

        sql_package_control = cls(
            default_version=default_version,
            exports=exports,
            schema=schema,
            updates_from=updates_from,
        )

        return sql_package_control
