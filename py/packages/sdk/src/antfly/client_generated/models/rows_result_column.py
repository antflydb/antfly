from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsResultColumn")


@_attrs_define
class RowsResultColumn:
    """Typed column metadata emitted by a native relational read plan.

    Attributes:
        name (str): Unique public result-object field name.
        path (str): Source path represented by this result field.
        type_ (str): Antfly scalar/container type for the result field.
        nullable (bool): Whether the result field may be null.
        display_name (None | str | Unset): Optional SQL/display label for the result column. This value may be non-
            unique; use `name` as the stable object key.
        array_item_type (None | str | Unset): Item type when `type` is `array`.
        collation (None | str | Unset): Optional source collation metadata for source-backed text or keyword result
            fields.
    """

    name: str
    path: str
    type_: str
    nullable: bool
    display_name: None | str | Unset = UNSET
    array_item_type: None | str | Unset = UNSET
    collation: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        path = self.path

        type_ = self.type_

        nullable = self.nullable

        display_name: None | str | Unset
        if isinstance(self.display_name, Unset):
            display_name = UNSET
        else:
            display_name = self.display_name

        array_item_type: None | str | Unset
        if isinstance(self.array_item_type, Unset):
            array_item_type = UNSET
        else:
            array_item_type = self.array_item_type

        collation: None | str | Unset
        if isinstance(self.collation, Unset):
            collation = UNSET
        else:
            collation = self.collation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "path": path,
                "type": type_,
                "nullable": nullable,
            }
        )
        if display_name is not UNSET:
            field_dict["display_name"] = display_name
        if array_item_type is not UNSET:
            field_dict["array_item_type"] = array_item_type
        if collation is not UNSET:
            field_dict["collation"] = collation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        path = d.pop("path")

        type_ = d.pop("type")

        nullable = d.pop("nullable")

        def _parse_display_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        display_name = _parse_display_name(d.pop("display_name", UNSET))

        def _parse_array_item_type(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        array_item_type = _parse_array_item_type(d.pop("array_item_type", UNSET))

        def _parse_collation(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        collation = _parse_collation(d.pop("collation", UNSET))

        rows_result_column = cls(
            name=name,
            path=path,
            type_=type_,
            nullable=nullable,
            display_name=display_name,
            array_item_type=array_item_type,
            collation=collation,
        )

        return rows_result_column
