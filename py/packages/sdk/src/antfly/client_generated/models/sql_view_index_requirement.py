from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLViewIndexRequirement")


@_attrs_define
class SQLViewIndexRequirement:
    """
    Attributes:
        view_name (str): Unqualified canonical view name in this document source's namespace. Does not create a view or
            grant permissions.
        required_indexes (list[str]): Required full-text indexes. Canonical view admission captures their actual server-
            managed incarnations. Dropping and recreating an index or changing its logical configuration invalidates the
            captured dependency until the view is explicitly replaced. Maintenance and rebuilds of the same logical index
            acquire a fresh ready publication for each retained read.
    """

    view_name: str
    required_indexes: list[str]

    def to_dict(self) -> dict[str, Any]:
        view_name = self.view_name

        required_indexes = self.required_indexes

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "view_name": view_name,
                "required_indexes": required_indexes,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        view_name = d.pop("view_name")

        required_indexes = cast(list[str], d.pop("required_indexes"))

        sql_view_index_requirement = cls(
            view_name=view_name,
            required_indexes=required_indexes,
        )

        return sql_view_index_requirement
