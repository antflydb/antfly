from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="HierarchyProjection")


@_attrs_define
class HierarchyProjection:
    """
    Attributes:
        fields (list[str]): Fields to include from the hydrated hierarchy document. This projection is
            required whenever the ancestor is requested so hierarchy hydration cannot
            accidentally return an unbounded document. Use an empty array to return
            hierarchy identity without stored document fields.
    """

    fields: list[str]

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fields": fields,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields"))

        hierarchy_projection = cls(
            fields=fields,
        )

        return hierarchy_projection
