from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="HierarchyProjection")


@_attrs_define
class HierarchyProjection:
    """
    Attributes:
        fields (list[str] | Unset): Fields to include from the hydrated hierarchy document. Omit to include all fields.
    """

    fields: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if fields is not UNSET:
            field_dict["fields"] = fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields", UNSET))

        hierarchy_projection = cls(
            fields=fields,
        )

        return hierarchy_projection
