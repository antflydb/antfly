from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphBindingsReturn")


@_attrs_define
class GraphBindingsReturn:
    """
    Attributes:
        bindings (list[str]):
        limit (int | Unset):  Default: 100.
        include_documents (bool | Unset): Hydrate documents for projected non-null bindings. Default: False.
        fields (list[str] | Unset): Document fields to hydrate. Requires include_documents=true; omit to include all
            fields.
    """

    bindings: list[str]
    limit: int | Unset = 100
    include_documents: bool | Unset = False
    fields: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        bindings = self.bindings

        limit = self.limit

        include_documents = self.include_documents

        fields: list[str] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "bindings": bindings,
            }
        )
        if limit is not UNSET:
            field_dict["limit"] = limit
        if include_documents is not UNSET:
            field_dict["include_documents"] = include_documents
        if fields is not UNSET:
            field_dict["fields"] = fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        bindings = cast(list[str], d.pop("bindings"))

        limit = d.pop("limit", UNSET)

        include_documents = d.pop("include_documents", UNSET)

        fields = cast(list[str], d.pop("fields", UNSET))

        graph_bindings_return = cls(
            bindings=bindings,
            limit=limit,
            include_documents=include_documents,
            fields=fields,
        )

        return graph_bindings_return
