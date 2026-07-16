from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphIndexSourceContext")


@_attrs_define
class GraphIndexSourceContext:
    """
    Attributes:
        doc_fields (list[str] | Unset): Document fields that source templates may reference through _doc.value.
    """

    doc_fields: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        doc_fields: list[str] | Unset = UNSET
        if not isinstance(self.doc_fields, Unset):
            doc_fields = self.doc_fields

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if doc_fields is not UNSET:
            field_dict["doc_fields"] = doc_fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        doc_fields = cast(list[str], d.pop("doc_fields", UNSET))

        graph_index_source_context = cls(
            doc_fields=doc_fields,
        )

        return graph_index_source_context
