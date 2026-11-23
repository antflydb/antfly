from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.document_sql_column_type import DocumentSQLColumnType

T = TypeVar("T", bound="DocumentSQLColumn")


@_attrs_define
class DocumentSQLColumn:
    """
    Attributes:
        name (str):
        path (list[str]): Literal object keys, without wildcard or array traversal.
        type_ (DocumentSQLColumnType): Existing strict SQL document scalar domain. Numeric/object values are never
            stringified for keyword or text columns.
    """

    name: str
    path: list[str]
    type_: DocumentSQLColumnType

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        path = self.path

        type_ = self.type_.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "path": path,
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        path = cast(list[str], d.pop("path"))

        type_ = DocumentSQLColumnType(d.pop("type"))

        document_sql_column = cls(
            name=name,
            path=path,
            type_=type_,
        )

        return document_sql_column
