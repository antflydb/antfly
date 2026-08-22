from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.table_catalog_changed_conflict_code import TableCatalogChangedConflictCode

T = TypeVar("T", bound="TableCatalogChangedConflict")


@_attrs_define
class TableCatalogChangedConflict:
    """
    Attributes:
        code (TableCatalogChangedConflictCode):
        error (str): Legacy human-readable error text. Use `code` for branching.
        message (str):
        retryable (bool):
    """

    code: TableCatalogChangedConflictCode
    error: str
    message: str
    retryable: bool

    def to_dict(self) -> dict[str, Any]:
        code = self.code.value

        error = self.error

        message = self.message

        retryable = self.retryable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "error": error,
                "message": message,
                "retryable": retryable,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = TableCatalogChangedConflictCode(d.pop("code"))

        error = d.pop("error")

        message = d.pop("message")

        retryable = d.pop("retryable")

        table_catalog_changed_conflict = cls(
            code=code,
            error=error,
            message=message,
            retryable=retryable,
        )

        return table_catalog_changed_conflict
