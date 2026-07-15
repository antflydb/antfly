from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RestoreJobResultFailureDetailsItem")


@_attrs_define
class RestoreJobResultFailureDetailsItem:
    """
    Attributes:
        table_name (str):
        error (str):
        table_name_truncated (bool):
    """

    table_name: str
    error: str
    table_name_truncated: bool

    def to_dict(self) -> dict[str, Any]:
        table_name = self.table_name

        error = self.error

        table_name_truncated = self.table_name_truncated

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table_name": table_name,
                "error": error,
                "table_name_truncated": table_name_truncated,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table_name = d.pop("table_name")

        error = d.pop("error")

        table_name_truncated = d.pop("table_name_truncated")

        restore_job_result_failure_details_item = cls(
            table_name=table_name,
            error=error,
            table_name_truncated=table_name_truncated,
        )

        return restore_job_result_failure_details_item
