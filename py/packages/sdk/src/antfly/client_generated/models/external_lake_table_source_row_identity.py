from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="ExternalLakeTableSourceRowIdentity")


@_attrs_define
class ExternalLakeTableSourceRowIdentity:
    """Optional verified stable identity for external rewrites. Keys must be non-null and unique in each published
    snapshot. Uses native typed equality; an unchanged key alone never authorizes index reuse.

        Attributes:
            key_columns (list[str]):
    """

    key_columns: list[str]

    def to_dict(self) -> dict[str, Any]:
        key_columns = self.key_columns

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "key_columns": key_columns,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key_columns = cast(list[str], d.pop("key_columns"))

        external_lake_table_source_row_identity = cls(
            key_columns=key_columns,
        )

        return external_lake_table_source_row_identity
