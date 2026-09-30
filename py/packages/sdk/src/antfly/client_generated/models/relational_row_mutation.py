from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_row_mutation_row import RelationalRowMutationRow


T = TypeVar("T", bound="RelationalRowMutation")


@_attrs_define
class RelationalRowMutation:
    """A complete row replacement, or deletion when row is omitted. No read-modify-write is implied.

    Attributes:
        key (str):
        expected_version (str): Exact observed version. Zero requires that the row does not exist.
        row (RelationalRowMutationRow | Unset):
        json_null_fields (list[str] | Unset): Names of JSON-typed columns whose row value is the JSON literal null
            rather than SQL NULL. Each name must identify a present null-valued JSON column; duplicates, unknown names, non-
            null values, and use with deletion are rejected.
    """

    key: str
    expected_version: str
    row: RelationalRowMutationRow | Unset = UNSET
    json_null_fields: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        expected_version = self.expected_version

        row: dict[str, Any] | Unset = UNSET
        if not isinstance(self.row, Unset):
            row = self.row.to_dict()

        json_null_fields: list[str] | Unset = UNSET
        if not isinstance(self.json_null_fields, Unset):
            json_null_fields = self.json_null_fields

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "key": key,
                "expected_version": expected_version,
            }
        )
        if row is not UNSET:
            field_dict["row"] = row
        if json_null_fields is not UNSET:
            field_dict["json_null_fields"] = json_null_fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_row_mutation_row import RelationalRowMutationRow

        d = dict(src_dict)
        key = d.pop("key")

        expected_version = d.pop("expected_version")

        _row = d.pop("row", UNSET)
        row: RelationalRowMutationRow | Unset
        if isinstance(_row, Unset):
            row = UNSET
        else:
            row = RelationalRowMutationRow.from_dict(_row)

        json_null_fields = cast(list[str], d.pop("json_null_fields", UNSET))

        relational_row_mutation = cls(
            key=key,
            expected_version=expected_version,
            row=row,
            json_null_fields=json_null_fields,
        )

        return relational_row_mutation
