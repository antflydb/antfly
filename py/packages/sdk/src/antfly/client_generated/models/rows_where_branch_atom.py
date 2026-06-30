from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_where_branch_atom_op import RowsWhereBranchAtomOp
from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsWhereBranchAtom")


@_attrs_define
class RowsWhereBranchAtom:
    """
    Attributes:
        field (str): Declared relational column for a single-atom branch.
        op (RowsWhereBranchAtomOp):
        value (Any | Unset): JSON comparison value or array operand for operators that require one.
        path (Any | Unset): Non-empty JSON path for `json_path_eq` and `json_path_exists`, encoded as a dot path string
            or array of path components.
        pattern (str | Unset): SQL LIKE pattern for `text_pattern`.
        case_insensitive (bool | Unset): ASCII case-insensitive matching for `text_pattern`. Default: False.
        negated (bool | Unset): Negates `text_pattern`. Default: False.
    """

    field: str
    op: RowsWhereBranchAtomOp
    value: Any | Unset = UNSET
    path: Any | Unset = UNSET
    pattern: str | Unset = UNSET
    case_insensitive: bool | Unset = False
    negated: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        op = self.op.value

        value = self.value

        path = self.path

        pattern = self.pattern

        case_insensitive = self.case_insensitive

        negated = self.negated

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
                "op": op,
            }
        )
        if value is not UNSET:
            field_dict["value"] = value
        if path is not UNSET:
            field_dict["path"] = path
        if pattern is not UNSET:
            field_dict["pattern"] = pattern
        if case_insensitive is not UNSET:
            field_dict["case_insensitive"] = case_insensitive
        if negated is not UNSET:
            field_dict["negated"] = negated

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        op = RowsWhereBranchAtomOp(d.pop("op"))

        value = d.pop("value", UNSET)

        path = d.pop("path", UNSET)

        pattern = d.pop("pattern", UNSET)

        case_insensitive = d.pop("case_insensitive", UNSET)

        negated = d.pop("negated", UNSET)

        rows_where_branch_atom = cls(
            field=field,
            op=op,
            value=value,
            path=path,
            pattern=pattern,
            case_insensitive=case_insensitive,
            negated=negated,
        )

        return rows_where_branch_atom
