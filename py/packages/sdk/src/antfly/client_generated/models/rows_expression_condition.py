from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.rows_expression_condition_op import RowsExpressionConditionOp
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression import RowsExpression


T = TypeVar("T", bound="RowsExpressionCondition")


@_attrs_define
class RowsExpressionCondition:
    """Computed expression predicate over the shared row-expression AST.

    Attributes:
        lhs (RowsExpression): Shared typed row-expression AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported operators
            include `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, numeric
            `add`/`sub`/`mul`/`div`, `cast`, `json_extract`, `array_length`,
            `string_to_array`, and searched `case` with `cases` and `else`.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
        op (RowsExpressionConditionOp):
        rhs (RowsExpression | Unset): Shared typed row-expression AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported operators
            include `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, numeric
            `add`/`sub`/`mul`/`div`, `cast`, `json_extract`, `array_length`,
            `string_to_array`, and searched `case` with `cases` and `else`.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
    """

    lhs: RowsExpression
    op: RowsExpressionConditionOp
    rhs: RowsExpression | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        lhs = self.lhs.to_dict()

        op = self.op.value

        rhs: dict[str, Any] | Unset = UNSET
        if not isinstance(self.rhs, Unset):
            rhs = self.rhs.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "lhs": lhs,
                "op": op,
            }
        )
        if rhs is not UNSET:
            field_dict["rhs"] = rhs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression import RowsExpression

        d = dict(src_dict)
        lhs = RowsExpression.from_dict(d.pop("lhs"))

        op = RowsExpressionConditionOp(d.pop("op"))

        _rhs = d.pop("rhs", UNSET)
        rhs: RowsExpression | Unset
        if isinstance(_rhs, Unset):
            rhs = UNSET
        else:
            rhs = RowsExpression.from_dict(_rhs)

        rows_expression_condition = cls(
            lhs=lhs,
            op=op,
            rhs=rhs,
        )

        rows_expression_condition.additional_properties = d
        return rows_expression_condition

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
