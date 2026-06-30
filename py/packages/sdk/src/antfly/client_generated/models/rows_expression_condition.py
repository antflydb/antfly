from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_expression_condition_op import RowsExpressionConditionOp
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue


T = TypeVar("T", bound="RowsExpressionCondition")


@_attrs_define
class RowsExpressionCondition:
    """Computed expression predicate over the shared row-expression AST.

    Attributes:
        lhs (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue): Shared typed row-expression AST. A
            node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
        op (RowsExpressionConditionOp):
        rhs (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset): Shared typed row-expression
            AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
    """

    lhs: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue
    op: RowsExpressionConditionOp
    rhs: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue

        lhs: dict[str, Any]
        if isinstance(self.lhs, RowsExpressionField):
            lhs = self.lhs.to_dict()
        elif isinstance(self.lhs, RowsExpressionValue):
            lhs = self.lhs.to_dict()
        else:
            lhs = self.lhs.to_dict()

        op = self.op.value

        rhs: dict[str, Any] | Unset
        if isinstance(self.rhs, Unset):
            rhs = UNSET
        elif isinstance(self.rhs, RowsExpressionField):
            rhs = self.rhs.to_dict()
        elif isinstance(self.rhs, RowsExpressionValue):
            rhs = self.rhs.to_dict()
        else:
            rhs = self.rhs.to_dict()

        field_dict: dict[str, Any] = {}

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
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_value import RowsExpressionValue

        d = dict(src_dict)

        def _parse_lhs(data: object) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_expression_type_0 = RowsExpressionField.from_dict(data)

                return componentsschemas_rows_expression_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_expression_type_1 = RowsExpressionValue.from_dict(data)

                return componentsschemas_rows_expression_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_rows_expression_type_2 = RowsExpressionOperator.from_dict(data)

            return componentsschemas_rows_expression_type_2

        lhs = _parse_lhs(d.pop("lhs"))

        op = RowsExpressionConditionOp(d.pop("op"))

        def _parse_rhs(data: object) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_expression_type_0 = RowsExpressionField.from_dict(data)

                return componentsschemas_rows_expression_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_expression_type_1 = RowsExpressionValue.from_dict(data)

                return componentsschemas_rows_expression_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_rows_expression_type_2 = RowsExpressionOperator.from_dict(data)

            return componentsschemas_rows_expression_type_2

        rhs = _parse_rhs(d.pop("rhs", UNSET))

        rows_expression_condition = cls(
            lhs=lhs,
            op=op,
            rhs=rhs,
        )

        return rows_expression_condition
