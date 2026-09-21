from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.relational_scalar_expression import RelationalScalarExpression


T = TypeVar("T", bound="RelationalColumnExpression")


@_attrs_define
class RelationalColumnExpression:
    """
    Attributes:
        column (str):
        expression (RelationalScalarExpression): Immutable typed scalar expression, limited to 128 nodes and 16 levels.
            A literal requires type; omitted value means typed null. A column
            requires column; other
            operations require args. Unknown or irrelevant fields are rejected.
            Arithmetic operands have the same integer or number type. Integer
            division truncates toward zero. Overflow and division by zero reject
            the write. Arithmetic and string operations propagate null. ASCII case
            operations leave non-ASCII bytes unchanged. No volatile functions are
            accepted. Allocated results are bounded to 1 MiB each. Allocations
            and byte-comparison operand work share a 4 MiB evaluation budget per
            row and expression set. An integer literal may use a decimal string
            for exact int64 transport; blob uses base64 and datetime uses the
            normal relational datetime representation.
            Comparisons require operands of the same type and return boolean or
            SQL UNKNOWN (null); is_distinct and is_not_distinct always return a
            boolean. Unary is_null and is_not_null test presence/null. AND and OR
            evaluate left to right with SQL three-valued short-circuit semantics;
            NOT preserves UNKNOWN. CHECK accepts TRUE and UNKNOWN, rejecting FALSE.
    """

    column: str
    expression: RelationalScalarExpression

    def to_dict(self) -> dict[str, Any]:
        column = self.column

        expression = self.expression.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "column": column,
                "expression": expression,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_scalar_expression import RelationalScalarExpression

        d = dict(src_dict)
        column = d.pop("column")

        expression = RelationalScalarExpression.from_dict(d.pop("expression"))

        relational_column_expression = cls(
            column=column,
            expression=expression,
        )

        return relational_column_expression
