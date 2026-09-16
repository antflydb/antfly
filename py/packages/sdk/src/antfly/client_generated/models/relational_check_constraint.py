from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_comparison_op import RelationalComparisonOp
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_scalar_expression import RelationalScalarExpression


T = TypeVar("T", bound="RelationalCheckConstraint")


@_attrs_define
class RelationalCheckConstraint:
    """A typed CHECK. Supply either expression or column and op (with optional
    value and collation), never both forms. Expressions must return boolean
    and use the shared bounded immutable scalar expression vocabulary.
    New writes are checked from schema publication;
    existing rows are validated separately. SQL UNKNOWN satisfies CHECK.
    Comparison values must match the column type. Integer values may also
    use exact decimal strings to avoid client-side floating-point rounding.

        Attributes:
            name (str):
            column (str | Unset):
            op (RelationalComparisonOp | Unset):
            value (Any | Unset): Scalar comparison operand. Omission represents NULL. Null tests require a NULL operand.
            collation (str | Unset): String comparison collation; uses the same rules as ordered indexes.
            expression (RelationalScalarExpression | Unset): Immutable typed scalar expression, limited to 128 nodes and 16
                levels.
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

    name: str
    column: str | Unset = UNSET
    op: RelationalComparisonOp | Unset = UNSET
    value: Any | Unset = UNSET
    collation: str | Unset = UNSET
    expression: RelationalScalarExpression | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        column = self.column

        op: str | Unset = UNSET
        if not isinstance(self.op, Unset):
            op = self.op.value

        value = self.value

        collation = self.collation

        expression: dict[str, Any] | Unset = UNSET
        if not isinstance(self.expression, Unset):
            expression = self.expression.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
            }
        )
        if column is not UNSET:
            field_dict["column"] = column
        if op is not UNSET:
            field_dict["op"] = op
        if value is not UNSET:
            field_dict["value"] = value
        if collation is not UNSET:
            field_dict["collation"] = collation
        if expression is not UNSET:
            field_dict["expression"] = expression

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_scalar_expression import RelationalScalarExpression

        d = dict(src_dict)
        name = d.pop("name")

        column = d.pop("column", UNSET)

        _op = d.pop("op", UNSET)
        op: RelationalComparisonOp | Unset
        if isinstance(_op, Unset):
            op = UNSET
        else:
            op = RelationalComparisonOp(_op)

        value = d.pop("value", UNSET)

        collation = d.pop("collation", UNSET)

        _expression = d.pop("expression", UNSET)
        expression: RelationalScalarExpression | Unset
        if isinstance(_expression, Unset):
            expression = UNSET
        else:
            expression = RelationalScalarExpression.from_dict(_expression)

        relational_check_constraint = cls(
            name=name,
            column=column,
            op=op,
            value=value,
            collation=collation,
            expression=expression,
        )

        return relational_check_constraint
