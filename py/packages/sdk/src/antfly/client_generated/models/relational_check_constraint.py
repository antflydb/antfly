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
                Numeric literals and arithmetic operations may specify sql_type to
                retain PostgreSQL builtin overflow and float4 rounding semantics.
                Without it, integer and number operations retain int64 and float64
                semantics. Numeric cast requires type and sql_type, takes one numeric
                argument, and performs a checked conversion when evaluated (not when
                the schema is compiled). Floating-to-integer casts round ties to even.
                The numeric expression type uses exact PostgreSQL NUMERIC values and
                may specify sql_type numeric. Its literals accept decimal strings or
                exact JSON numeric lexemes, including string-valued special values.
                Exact NUMERIC programs require reader capability version 21 even when
                their result is boolean or integer. Float/integer assignment casts keep
                their declared PostgreSQL rounding and overflow semantics.
                A cast to numeric may specify numeric_modifier for PostgreSQL precision
                and signed-scale coercion. Overflow is checked when the selected cast
                executes; unselected lazy branches do not fail. Modifier-bearing programs
                require reader capability version 23 even with integer/boolean output.
                The sql_array expression type requires sql_type on literals, including
                typed NULL, to declare the element builtin. Non-null literals use the
                ordinal SQL array envelope (dimensions with length/lower_bound, values,
                and sql_nulls), retaining shape and lower bounds. Array columns derive
                their exact element identity from the immutable schema. Comparisons,
                IN, COALESCE and CASE require matching array element identities; no
                element type is inferred from values. Array-dependent programs require
                reader capability version 24 even with scalar output. Assignment to a
                NUMERIC array column applies its precision/signed-scale modifier to
                each element. Array casts require type sql_array and an explicit matching
                sql_type; identity casts borrow the immutable input. Casts of numeric
                arrays may additionally specify numeric_modifier, coercing each non-NULL
                element with PostgreSQL precision and signed-scale semantics while
                preserving dimensions, lower bounds and NULL slots. Coercion is lazy
                and shares invocation admission with the surrounding expression.
                Array-valued ordered index keys and element-changing array casts are
                not supported by this expression contract.
                The array constructor requires sql_type and zero to 32 arguments.
                Constructor programs additionally require reader capability version 25,
                including constructors hidden inside scalar/boolean expressions.
                Scalar arguments must have the declared element domain, with explicit
                width-preserving numeric casts where needed. SQL NULL arguments become
                NULL elements. Array arguments must all have matching element types,
                dimensions and lower bounds; one leading dimension with lower bound 1
                is added. All empty/NULL subarrays produce an empty array; mixing an
                empty/NULL subarray with a nonempty one is a dimension mismatch. Child
                expressions execute once, with shared work/cancellation and byte limits.
                case_when takes alternating boolean conditions and result expressions,
                followed by a mandatory fallback result (3 to 31 arguments, at most
                15 branches). Conditions are evaluated in order; only the selected
                result is evaluated, and a NULL condition is not TRUE. All result
                expressions must have the same physical type. Numeric SQL lowering
                records builtin result-domain promotions as explicit casts. This
                operation requires schema capability version 18.
                modulo takes two same-domain integer or NUMERIC operands and returns the signed
                remainder (minInt modulo -1 is zero); a zero divisor rejects the write.
                in_list and not_in_list take one probe followed by 1 to 31 same-domain
                candidates. The probe is evaluated once; NULL probes return UNKNOWN.
                A matching candidate wins over NULL candidates; otherwise a NULL
                candidate makes the result UNKNOWN. These operations require schema
                capability version 19.
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
