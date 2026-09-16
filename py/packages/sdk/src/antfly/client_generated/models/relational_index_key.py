from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_expression_type import RelationalExpressionType
from ..models.relational_index_key_direction import RelationalIndexKeyDirection
from ..models.relational_index_key_nulls import RelationalIndexKeyNulls
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_scalar_expression import RelationalScalarExpression


T = TypeVar("T", bound="RelationalIndexKey")


@_attrs_define
class RelationalIndexKey:
    """Ordered component of a relational ordered-tuple index key. Supply
    either a declared column or a deterministic typed scalar expression
    with its result_type. Composite keys may mix both forms. Bounds use
    the expression result type, not its input columns.

        Attributes:
            column (str | Unset): Declared relational column used by this key component.
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
            result_type (RelationalExpressionType | Unset):
            collation (str | Unset): String-key collation. Omission selects binary ordering. Supported
                binary aliases are C, POSIX, and binary. The aliases ci,
                case_insensitive, and antfly.case_insensitive select ASCII-only
                case folding, not locale-aware or Unicode case folding.
            direction (RelationalIndexKeyDirection | Unset): Direction of one ordered index key component. Omission selects
                asc.
            nulls (RelationalIndexKeyNulls | Unset): Null placement for one ordered index key component. The default is
                last for ascending keys and first for descending keys. Omission selects default.
    """

    column: str | Unset = UNSET
    expression: RelationalScalarExpression | Unset = UNSET
    result_type: RelationalExpressionType | Unset = UNSET
    collation: str | Unset = UNSET
    direction: RelationalIndexKeyDirection | Unset = UNSET
    nulls: RelationalIndexKeyNulls | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        column = self.column

        expression: dict[str, Any] | Unset = UNSET
        if not isinstance(self.expression, Unset):
            expression = self.expression.to_dict()

        result_type: str | Unset = UNSET
        if not isinstance(self.result_type, Unset):
            result_type = self.result_type.value

        collation = self.collation

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        nulls: str | Unset = UNSET
        if not isinstance(self.nulls, Unset):
            nulls = self.nulls.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if column is not UNSET:
            field_dict["column"] = column
        if expression is not UNSET:
            field_dict["expression"] = expression
        if result_type is not UNSET:
            field_dict["result_type"] = result_type
        if collation is not UNSET:
            field_dict["collation"] = collation
        if direction is not UNSET:
            field_dict["direction"] = direction
        if nulls is not UNSET:
            field_dict["nulls"] = nulls

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_scalar_expression import RelationalScalarExpression

        d = dict(src_dict)
        column = d.pop("column", UNSET)

        _expression = d.pop("expression", UNSET)
        expression: RelationalScalarExpression | Unset
        if isinstance(_expression, Unset):
            expression = UNSET
        else:
            expression = RelationalScalarExpression.from_dict(_expression)

        _result_type = d.pop("result_type", UNSET)
        result_type: RelationalExpressionType | Unset
        if isinstance(_result_type, Unset):
            result_type = UNSET
        else:
            result_type = RelationalExpressionType(_result_type)

        collation = d.pop("collation", UNSET)

        _direction = d.pop("direction", UNSET)
        direction: RelationalIndexKeyDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = RelationalIndexKeyDirection(_direction)

        _nulls = d.pop("nulls", UNSET)
        nulls: RelationalIndexKeyNulls | Unset
        if isinstance(_nulls, Unset):
            nulls = UNSET
        else:
            nulls = RelationalIndexKeyNulls(_nulls)

        relational_index_key = cls(
            column=column,
            expression=expression,
            result_type=result_type,
            collation=collation,
            direction=direction,
            nulls=nulls,
        )

        return relational_index_key
