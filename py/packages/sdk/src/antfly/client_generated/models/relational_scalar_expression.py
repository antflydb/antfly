from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_expression_op import RelationalExpressionOp
from ..models.relational_expression_type import RelationalExpressionType
from ..models.sql_builtin_type import SQLBuiltinType
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalScalarExpression")


@_attrs_define
class RelationalScalarExpression:
    """Immutable typed scalar expression, limited to 128 nodes and 16 levels.
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
    Only int16/int32/int64/float32/float64 identities are accepted here.

        Attributes:
            op (RelationalExpressionOp):
            type_ (RelationalExpressionType | Unset):
            sql_type (SQLBuiltinType | Unset): Exact PostgreSQL builtin identity for a relational root scalar column
                or the element identity of a `sql_array` column.
                Set the JSON Schema property's `x-antfly-sql-type` annotation to one of
                these values. The underlying property type must match. SQL array storage
                is not implied by this annotation. Existing unannotated schemas retain
                their original domains.
            value (Any | Unset): Typed literal value, including null.
            column (str | Unset):
            collation (str | Unset): Optional binary or ASCII case-insensitive collation for binary string comparison
                operations only; aliases match ordered indexes.
            args (list[RelationalScalarExpression] | Unset):
    """

    op: RelationalExpressionOp
    type_: RelationalExpressionType | Unset = UNSET
    sql_type: SQLBuiltinType | Unset = UNSET
    value: Any | Unset = UNSET
    column: str | Unset = UNSET
    collation: str | Unset = UNSET
    args: list[RelationalScalarExpression] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        op = self.op.value

        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        sql_type: str | Unset = UNSET
        if not isinstance(self.sql_type, Unset):
            sql_type = self.sql_type.value

        value = self.value

        column = self.column

        collation = self.collation

        args: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.args, Unset):
            args = []
            for args_item_data in self.args:
                args_item = args_item_data.to_dict()
                args.append(args_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "op": op,
            }
        )
        if type_ is not UNSET:
            field_dict["type"] = type_
        if sql_type is not UNSET:
            field_dict["sql_type"] = sql_type
        if value is not UNSET:
            field_dict["value"] = value
        if column is not UNSET:
            field_dict["column"] = column
        if collation is not UNSET:
            field_dict["collation"] = collation
        if args is not UNSET:
            field_dict["args"] = args

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        op = RelationalExpressionOp(d.pop("op"))

        _type_ = d.pop("type", UNSET)
        type_: RelationalExpressionType | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = RelationalExpressionType(_type_)

        _sql_type = d.pop("sql_type", UNSET)
        sql_type: SQLBuiltinType | Unset
        if isinstance(_sql_type, Unset):
            sql_type = UNSET
        else:
            sql_type = SQLBuiltinType(_sql_type)

        value = d.pop("value", UNSET)

        column = d.pop("column", UNSET)

        collation = d.pop("collation", UNSET)

        _args = d.pop("args", UNSET)
        args: list[RelationalScalarExpression] | Unset = UNSET
        if _args is not UNSET:
            args = []
            for args_item_data in _args:
                args_item = RelationalScalarExpression.from_dict(args_item_data)

                args.append(args_item)

        relational_scalar_expression = cls(
            op=op,
            type_=type_,
            sql_type=sql_type,
            value=value,
            column=column,
            collation=collation,
            args=args,
        )

        return relational_scalar_expression
