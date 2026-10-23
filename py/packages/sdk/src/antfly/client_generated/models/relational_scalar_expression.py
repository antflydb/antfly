from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_expression_op import RelationalExpressionOp
from ..models.relational_expression_type import RelationalExpressionType
from ..models.sql_builtin_type import SQLBuiltinType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_numeric_modifier import SQLNumericModifier


T = TypeVar("T", bound="RelationalScalarExpression")


@_attrs_define
class RelationalScalarExpression:
    """Typed scalar expression, limited to 128 nodes and 16 levels. CHECK,
    generated-column and index expressions must be immutable. UUIDv4 and
    UUIDv7 generation are permitted only in column defaults and require
    reader capability version 33. They run once during owned row
    preparation, never during schema publication or restore verification.
    The uuidv4 and uuidv7 operations take no args; uuidv7 may specify shift.
    A literal requires type; omitted value means typed null. A column
    requires column; other
    operations except uuidv4 and uuidv7 require args. Unknown or irrelevant fields are rejected.
    Arithmetic operands have the same integer or number type. Integer
    division truncates toward zero. Overflow and division by zero reject
    the write. Arithmetic and string operations propagate null. ASCII case
    operations leave non-ASCII bytes unchanged. No volatile functions are
    accepted. Allocated results are bounded to 1 MiB each.
    The replace operation takes three text arguments and performs literal,
    non-overlapping replacement; an empty search string leaves the input
    unchanged. md5 takes one text argument and returns 32 lowercase hex
    characters (a SQL compatibility fingerprint, not a security primitive).
    Both are strict in SQL NULL and require reader capability version 27,
    including when nested inside scalar/boolean expressions. Literal search
    patterns are prepared with immutable plans; matching, copies and dynamic
    pattern preparation share invocation work/cancellation admission.
    Numeric functions abs, ceil, floor, round, trunc, sign and sqrt require
    reader capability version 28, even when nested in boolean programs.
    They take one NUMERIC or float64 argument; abs also preserves integer
    widths and float32. round and trunc optionally accept an int32 scale
    as a second argument only for NUMERIC. Operands must already have their
    resolved SQL types; SQL lowering inserts required promotions. NUMERIC
    round uses ties away from zero; float64 round uses ties to even. All
    selected arguments evaluate before strict NULL propagation. NUMERIC
    execution never converts through float and shares invocation admission.
    Allocations and byte-comparison operand work share a 4 MiB evaluation budget per
    row and expression set. An integer literal may use a decimal string
    for exact int64 transport; blob uses base64 and datetime uses the
    normal relational datetime representation.
    Comparisons require operands of compatible types (integer and number may mix) and return boolean or
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
    in_list and not_in_list take one probe followed by 1 to 127 same-domain
    candidates. The probe is evaluated once; NULL probes return UNKNOWN.
    A matching candidate wins over NULL candidates; otherwise a NULL
    candidate makes the result UNKNOWN. These operations require schema
    capability version 19.
    JSON expressions require reader capability version 30. The json type
    carries JSONB semantics, including exact numeric values and display
    scale. jsonb_typeof and jsonb_array_length take one JSON argument;
    array length rejects a non-array value. jsonb_extract_path and
    jsonb_extract_path_text take a JSON argument followed by 1 to 31 text
    path components. Negative array ordinals count from the end; a missing
    path returns SQL NULL. JSON extraction retains JSON null; text extraction
    converts JSON null to SQL NULL. Arguments are evaluated strictly, while
    enclosing CASE/coalesce retain their short-circuit semantics.

        Attributes:
            op (RelationalExpressionOp):
            type_ (RelationalExpressionType | Unset):
            sql_type (SQLBuiltinType | Unset): Exact PostgreSQL builtin identity for a relational root scalar column.
                SQL array columns use SQLArrayElementType for their element identity.
                Set the JSON Schema property's `x-antfly-sql-type` annotation to one of
                these values. The underlying property type must match. SQL array storage
                is not implied by this annotation. Existing unannotated schemas retain
                their original domains.
                The numeric identity uses an underlying number property and exact
                PostgreSQL NUMERIC semantics, never binary float. Submit finite values
                as JSON numeric lexemes or decimal strings; special values use strings
                NaN, Infinity and -Infinity. Const/enum finite numeric members must be
                JSON numbers, not strings. Bounds and multipleOf are exact decimals.
                Public scalar NUMERIC schemas require reader capability version 22.
                To constrain a NUMERIC scalar or SQL-array column, set the root
                property's `x-antfly-sql-numeric-modifier` annotation to an object with
                precision (1..1000) and signed scale (-1000..1000). The annotation
                requires numeric identity and reader capability version 23. Assignment
                rounds before constraints, indexes and generated dependents; overflow
                rejects the write. Restore verifies stored values without rounding.
            numeric_modifier (SQLNumericModifier | Unset): PostgreSQL NUMERIC precision and signed scale. For arrays this
                describes every element, not dimensions. Absent means unconstrained NUMERIC.
            value (Any | Unset): Typed literal value, including null.
            shift (str | Unset): Canonical PostgreSQL interval text, accepted only on a uuidv7 default operation. SQL
                lowering resolves and binds calendar months, days, and microseconds before publication. Omission generates an
                unshifted UUID; SQL NULL shifts lower to a NULL literal instead. Infinite intervals remain representable
                declarations but fail when evaluated.
            json_null (bool | Unset): For a literal of type json only. True denotes present JSON null and requires value to
                be omitted or null. False or omission leaves an omitted/null value as SQL NULL. This explicit marker survives
                SDKs that omit optional null values. Requires reader capability version 30.
            column (str | Unset):
            collation (str | Unset): Optional binary or ASCII case-insensitive collation for string comparisons and in_list;
                aliases match ordered indexes.
            args (list[RelationalScalarExpression] | Unset):
    """

    op: RelationalExpressionOp
    type_: RelationalExpressionType | Unset = UNSET
    sql_type: SQLBuiltinType | Unset = UNSET
    numeric_modifier: SQLNumericModifier | Unset = UNSET
    value: Any | Unset = UNSET
    shift: str | Unset = UNSET
    json_null: bool | Unset = UNSET
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

        numeric_modifier: dict[str, Any] | Unset = UNSET
        if not isinstance(self.numeric_modifier, Unset):
            numeric_modifier = self.numeric_modifier.to_dict()

        value = self.value

        shift = self.shift

        json_null = self.json_null

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
        if numeric_modifier is not UNSET:
            field_dict["numeric_modifier"] = numeric_modifier
        if value is not UNSET:
            field_dict["value"] = value
        if shift is not UNSET:
            field_dict["shift"] = shift
        if json_null is not UNSET:
            field_dict["json_null"] = json_null
        if column is not UNSET:
            field_dict["column"] = column
        if collation is not UNSET:
            field_dict["collation"] = collation
        if args is not UNSET:
            field_dict["args"] = args

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_numeric_modifier import SQLNumericModifier

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

        _numeric_modifier = d.pop("numeric_modifier", UNSET)
        numeric_modifier: SQLNumericModifier | Unset
        if isinstance(_numeric_modifier, Unset):
            numeric_modifier = UNSET
        else:
            numeric_modifier = SQLNumericModifier.from_dict(_numeric_modifier)

        value = d.pop("value", UNSET)

        shift = d.pop("shift", UNSET)

        json_null = d.pop("json_null", UNSET)

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
            numeric_modifier=numeric_modifier,
            value=value,
            shift=shift,
            json_null=json_null,
            column=column,
            collation=collation,
            args=args,
        )

        return relational_scalar_expression
