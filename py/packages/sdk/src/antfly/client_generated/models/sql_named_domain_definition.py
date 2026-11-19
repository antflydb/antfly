from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_scalar_expression import RelationalScalarExpression
    from ..models.sql_named_domain_base import SQLNamedDomainBase
    from ..models.sql_named_domain_check import SQLNamedDomainCheck
    from ..models.sql_numeric_modifier import SQLNumericModifier


T = TypeVar("T", bound="SQLNamedDomainDefinition")


@_attrs_define
class SQLNamedDomainDefinition:
    """Server-resolved immutable domain contract. Defaults affect future assignments; restrictive changes require
    dependent-owner activation.

        Attributes:
            base (SQLNamedDomainBase): Exactly one builtin or named base. The server resolves and verifies the complete base
                chain before publication.
            not_null (bool):
            checks (list[SQLNamedDomainCheck]):
            numeric_modifier (SQLNumericModifier | Unset): PostgreSQL NUMERIC precision and signed scale. For arrays this
                describes every element, not dimensions. Absent means unconstrained NUMERIC.
            default_expression (RelationalScalarExpression | Unset): Typed scalar expression, limited to 128 nodes and 16
                levels. CHECK,
                generated-column and index expressions must be immutable. UUIDv4 and
                UUIDv7 generation are permitted only in column defaults and require
                reader capability version 33. They run once during owned row
                preparation, never during schema publication or restore verification.
                Transaction-clock defaults require reader capability version 34 and
                use the transaction owner's captured instant, shared across rows and
                statements in an explicit transaction. Stored values are preserved on restore.
                The uuidv4, uuidv7 and transaction_timestamp operations take no args;
                uuidv7 may specify shift and transaction_timestamp may specify precision.
                A literal requires type; omitted value means typed null. A column
                requires column; other
                operations except uuidv4, uuidv7 and transaction_timestamp require args. Unknown or irrelevant fields are
                rejected.
                Arithmetic operands have the same integer or number type. Integer
                division truncates toward zero. Overflow and division by zero reject
                the write. Arithmetic and string operations propagate null. ASCII case
                operations leave non-ASCII bytes unchanged. Volatile operations are restricted to column defaults. Allocated
                results are bounded to 1 MiB each.
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
                Enumeration expressions require reader capability version 40. Enum
                literals and casts declare named_type_id from the owning schema directory.
                Input labels are exact and case-sensitive; comparisons retain declaration
                order and reject unrelated named identities. Enum/text conversions are
                explicit; stored enum values retain four-byte symbols. CHECK, generated
                values, defaults, typed-row ingress and restore share the native validator.
                JSON expressions require reader capability version 30. The json type
                carries JSONB semantics, including exact numeric values and display
                scale. jsonb_typeof and jsonb_array_length take one JSON argument;
                array length rejects a non-array value. jsonb_extract_path and
                jsonb_extract_path_text take a JSON argument followed by 1 to 31 text
                path components. Negative array ordinals count from the end; a missing
                path returns SQL NULL. JSON extraction retains JSON null; text extraction
                converts JSON null to SQL NULL. Arguments are evaluated strictly, while
                enclosing CASE/coalesce retain their short-circuit semantics.
    """

    base: SQLNamedDomainBase
    not_null: bool
    checks: list[SQLNamedDomainCheck]
    numeric_modifier: SQLNumericModifier | Unset = UNSET
    default_expression: RelationalScalarExpression | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        base = self.base.to_dict()

        not_null = self.not_null

        checks = []
        for checks_item_data in self.checks:
            checks_item = checks_item_data.to_dict()
            checks.append(checks_item)

        numeric_modifier: dict[str, Any] | Unset = UNSET
        if not isinstance(self.numeric_modifier, Unset):
            numeric_modifier = self.numeric_modifier.to_dict()

        default_expression: dict[str, Any] | Unset = UNSET
        if not isinstance(self.default_expression, Unset):
            default_expression = self.default_expression.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "base": base,
                "not_null": not_null,
                "checks": checks,
            }
        )
        if numeric_modifier is not UNSET:
            field_dict["numeric_modifier"] = numeric_modifier
        if default_expression is not UNSET:
            field_dict["default_expression"] = default_expression

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_scalar_expression import RelationalScalarExpression
        from ..models.sql_named_domain_base import SQLNamedDomainBase
        from ..models.sql_named_domain_check import SQLNamedDomainCheck
        from ..models.sql_numeric_modifier import SQLNumericModifier

        d = dict(src_dict)
        base = SQLNamedDomainBase.from_dict(d.pop("base"))

        not_null = d.pop("not_null")

        checks = []
        _checks = d.pop("checks")
        for checks_item_data in _checks:
            checks_item = SQLNamedDomainCheck.from_dict(checks_item_data)

            checks.append(checks_item)

        _numeric_modifier = d.pop("numeric_modifier", UNSET)
        numeric_modifier: SQLNumericModifier | Unset
        if isinstance(_numeric_modifier, Unset):
            numeric_modifier = UNSET
        else:
            numeric_modifier = SQLNumericModifier.from_dict(_numeric_modifier)

        _default_expression = d.pop("default_expression", UNSET)
        default_expression: RelationalScalarExpression | Unset
        if isinstance(_default_expression, Unset):
            default_expression = UNSET
        else:
            default_expression = RelationalScalarExpression.from_dict(_default_expression)

        sql_named_domain_definition = cls(
            base=base,
            not_null=not_null,
            checks=checks,
            numeric_modifier=numeric_modifier,
            default_expression=default_expression,
        )

        return sql_named_domain_definition
