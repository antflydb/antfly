from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.projection_expression_case_item import ProjectionExpressionCaseItem
    from ..models.projection_expression_cast import ProjectionExpressionCast


T = TypeVar("T", bound="ProjectionExpression")


@_attrs_define
class ProjectionExpression:
    """Exactly one expression node: literal, field, ref, param, call with args,
    case with optional else, cast, or a binary operator. Pure expressions
    lower to the same typed scalar IR as SQL SELECT and CASE. AI/provider
    calls remain in evaluate, never execute implicitly in select.
    value_identity(field) returns an opaque identity for a present logical
    value (including JSON null); missing has no identity. is_missing(field)
    distinguishes missing from present null. Identity reads require the same
    authorization and retained source revision as value reads. Metadata may
    accelerate them; unsupported/old metadata falls back to ordinary reads.
    Untaken CASE branches do not load fields or evaluate erroring calls.
    Supported binary nodes include eq, neq, lt, lte, gt, gte, is_distinct_from,
    is_not_distinct_from, and, or, add, subtract, multiply, divide and concat.
    Expressions are limited to depth 64 including referenced aliases, and
    8192 nodes per request. Alias dependencies are acyclic; each alias is a
    requested output and is evaluated once per row.

        Attributes:
            literal (Any | Unset):
            field (str | Unset): Logical source path. Dots traverse objects; numeric array components select an original
                index (items.1.name), and named components traverse arrays collecting present values (items.name). Nested user
                keys such as metadata._type are allowed. Internal root fields are reserved except _id.
            ref (str | Unset):
            param (str | Unset):
            call (str | Unset): Public pure SQL scalar function name, including value_identity and is_missing. Internal and
                provider-backed functions are forbidden.
            args (list[ProjectionExpression] | Unset):
            case (list[ProjectionExpressionCaseItem] | Unset):
            else_ (ProjectionExpression | Unset): Exactly one expression node: literal, field, ref, param, call with args,
                case with optional else, cast, or a binary operator. Pure expressions
                lower to the same typed scalar IR as SQL SELECT and CASE. AI/provider
                calls remain in evaluate, never execute implicitly in select.
                value_identity(field) returns an opaque identity for a present logical
                value (including JSON null); missing has no identity. is_missing(field)
                distinguishes missing from present null. Identity reads require the same
                authorization and retained source revision as value reads. Metadata may
                accelerate them; unsupported/old metadata falls back to ordinary reads.
                Untaken CASE branches do not load fields or evaluate erroring calls.
                Supported binary nodes include eq, neq, lt, lte, gt, gte, is_distinct_from,
                is_not_distinct_from, and, or, add, subtract, multiply, divide and concat.
                Expressions are limited to depth 64 including referenced aliases, and
                8192 nodes per request. Alias dependencies are acyclic; each alias is a
                requested output and is evaluated once per row.
            cast (ProjectionExpressionCast | Unset):
            eq (list[ProjectionExpression] | Unset):
            neq (list[ProjectionExpression] | Unset):
            lt (list[ProjectionExpression] | Unset):
            lte (list[ProjectionExpression] | Unset):
            gt (list[ProjectionExpression] | Unset):
            gte (list[ProjectionExpression] | Unset):
            is_distinct_from (list[ProjectionExpression] | Unset):
            is_not_distinct_from (list[ProjectionExpression] | Unset):
            and_ (list[ProjectionExpression] | Unset):
            or_ (list[ProjectionExpression] | Unset):
            add (list[ProjectionExpression] | Unset):
            subtract (list[ProjectionExpression] | Unset):
            multiply (list[ProjectionExpression] | Unset):
            divide (list[ProjectionExpression] | Unset):
            concat (list[ProjectionExpression] | Unset):
    """

    literal: Any | Unset = UNSET
    field: str | Unset = UNSET
    ref: str | Unset = UNSET
    param: str | Unset = UNSET
    call: str | Unset = UNSET
    args: list[ProjectionExpression] | Unset = UNSET
    case: list[ProjectionExpressionCaseItem] | Unset = UNSET
    else_: ProjectionExpression | Unset = UNSET
    cast: ProjectionExpressionCast | Unset = UNSET
    eq: list[ProjectionExpression] | Unset = UNSET
    neq: list[ProjectionExpression] | Unset = UNSET
    lt: list[ProjectionExpression] | Unset = UNSET
    lte: list[ProjectionExpression] | Unset = UNSET
    gt: list[ProjectionExpression] | Unset = UNSET
    gte: list[ProjectionExpression] | Unset = UNSET
    is_distinct_from: list[ProjectionExpression] | Unset = UNSET
    is_not_distinct_from: list[ProjectionExpression] | Unset = UNSET
    and_: list[ProjectionExpression] | Unset = UNSET
    or_: list[ProjectionExpression] | Unset = UNSET
    add: list[ProjectionExpression] | Unset = UNSET
    subtract: list[ProjectionExpression] | Unset = UNSET
    multiply: list[ProjectionExpression] | Unset = UNSET
    divide: list[ProjectionExpression] | Unset = UNSET
    concat: list[ProjectionExpression] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        literal = self.literal

        field = self.field

        ref = self.ref

        param = self.param

        call = self.call

        args: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.args, Unset):
            args = []
            for args_item_data in self.args:
                args_item = args_item_data.to_dict()
                args.append(args_item)

        case: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.case, Unset):
            case = []
            for case_item_data in self.case:
                case_item = case_item_data.to_dict()
                case.append(case_item)

        else_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.else_, Unset):
            else_ = self.else_.to_dict()

        cast: dict[str, Any] | Unset = UNSET
        if not isinstance(self.cast, Unset):
            cast = self.cast.to_dict()

        eq: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.eq, Unset):
            eq = []
            for eq_item_data in self.eq:
                eq_item = eq_item_data.to_dict()
                eq.append(eq_item)

        neq: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.neq, Unset):
            neq = []
            for neq_item_data in self.neq:
                neq_item = neq_item_data.to_dict()
                neq.append(neq_item)

        lt: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.lt, Unset):
            lt = []
            for lt_item_data in self.lt:
                lt_item = lt_item_data.to_dict()
                lt.append(lt_item)

        lte: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.lte, Unset):
            lte = []
            for lte_item_data in self.lte:
                lte_item = lte_item_data.to_dict()
                lte.append(lte_item)

        gt: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.gt, Unset):
            gt = []
            for gt_item_data in self.gt:
                gt_item = gt_item_data.to_dict()
                gt.append(gt_item)

        gte: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.gte, Unset):
            gte = []
            for gte_item_data in self.gte:
                gte_item = gte_item_data.to_dict()
                gte.append(gte_item)

        is_distinct_from: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.is_distinct_from, Unset):
            is_distinct_from = []
            for is_distinct_from_item_data in self.is_distinct_from:
                is_distinct_from_item = is_distinct_from_item_data.to_dict()
                is_distinct_from.append(is_distinct_from_item)

        is_not_distinct_from: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.is_not_distinct_from, Unset):
            is_not_distinct_from = []
            for is_not_distinct_from_item_data in self.is_not_distinct_from:
                is_not_distinct_from_item = is_not_distinct_from_item_data.to_dict()
                is_not_distinct_from.append(is_not_distinct_from_item)

        and_: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.and_, Unset):
            and_ = []
            for and_item_data in self.and_:
                and_item = and_item_data.to_dict()
                and_.append(and_item)

        or_: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.or_, Unset):
            or_ = []
            for or_item_data in self.or_:
                or_item = or_item_data.to_dict()
                or_.append(or_item)

        add: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.add, Unset):
            add = []
            for add_item_data in self.add:
                add_item = add_item_data.to_dict()
                add.append(add_item)

        subtract: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.subtract, Unset):
            subtract = []
            for subtract_item_data in self.subtract:
                subtract_item = subtract_item_data.to_dict()
                subtract.append(subtract_item)

        multiply: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.multiply, Unset):
            multiply = []
            for multiply_item_data in self.multiply:
                multiply_item = multiply_item_data.to_dict()
                multiply.append(multiply_item)

        divide: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.divide, Unset):
            divide = []
            for divide_item_data in self.divide:
                divide_item = divide_item_data.to_dict()
                divide.append(divide_item)

        concat: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.concat, Unset):
            concat = []
            for concat_item_data in self.concat:
                concat_item = concat_item_data.to_dict()
                concat.append(concat_item)

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if literal is not UNSET:
            field_dict["literal"] = literal
        if field is not UNSET:
            field_dict["field"] = field
        if ref is not UNSET:
            field_dict["ref"] = ref
        if param is not UNSET:
            field_dict["param"] = param
        if call is not UNSET:
            field_dict["call"] = call
        if args is not UNSET:
            field_dict["args"] = args
        if case is not UNSET:
            field_dict["case"] = case
        if else_ is not UNSET:
            field_dict["else"] = else_
        if cast is not UNSET:
            field_dict["cast"] = cast
        if eq is not UNSET:
            field_dict["eq"] = eq
        if neq is not UNSET:
            field_dict["neq"] = neq
        if lt is not UNSET:
            field_dict["lt"] = lt
        if lte is not UNSET:
            field_dict["lte"] = lte
        if gt is not UNSET:
            field_dict["gt"] = gt
        if gte is not UNSET:
            field_dict["gte"] = gte
        if is_distinct_from is not UNSET:
            field_dict["is_distinct_from"] = is_distinct_from
        if is_not_distinct_from is not UNSET:
            field_dict["is_not_distinct_from"] = is_not_distinct_from
        if and_ is not UNSET:
            field_dict["and"] = and_
        if or_ is not UNSET:
            field_dict["or"] = or_
        if add is not UNSET:
            field_dict["add"] = add
        if subtract is not UNSET:
            field_dict["subtract"] = subtract
        if multiply is not UNSET:
            field_dict["multiply"] = multiply
        if divide is not UNSET:
            field_dict["divide"] = divide
        if concat is not UNSET:
            field_dict["concat"] = concat

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.projection_expression_case_item import ProjectionExpressionCaseItem
        from ..models.projection_expression_cast import ProjectionExpressionCast

        d = dict(src_dict)
        literal = d.pop("literal", UNSET)

        field = d.pop("field", UNSET)

        ref = d.pop("ref", UNSET)

        param = d.pop("param", UNSET)

        call = d.pop("call", UNSET)

        _args = d.pop("args", UNSET)
        args: list[ProjectionExpression] | Unset = UNSET
        if _args is not UNSET:
            args = []
            for args_item_data in _args:
                args_item = ProjectionExpression.from_dict(args_item_data)

                args.append(args_item)

        _case = d.pop("case", UNSET)
        case: list[ProjectionExpressionCaseItem] | Unset = UNSET
        if _case is not UNSET:
            case = []
            for case_item_data in _case:
                case_item = ProjectionExpressionCaseItem.from_dict(case_item_data)

                case.append(case_item)

        _else_ = d.pop("else", UNSET)
        else_: ProjectionExpression | Unset
        if isinstance(_else_, Unset):
            else_ = UNSET
        else:
            else_ = ProjectionExpression.from_dict(_else_)

        _cast = d.pop("cast", UNSET)
        cast: ProjectionExpressionCast | Unset
        if isinstance(_cast, Unset):
            cast = UNSET
        else:
            cast = ProjectionExpressionCast.from_dict(_cast)

        _eq = d.pop("eq", UNSET)
        eq: list[ProjectionExpression] | Unset = UNSET
        if _eq is not UNSET:
            eq = []
            for eq_item_data in _eq:
                eq_item = ProjectionExpression.from_dict(eq_item_data)

                eq.append(eq_item)

        _neq = d.pop("neq", UNSET)
        neq: list[ProjectionExpression] | Unset = UNSET
        if _neq is not UNSET:
            neq = []
            for neq_item_data in _neq:
                neq_item = ProjectionExpression.from_dict(neq_item_data)

                neq.append(neq_item)

        _lt = d.pop("lt", UNSET)
        lt: list[ProjectionExpression] | Unset = UNSET
        if _lt is not UNSET:
            lt = []
            for lt_item_data in _lt:
                lt_item = ProjectionExpression.from_dict(lt_item_data)

                lt.append(lt_item)

        _lte = d.pop("lte", UNSET)
        lte: list[ProjectionExpression] | Unset = UNSET
        if _lte is not UNSET:
            lte = []
            for lte_item_data in _lte:
                lte_item = ProjectionExpression.from_dict(lte_item_data)

                lte.append(lte_item)

        _gt = d.pop("gt", UNSET)
        gt: list[ProjectionExpression] | Unset = UNSET
        if _gt is not UNSET:
            gt = []
            for gt_item_data in _gt:
                gt_item = ProjectionExpression.from_dict(gt_item_data)

                gt.append(gt_item)

        _gte = d.pop("gte", UNSET)
        gte: list[ProjectionExpression] | Unset = UNSET
        if _gte is not UNSET:
            gte = []
            for gte_item_data in _gte:
                gte_item = ProjectionExpression.from_dict(gte_item_data)

                gte.append(gte_item)

        _is_distinct_from = d.pop("is_distinct_from", UNSET)
        is_distinct_from: list[ProjectionExpression] | Unset = UNSET
        if _is_distinct_from is not UNSET:
            is_distinct_from = []
            for is_distinct_from_item_data in _is_distinct_from:
                is_distinct_from_item = ProjectionExpression.from_dict(is_distinct_from_item_data)

                is_distinct_from.append(is_distinct_from_item)

        _is_not_distinct_from = d.pop("is_not_distinct_from", UNSET)
        is_not_distinct_from: list[ProjectionExpression] | Unset = UNSET
        if _is_not_distinct_from is not UNSET:
            is_not_distinct_from = []
            for is_not_distinct_from_item_data in _is_not_distinct_from:
                is_not_distinct_from_item = ProjectionExpression.from_dict(is_not_distinct_from_item_data)

                is_not_distinct_from.append(is_not_distinct_from_item)

        _and_ = d.pop("and", UNSET)
        and_: list[ProjectionExpression] | Unset = UNSET
        if _and_ is not UNSET:
            and_ = []
            for and_item_data in _and_:
                and_item = ProjectionExpression.from_dict(and_item_data)

                and_.append(and_item)

        _or_ = d.pop("or", UNSET)
        or_: list[ProjectionExpression] | Unset = UNSET
        if _or_ is not UNSET:
            or_ = []
            for or_item_data in _or_:
                or_item = ProjectionExpression.from_dict(or_item_data)

                or_.append(or_item)

        _add = d.pop("add", UNSET)
        add: list[ProjectionExpression] | Unset = UNSET
        if _add is not UNSET:
            add = []
            for add_item_data in _add:
                add_item = ProjectionExpression.from_dict(add_item_data)

                add.append(add_item)

        _subtract = d.pop("subtract", UNSET)
        subtract: list[ProjectionExpression] | Unset = UNSET
        if _subtract is not UNSET:
            subtract = []
            for subtract_item_data in _subtract:
                subtract_item = ProjectionExpression.from_dict(subtract_item_data)

                subtract.append(subtract_item)

        _multiply = d.pop("multiply", UNSET)
        multiply: list[ProjectionExpression] | Unset = UNSET
        if _multiply is not UNSET:
            multiply = []
            for multiply_item_data in _multiply:
                multiply_item = ProjectionExpression.from_dict(multiply_item_data)

                multiply.append(multiply_item)

        _divide = d.pop("divide", UNSET)
        divide: list[ProjectionExpression] | Unset = UNSET
        if _divide is not UNSET:
            divide = []
            for divide_item_data in _divide:
                divide_item = ProjectionExpression.from_dict(divide_item_data)

                divide.append(divide_item)

        _concat = d.pop("concat", UNSET)
        concat: list[ProjectionExpression] | Unset = UNSET
        if _concat is not UNSET:
            concat = []
            for concat_item_data in _concat:
                concat_item = ProjectionExpression.from_dict(concat_item_data)

                concat.append(concat_item)

        projection_expression = cls(
            literal=literal,
            field=field,
            ref=ref,
            param=param,
            call=call,
            args=args,
            case=case,
            else_=else_,
            cast=cast,
            eq=eq,
            neq=neq,
            lt=lt,
            lte=lte,
            gt=gt,
            gte=gte,
            is_distinct_from=is_distinct_from,
            is_not_distinct_from=is_not_distinct_from,
            and_=and_,
            or_=or_,
            add=add,
            subtract=subtract,
            multiply=multiply,
            divide=divide,
            concat=concat,
        )

        return projection_expression
