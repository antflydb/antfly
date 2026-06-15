from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_expression_operator_op import RowsExpressionOperatorOp
from ..models.rows_expression_operator_to import RowsExpressionOperatorTo
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_case_branch import RowsExpressionCaseBranch
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_value import RowsExpressionValue


T = TypeVar("T", bound="RowsExpressionOperator")


@_attrs_define
class RowsExpressionOperator:
    """
    Attributes:
        op (RowsExpressionOperatorOp):
        args (list[RowsExpressionField | RowsExpressionOperator | RowsExpressionValue] | Unset): Operand expressions for
            operator nodes.
        to (RowsExpressionOperatorTo | Unset): Cast target for `cast`.
        path (Any | Unset): Structured JSON path for `json_extract` and `json_path_exists`.
        as_text (bool | Unset): Return JSON path extraction as text.
        cases (list[RowsExpressionCaseBranch] | Unset): Searched case branches, each with `when` and `then`.
        else_ (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset): Shared typed row-expression
            AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
    """

    op: RowsExpressionOperatorOp
    args: list[RowsExpressionField | RowsExpressionOperator | RowsExpressionValue] | Unset = UNSET
    to: RowsExpressionOperatorTo | Unset = UNSET
    path: Any | Unset = UNSET
    as_text: bool | Unset = UNSET
    cases: list[RowsExpressionCaseBranch] | Unset = UNSET
    else_: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue

        op = self.op.value

        args: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.args, Unset):
            args = []
            for args_item_data in self.args:
                args_item: dict[str, Any]
                if isinstance(args_item_data, RowsExpressionField):
                    args_item = args_item_data.to_dict()
                elif isinstance(args_item_data, RowsExpressionValue):
                    args_item = args_item_data.to_dict()
                else:
                    args_item = args_item_data.to_dict()

                args.append(args_item)

        to: str | Unset = UNSET
        if not isinstance(self.to, Unset):
            to = self.to.value

        path = self.path

        as_text = self.as_text

        cases: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.cases, Unset):
            cases = []
            for cases_item_data in self.cases:
                cases_item = cases_item_data.to_dict()
                cases.append(cases_item)

        else_: dict[str, Any] | Unset
        if isinstance(self.else_, Unset):
            else_ = UNSET
        elif isinstance(self.else_, RowsExpressionField):
            else_ = self.else_.to_dict()
        elif isinstance(self.else_, RowsExpressionValue):
            else_ = self.else_.to_dict()
        else:
            else_ = self.else_.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "op": op,
            }
        )
        if args is not UNSET:
            field_dict["args"] = args
        if to is not UNSET:
            field_dict["to"] = to
        if path is not UNSET:
            field_dict["path"] = path
        if as_text is not UNSET:
            field_dict["as_text"] = as_text
        if cases is not UNSET:
            field_dict["cases"] = cases
        if else_ is not UNSET:
            field_dict["else"] = else_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_case_branch import RowsExpressionCaseBranch
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue

        d = dict(src_dict)
        op = RowsExpressionOperatorOp(d.pop("op"))

        _args = d.pop("args", UNSET)
        args: list[RowsExpressionField | RowsExpressionOperator | RowsExpressionValue] | Unset = UNSET
        if _args is not UNSET:
            args = []
            for args_item_data in _args:

                def _parse_args_item(
                    data: object,
                ) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue:
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

                args_item = _parse_args_item(args_item_data)

                args.append(args_item)

        _to = d.pop("to", UNSET)
        to: RowsExpressionOperatorTo | Unset
        if isinstance(_to, Unset):
            to = UNSET
        else:
            to = RowsExpressionOperatorTo(_to)

        path = d.pop("path", UNSET)

        as_text = d.pop("as_text", UNSET)

        _cases = d.pop("cases", UNSET)
        cases: list[RowsExpressionCaseBranch] | Unset = UNSET
        if _cases is not UNSET:
            cases = []
            for cases_item_data in _cases:
                cases_item = RowsExpressionCaseBranch.from_dict(cases_item_data)

                cases.append(cases_item)

        def _parse_else_(data: object) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset:
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

        else_ = _parse_else_(d.pop("else", UNSET))

        rows_expression_operator = cls(
            op=op,
            args=args,
            to=to,
            path=path,
            as_text=as_text,
            cases=cases,
            else_=else_,
        )

        return rows_expression_operator
