from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.rows_expression_op import RowsExpressionOp
from ..models.rows_expression_source import RowsExpressionSource
from ..models.rows_expression_to import RowsExpressionTo
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_case_branch import RowsExpressionCaseBranch


T = TypeVar("T", bound="RowsExpression")


@_attrs_define
class RowsExpression:
    """Shared typed row-expression AST. A node is exactly one of `{ "field": "name" }`,
    `{ "value": ... }`, or an operator node such as
    `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported operators
    include `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, numeric
    `add`/`sub`/`mul`/`div`, `cast`, `json_extract`, `array_length`,
    `string_to_array`, and searched `case` with `cases` and `else`.
    Mutation expressions may set `source` to `existing` or `proposed`; query
    expressions use the default row source.

        Attributes:
            field (str | Unset):
            source (RowsExpressionSource | Unset):
            value (Any | Unset): Literal JSON value for a value node.
            op (RowsExpressionOp | Unset):
            args (list[RowsExpression] | Unset): Operand expressions for operator nodes.
            to (RowsExpressionTo | Unset): Cast target for `cast`.
            path (Any | Unset): Structured JSON path for `json_extract`.
            as_text (bool | Unset): Return JSON path extraction as text.
            cases (list[RowsExpressionCaseBranch] | Unset): Searched case branches, each with `when` and `then`.
            else_ (RowsExpression | Unset): Shared typed row-expression AST. A node is exactly one of `{ "field": "name" }`,
                `{ "value": ... }`, or an operator node such as
                `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported operators
                include `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, numeric
                `add`/`sub`/`mul`/`div`, `cast`, `json_extract`, `array_length`,
                `string_to_array`, and searched `case` with `cases` and `else`.
                Mutation expressions may set `source` to `existing` or `proposed`; query
                expressions use the default row source.
    """

    field: str | Unset = UNSET
    source: RowsExpressionSource | Unset = UNSET
    value: Any | Unset = UNSET
    op: RowsExpressionOp | Unset = UNSET
    args: list[RowsExpression] | Unset = UNSET
    to: RowsExpressionTo | Unset = UNSET
    path: Any | Unset = UNSET
    as_text: bool | Unset = UNSET
    cases: list[RowsExpressionCaseBranch] | Unset = UNSET
    else_: RowsExpression | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        source: str | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.value

        value = self.value

        op: str | Unset = UNSET
        if not isinstance(self.op, Unset):
            op = self.op.value

        args: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.args, Unset):
            args = []
            for args_item_data in self.args:
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

        else_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.else_, Unset):
            else_ = self.else_.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if field is not UNSET:
            field_dict["field"] = field
        if source is not UNSET:
            field_dict["source"] = source
        if value is not UNSET:
            field_dict["value"] = value
        if op is not UNSET:
            field_dict["op"] = op
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

        d = dict(src_dict)
        field = d.pop("field", UNSET)

        _source = d.pop("source", UNSET)
        source: RowsExpressionSource | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = RowsExpressionSource(_source)

        value = d.pop("value", UNSET)

        _op = d.pop("op", UNSET)
        op: RowsExpressionOp | Unset
        if isinstance(_op, Unset):
            op = UNSET
        else:
            op = RowsExpressionOp(_op)

        _args = d.pop("args", UNSET)
        args: list[RowsExpression] | Unset = UNSET
        if _args is not UNSET:
            args = []
            for args_item_data in _args:
                args_item = RowsExpression.from_dict(args_item_data)

                args.append(args_item)

        _to = d.pop("to", UNSET)
        to: RowsExpressionTo | Unset
        if isinstance(_to, Unset):
            to = UNSET
        else:
            to = RowsExpressionTo(_to)

        path = d.pop("path", UNSET)

        as_text = d.pop("as_text", UNSET)

        _cases = d.pop("cases", UNSET)
        cases: list[RowsExpressionCaseBranch] | Unset = UNSET
        if _cases is not UNSET:
            cases = []
            for cases_item_data in _cases:
                cases_item = RowsExpressionCaseBranch.from_dict(cases_item_data)

                cases.append(cases_item)

        _else_ = d.pop("else", UNSET)
        else_: RowsExpression | Unset
        if isinstance(_else_, Unset):
            else_ = UNSET
        else:
            else_ = RowsExpression.from_dict(_else_)

        rows_expression = cls(
            field=field,
            source=source,
            value=value,
            op=op,
            args=args,
            to=to,
            path=path,
            as_text=as_text,
            cases=cases,
            else_=else_,
        )

        rows_expression.additional_properties = d
        return rows_expression

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
