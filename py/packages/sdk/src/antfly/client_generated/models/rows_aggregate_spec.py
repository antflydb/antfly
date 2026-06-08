from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_aggregate_spec_filter import RowsAggregateSpecFilter
    from ..models.rows_expression import RowsExpression
    from ..models.rows_expression_condition import RowsExpressionCondition


T = TypeVar("T", bound="RowsAggregateSpec")


@_attrs_define
class RowsAggregateSpec:
    """
    Attributes:
        name (str):
        op (str):
        field (str | Unset):
        expression (RowsExpression | Unset): Shared typed row-expression AST. A node is exactly one of `{ "field":
            "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported operators
            include `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, numeric
            `add`/`sub`/`mul`/`div`, `cast`, `json_extract`, `array_length`,
            `string_to_array`, and searched `case` with `cases` and `else`.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
        distinct (bool | Unset):
        filter_ (RowsAggregateSpecFilter | Unset):
        filter_expressions (list[RowsExpressionCondition] | Unset):
    """

    name: str
    op: str
    field: str | Unset = UNSET
    expression: RowsExpression | Unset = UNSET
    distinct: bool | Unset = UNSET
    filter_: RowsAggregateSpecFilter | Unset = UNSET
    filter_expressions: list[RowsExpressionCondition] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        op = self.op

        field = self.field

        expression: dict[str, Any] | Unset = UNSET
        if not isinstance(self.expression, Unset):
            expression = self.expression.to_dict()

        distinct = self.distinct

        filter_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filter_, Unset):
            filter_ = self.filter_.to_dict()

        filter_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_expressions, Unset):
            filter_expressions = []
            for filter_expressions_item_data in self.filter_expressions:
                filter_expressions_item = filter_expressions_item_data.to_dict()
                filter_expressions.append(filter_expressions_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "op": op,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if expression is not UNSET:
            field_dict["expression"] = expression
        if distinct is not UNSET:
            field_dict["distinct"] = distinct
        if filter_ is not UNSET:
            field_dict["filter"] = filter_
        if filter_expressions is not UNSET:
            field_dict["filter_expressions"] = filter_expressions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_aggregate_spec_filter import RowsAggregateSpecFilter
        from ..models.rows_expression import RowsExpression
        from ..models.rows_expression_condition import RowsExpressionCondition

        d = dict(src_dict)
        name = d.pop("name")

        op = d.pop("op")

        field = d.pop("field", UNSET)

        _expression = d.pop("expression", UNSET)
        expression: RowsExpression | Unset
        if isinstance(_expression, Unset):
            expression = UNSET
        else:
            expression = RowsExpression.from_dict(_expression)

        distinct = d.pop("distinct", UNSET)

        _filter_ = d.pop("filter", UNSET)
        filter_: RowsAggregateSpecFilter | Unset
        if isinstance(_filter_, Unset):
            filter_ = UNSET
        else:
            filter_ = RowsAggregateSpecFilter.from_dict(_filter_)

        _filter_expressions = d.pop("filter_expressions", UNSET)
        filter_expressions: list[RowsExpressionCondition] | Unset = UNSET
        if _filter_expressions is not UNSET:
            filter_expressions = []
            for filter_expressions_item_data in _filter_expressions:
                filter_expressions_item = RowsExpressionCondition.from_dict(filter_expressions_item_data)

                filter_expressions.append(filter_expressions_item)

        rows_aggregate_spec = cls(
            name=name,
            op=op,
            field=field,
            expression=expression,
            distinct=distinct,
            filter_=filter_,
            filter_expressions=filter_expressions,
        )

        rows_aggregate_spec.additional_properties = d
        return rows_aggregate_spec

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
