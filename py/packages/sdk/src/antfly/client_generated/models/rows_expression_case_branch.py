from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.rows_expression import RowsExpression
    from ..models.rows_expression_condition import RowsExpressionCondition


T = TypeVar("T", bound="RowsExpressionCaseBranch")


@_attrs_define
class RowsExpressionCaseBranch:
    """
    Attributes:
        when (RowsExpressionCondition): Computed expression predicate over the shared row-expression AST.
        then (RowsExpression): Shared typed row-expression AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported operators
            include `now`, `coalesce`, `lower`, `upper`, `concat`, `nullif`, numeric
            `add`/`sub`/`mul`/`div`, `cast`, `json_extract`, `array_length`,
            `string_to_array`, and searched `case` with `cases` and `else`.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
    """

    when: RowsExpressionCondition
    then: RowsExpression
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        when = self.when.to_dict()

        then = self.then.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "when": when,
                "then": then,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression import RowsExpression
        from ..models.rows_expression_condition import RowsExpressionCondition

        d = dict(src_dict)
        when = RowsExpressionCondition.from_dict(d.pop("when"))

        then = RowsExpression.from_dict(d.pop("then"))

        rows_expression_case_branch = cls(
            when=when,
            then=then,
        )

        rows_expression_case_branch.additional_properties = d
        return rows_expression_case_branch

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
