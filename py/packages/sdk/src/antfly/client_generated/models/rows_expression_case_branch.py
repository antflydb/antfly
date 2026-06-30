from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue


T = TypeVar("T", bound="RowsExpressionCaseBranch")


@_attrs_define
class RowsExpressionCaseBranch:
    """
    Attributes:
        when (RowsExpressionCondition): Computed expression predicate over the shared row-expression AST.
        then (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue): Shared typed row-expression AST. A
            node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
    """

    when: RowsExpressionCondition
    then: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue

        when = self.when.to_dict()

        then: dict[str, Any]
        if isinstance(self.then, RowsExpressionField):
            then = self.then.to_dict()
        elif isinstance(self.then, RowsExpressionValue):
            then = self.then.to_dict()
        else:
            then = self.then.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "when": when,
                "then": then,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_value import RowsExpressionValue

        d = dict(src_dict)
        when = RowsExpressionCondition.from_dict(d.pop("when"))

        def _parse_then(data: object) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue:
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

        then = _parse_then(d.pop("then"))

        rows_expression_case_branch = cls(
            when=when,
            then=then,
        )

        return rows_expression_case_branch
