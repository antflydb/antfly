from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.unique_constraint_validation_state import UniqueConstraintValidationState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_unique_predicate_group import RowsUniquePredicateGroup
    from ..models.unique_constraint_expressions_item import UniqueConstraintExpressionsItem


T = TypeVar("T", bound="UniqueConstraint")


@_attrs_define
class UniqueConstraint:
    """Relational unique constraint.

    Attributes:
        name (str | Unset): Constraint name, unique within the table schema.
        columns (list[str] | Unset): Unique columns. One or more ordered non-json relational columns are supported.
        expressions (list[UniqueConstraintExpressionsItem] | Unset): Stable expression keys supported by unique-owner
            maintenance.
        without_overlaps_period (str | Unset): Application-time period name for `WITHOUT OVERLAPS` temporal uniqueness.
        where (RowsUniquePredicateGroup | Unset): Conjunction of partial-unique predicate atoms.
        where_expressions (list[RowsExpressionCondition] | Unset): Deterministic row-expression predicates that decide
            whether a row participates in this unique constraint.
        validation_state (UniqueConstraintValidationState | Unset): Unique validation state. Unvalidated constraints are
            durable metadata but do not enforce writes until promoted.
    """

    name: str | Unset = UNSET
    columns: list[str] | Unset = UNSET
    expressions: list[UniqueConstraintExpressionsItem] | Unset = UNSET
    without_overlaps_period: str | Unset = UNSET
    where: RowsUniquePredicateGroup | Unset = UNSET
    where_expressions: list[RowsExpressionCondition] | Unset = UNSET
    validation_state: UniqueConstraintValidationState | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.expressions, Unset):
            expressions = []
            for expressions_item_data in self.expressions:
                expressions_item = expressions_item_data.to_dict()
                expressions.append(expressions_item)

        without_overlaps_period = self.without_overlaps_period

        where: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = self.where.to_dict()

        where_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.where_expressions, Unset):
            where_expressions = []
            for where_expressions_item_data in self.where_expressions:
                where_expressions_item = where_expressions_item_data.to_dict()
                where_expressions.append(where_expressions_item)

        validation_state: str | Unset = UNSET
        if not isinstance(self.validation_state, Unset):
            validation_state = self.validation_state.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if columns is not UNSET:
            field_dict["columns"] = columns
        if expressions is not UNSET:
            field_dict["expressions"] = expressions
        if without_overlaps_period is not UNSET:
            field_dict["without_overlaps_period"] = without_overlaps_period
        if where is not UNSET:
            field_dict["where"] = where
        if where_expressions is not UNSET:
            field_dict["where_expressions"] = where_expressions
        if validation_state is not UNSET:
            field_dict["validation_state"] = validation_state

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_unique_predicate_group import RowsUniquePredicateGroup
        from ..models.unique_constraint_expressions_item import UniqueConstraintExpressionsItem

        d = dict(src_dict)
        name = d.pop("name", UNSET)

        columns = cast(list[str], d.pop("columns", UNSET))

        _expressions = d.pop("expressions", UNSET)
        expressions: list[UniqueConstraintExpressionsItem] | Unset = UNSET
        if _expressions is not UNSET:
            expressions = []
            for expressions_item_data in _expressions:
                expressions_item = UniqueConstraintExpressionsItem.from_dict(expressions_item_data)

                expressions.append(expressions_item)

        without_overlaps_period = d.pop("without_overlaps_period", UNSET)

        _where = d.pop("where", UNSET)
        where: RowsUniquePredicateGroup | Unset
        if isinstance(_where, Unset):
            where = UNSET
        else:
            where = RowsUniquePredicateGroup.from_dict(_where)

        _where_expressions = d.pop("where_expressions", UNSET)
        where_expressions: list[RowsExpressionCondition] | Unset = UNSET
        if _where_expressions is not UNSET:
            where_expressions = []
            for where_expressions_item_data in _where_expressions:
                where_expressions_item = RowsExpressionCondition.from_dict(where_expressions_item_data)

                where_expressions.append(where_expressions_item)

        _validation_state = d.pop("validation_state", UNSET)
        validation_state: UniqueConstraintValidationState | Unset
        if isinstance(_validation_state, Unset):
            validation_state = UNSET
        else:
            validation_state = UniqueConstraintValidationState(_validation_state)

        unique_constraint = cls(
            name=name,
            columns=columns,
            expressions=expressions,
            without_overlaps_period=without_overlaps_period,
            where=where,
            where_expressions=where_expressions,
            validation_state=validation_state,
        )

        return unique_constraint
