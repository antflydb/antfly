from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_unique_predicate_group import RowsUniquePredicateGroup


T = TypeVar("T", bound="RowsConflictUniqueTarget")


@_attrs_define
class RowsConflictUniqueTarget:
    """Declared unique constraint target for `ON CONFLICT`.

    Attributes:
        name (str): Unique constraint name.
        where (RowsUniquePredicateGroup | Unset): Conjunction of partial-unique predicate atoms.
        where_expressions (list[RowsExpressionCondition] | Unset): Expression predicates for expression-partial unique
            conflict targets.
            When present, the list must exactly match the named unique
            constraint's stored expression predicate metadata.
    """

    name: str
    where: RowsUniquePredicateGroup | Unset = UNSET
    where_expressions: list[RowsExpressionCondition] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        where: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = self.where.to_dict()

        where_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.where_expressions, Unset):
            where_expressions = []
            for where_expressions_item_data in self.where_expressions:
                where_expressions_item = where_expressions_item_data.to_dict()
                where_expressions.append(where_expressions_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
            }
        )
        if where is not UNSET:
            field_dict["where"] = where
        if where_expressions is not UNSET:
            field_dict["where_expressions"] = where_expressions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_unique_predicate_group import RowsUniquePredicateGroup

        d = dict(src_dict)
        name = d.pop("name")

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

        rows_conflict_unique_target = cls(
            name=name,
            where=where,
            where_expressions=where_expressions,
        )

        return rows_conflict_unique_target
