from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_expression import QueryExpression


T = TypeVar("T", bound="QueryEvaluationOrderByItem")


@_attrs_define
class QueryEvaluationOrderByItem:
    """
    Attributes:
        expression (QueryExpression): Exactly one of literal, field, ref, or call. A call requires input and
            decider. ai_decide requires questions; ai_probability requires statement;
            ai_choice and ai_score require instructions and criteria. Named refs may
            select nested JSON members with dotted paths. Binding cycles are invalid.
        descending (bool | Unset):
    """

    expression: QueryExpression
    descending: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        expression = self.expression.to_dict()

        descending = self.descending

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "expression": expression,
            }
        )
        if descending is not UNSET:
            field_dict["descending"] = descending

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_expression import QueryExpression

        d = dict(src_dict)
        expression = QueryExpression.from_dict(d.pop("expression"))

        descending = d.pop("descending", UNSET)

        query_evaluation_order_by_item = cls(
            expression=expression,
            descending=descending,
        )

        return query_evaluation_order_by_item
