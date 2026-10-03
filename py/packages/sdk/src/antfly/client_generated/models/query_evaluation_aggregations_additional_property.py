from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.query_evaluation_aggregations_additional_property_type import (
    QueryEvaluationAggregationsAdditionalPropertyType,
)

if TYPE_CHECKING:
    from ..models.query_expression import QueryExpression


T = TypeVar("T", bound="QueryEvaluationAggregationsAdditionalProperty")


@_attrs_define
class QueryEvaluationAggregationsAdditionalProperty:
    """
    Attributes:
        type_ (QueryEvaluationAggregationsAdditionalPropertyType):
        expression (QueryExpression): Exactly one of literal, field, ref, or call. A call requires input and
            decider. ai_decide requires questions; ai_probability requires statement;
            ai_choice and ai_score require instructions and criteria. Named refs may
            select nested JSON members with dotted paths. Binding cycles are invalid.
    """

    type_: QueryEvaluationAggregationsAdditionalPropertyType
    expression: QueryExpression

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        expression = self.expression.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "expression": expression,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_expression import QueryExpression

        d = dict(src_dict)
        type_ = QueryEvaluationAggregationsAdditionalPropertyType(d.pop("type"))

        expression = QueryExpression.from_dict(d.pop("expression"))

        query_evaluation_aggregations_additional_property = cls(
            type_=type_,
            expression=expression,
        )

        return query_evaluation_aggregations_additional_property
