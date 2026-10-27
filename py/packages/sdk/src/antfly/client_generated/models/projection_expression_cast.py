from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.projection_expression_cast_type import ProjectionExpressionCastType

if TYPE_CHECKING:
    from ..models.projection_expression import ProjectionExpression


T = TypeVar("T", bound="ProjectionExpressionCast")


@_attrs_define
class ProjectionExpressionCast:
    """
    Attributes:
        type_ (ProjectionExpressionCastType):
        expression (ProjectionExpression): Exactly one expression node: literal, field, ref, param, call with args,
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
    """

    type_: ProjectionExpressionCastType
    expression: ProjectionExpression

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
        from ..models.projection_expression import ProjectionExpression

        d = dict(src_dict)
        type_ = ProjectionExpressionCastType(d.pop("type"))

        expression = ProjectionExpression.from_dict(d.pop("expression"))

        projection_expression_cast = cls(
            type_=type_,
            expression=expression,
        )

        return projection_expression_cast
