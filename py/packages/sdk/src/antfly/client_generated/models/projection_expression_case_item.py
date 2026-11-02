from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.projection_expression import ProjectionExpression


T = TypeVar("T", bound="ProjectionExpressionCaseItem")


@_attrs_define
class ProjectionExpressionCaseItem:
    """
    Attributes:
        when (ProjectionExpression): Exactly one expression node: literal, field, ref, param, call with args,
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
        then (ProjectionExpression): Exactly one expression node: literal, field, ref, param, call with args,
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

    when: ProjectionExpression
    then: ProjectionExpression

    def to_dict(self) -> dict[str, Any]:
        when = self.when.to_dict()

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
        from ..models.projection_expression import ProjectionExpression

        d = dict(src_dict)
        when = ProjectionExpression.from_dict(d.pop("when"))

        then = ProjectionExpression.from_dict(d.pop("then"))

        projection_expression_case_item = cls(
            when=when,
            then=then,
        )

        return projection_expression_case_item
