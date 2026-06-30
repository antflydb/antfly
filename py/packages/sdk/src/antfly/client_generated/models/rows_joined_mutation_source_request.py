from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.rows_joined_mutation_source_request_op import RowsJoinedMutationSourceRequestOp
from ..models.rows_joined_mutation_source_request_target_side import RowsJoinedMutationSourceRequestTargetSide
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
    from ..models.rows_expression_assignment_map import RowsExpressionAssignmentMap
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
    from ..models.rows_expression_projection import RowsExpressionProjection
    from ..models.rows_field_patch import RowsFieldPatch
    from ..models.rows_join_request import RowsJoinRequest
    from ..models.rows_joined_mutation_source_assignment import RowsJoinedMutationSourceAssignment
    from ..models.rows_numeric_increment import RowsNumericIncrement


T = TypeVar("T", bound="RowsJoinedMutationSourceRequest")


@_attrs_define
class RowsJoinedMutationSourceRequest:
    """Typed relational joined mutation-source plan. The target side of the
    `join` must carry a lockable `row_claim.transaction_id`; the non-target
    side is read-only input. Update plans can copy same-typed values from
    the source side through `source_assignments` and can apply target-local
    patches or expression assignments. Delete plans reject update
    assignments. Execution must stage intents only for claimed target rows.

        Attributes:
            op (RowsJoinedMutationSourceRequestOp):
            target_side (RowsJoinedMutationSourceRequestTargetSide):
            join (RowsJoinRequest): Typed equality join plan. Each side is a full row-query request and can read an ordered
                CTE through `source_cte`.
            source_table (str | Unset): Optional source table name for cross-table joined mutation-source plans. Omit or set
                to the target table for same-table joined mutation sources. Catalog-routed execution reads source rows through
                the source table's owner ranges and stages only target-row intents through the target table's owner ranges.
            match_expression_where (list[RowsExpressionCondition] | Unset): Post-match computed predicates over the target
                row and joined source row. Unqualified fields bind to the target row; fields with `source: source` bind to the
                source row.
            match_expression_any (list[RowsExpressionConditionGroup] | Unset): OR groups of post-match computed predicates
                over the target row and joined source row.
            match_expression_not (list[RowsExpressionConditionGroup] | Unset): NOT groups of post-match computed predicates
                over the target row and joined source row.
            match_expression_array_contains (list[RowsExpressionArrayContainsPredicate] | Unset): Post-match computed array-
                containment predicates over the target row and joined source row.
            source_assignments (list[RowsJoinedMutationSourceAssignment] | Unset): Source-side assignments that copy values
                from the read-only joined source side into the claimed target side.
            patch (RowsFieldPatch | Unset): Static field patch for top-level relational columns. Primary-key fields
                are rejected by the server.
            increment (RowsNumericIncrement | Unset): Numeric increment map keyed by declared numeric columns.
            patch_expr (RowsExpressionAssignmentMap | Unset): Field-to-expression assignment map over the shared row-
                expression AST.
            increment_expr (RowsExpressionAssignmentMap | Unset): Field-to-expression assignment map over the shared row-
                expression AST.
            returning (list[str] | Unset): Fields to return from the final target update image or deleted target row image.
                `*` returns the full row.
            returning_expressions (list[RowsExpressionProjection] | Unset): Typed row-expression projections over the final
                target update image or deleted target row image.
    """

    op: RowsJoinedMutationSourceRequestOp
    target_side: RowsJoinedMutationSourceRequestTargetSide
    join: RowsJoinRequest
    source_table: str | Unset = UNSET
    match_expression_where: list[RowsExpressionCondition] | Unset = UNSET
    match_expression_any: list[RowsExpressionConditionGroup] | Unset = UNSET
    match_expression_not: list[RowsExpressionConditionGroup] | Unset = UNSET
    match_expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
    source_assignments: list[RowsJoinedMutationSourceAssignment] | Unset = UNSET
    patch: RowsFieldPatch | Unset = UNSET
    increment: RowsNumericIncrement | Unset = UNSET
    patch_expr: RowsExpressionAssignmentMap | Unset = UNSET
    increment_expr: RowsExpressionAssignmentMap | Unset = UNSET
    returning: list[str] | Unset = UNSET
    returning_expressions: list[RowsExpressionProjection] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        op = self.op.value

        target_side = self.target_side.value

        join = self.join.to_dict()

        source_table = self.source_table

        match_expression_where: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.match_expression_where, Unset):
            match_expression_where = []
            for match_expression_where_item_data in self.match_expression_where:
                match_expression_where_item = match_expression_where_item_data.to_dict()
                match_expression_where.append(match_expression_where_item)

        match_expression_any: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.match_expression_any, Unset):
            match_expression_any = []
            for match_expression_any_item_data in self.match_expression_any:
                match_expression_any_item = match_expression_any_item_data.to_dict()
                match_expression_any.append(match_expression_any_item)

        match_expression_not: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.match_expression_not, Unset):
            match_expression_not = []
            for match_expression_not_item_data in self.match_expression_not:
                match_expression_not_item = match_expression_not_item_data.to_dict()
                match_expression_not.append(match_expression_not_item)

        match_expression_array_contains: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.match_expression_array_contains, Unset):
            match_expression_array_contains = []
            for match_expression_array_contains_item_data in self.match_expression_array_contains:
                match_expression_array_contains_item = match_expression_array_contains_item_data.to_dict()
                match_expression_array_contains.append(match_expression_array_contains_item)

        source_assignments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.source_assignments, Unset):
            source_assignments = []
            for source_assignments_item_data in self.source_assignments:
                source_assignments_item = source_assignments_item_data.to_dict()
                source_assignments.append(source_assignments_item)

        patch: dict[str, Any] | Unset = UNSET
        if not isinstance(self.patch, Unset):
            patch = self.patch.to_dict()

        increment: dict[str, Any] | Unset = UNSET
        if not isinstance(self.increment, Unset):
            increment = self.increment.to_dict()

        patch_expr: dict[str, Any] | Unset = UNSET
        if not isinstance(self.patch_expr, Unset):
            patch_expr = self.patch_expr.to_dict()

        increment_expr: dict[str, Any] | Unset = UNSET
        if not isinstance(self.increment_expr, Unset):
            increment_expr = self.increment_expr.to_dict()

        returning: list[str] | Unset = UNSET
        if not isinstance(self.returning, Unset):
            returning = self.returning

        returning_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.returning_expressions, Unset):
            returning_expressions = []
            for returning_expressions_item_data in self.returning_expressions:
                returning_expressions_item = returning_expressions_item_data.to_dict()
                returning_expressions.append(returning_expressions_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "op": op,
                "target_side": target_side,
                "join": join,
            }
        )
        if source_table is not UNSET:
            field_dict["source_table"] = source_table
        if match_expression_where is not UNSET:
            field_dict["match_expression_where"] = match_expression_where
        if match_expression_any is not UNSET:
            field_dict["match_expression_any"] = match_expression_any
        if match_expression_not is not UNSET:
            field_dict["match_expression_not"] = match_expression_not
        if match_expression_array_contains is not UNSET:
            field_dict["match_expression_array_contains"] = match_expression_array_contains
        if source_assignments is not UNSET:
            field_dict["source_assignments"] = source_assignments
        if patch is not UNSET:
            field_dict["patch"] = patch
        if increment is not UNSET:
            field_dict["increment"] = increment
        if patch_expr is not UNSET:
            field_dict["patch_expr"] = patch_expr
        if increment_expr is not UNSET:
            field_dict["increment_expr"] = increment_expr
        if returning is not UNSET:
            field_dict["returning"] = returning
        if returning_expressions is not UNSET:
            field_dict["returning_expressions"] = returning_expressions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
        from ..models.rows_expression_assignment_map import RowsExpressionAssignmentMap
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
        from ..models.rows_expression_projection import RowsExpressionProjection
        from ..models.rows_field_patch import RowsFieldPatch
        from ..models.rows_join_request import RowsJoinRequest
        from ..models.rows_joined_mutation_source_assignment import RowsJoinedMutationSourceAssignment
        from ..models.rows_numeric_increment import RowsNumericIncrement

        d = dict(src_dict)
        op = RowsJoinedMutationSourceRequestOp(d.pop("op"))

        target_side = RowsJoinedMutationSourceRequestTargetSide(d.pop("target_side"))

        join = RowsJoinRequest.from_dict(d.pop("join"))

        source_table = d.pop("source_table", UNSET)

        _match_expression_where = d.pop("match_expression_where", UNSET)
        match_expression_where: list[RowsExpressionCondition] | Unset = UNSET
        if _match_expression_where is not UNSET:
            match_expression_where = []
            for match_expression_where_item_data in _match_expression_where:
                match_expression_where_item = RowsExpressionCondition.from_dict(match_expression_where_item_data)

                match_expression_where.append(match_expression_where_item)

        _match_expression_any = d.pop("match_expression_any", UNSET)
        match_expression_any: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _match_expression_any is not UNSET:
            match_expression_any = []
            for match_expression_any_item_data in _match_expression_any:
                match_expression_any_item = RowsExpressionConditionGroup.from_dict(match_expression_any_item_data)

                match_expression_any.append(match_expression_any_item)

        _match_expression_not = d.pop("match_expression_not", UNSET)
        match_expression_not: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _match_expression_not is not UNSET:
            match_expression_not = []
            for match_expression_not_item_data in _match_expression_not:
                match_expression_not_item = RowsExpressionConditionGroup.from_dict(match_expression_not_item_data)

                match_expression_not.append(match_expression_not_item)

        _match_expression_array_contains = d.pop("match_expression_array_contains", UNSET)
        match_expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
        if _match_expression_array_contains is not UNSET:
            match_expression_array_contains = []
            for match_expression_array_contains_item_data in _match_expression_array_contains:
                match_expression_array_contains_item = RowsExpressionArrayContainsPredicate.from_dict(
                    match_expression_array_contains_item_data
                )

                match_expression_array_contains.append(match_expression_array_contains_item)

        _source_assignments = d.pop("source_assignments", UNSET)
        source_assignments: list[RowsJoinedMutationSourceAssignment] | Unset = UNSET
        if _source_assignments is not UNSET:
            source_assignments = []
            for source_assignments_item_data in _source_assignments:
                source_assignments_item = RowsJoinedMutationSourceAssignment.from_dict(source_assignments_item_data)

                source_assignments.append(source_assignments_item)

        _patch = d.pop("patch", UNSET)
        patch: RowsFieldPatch | Unset
        if isinstance(_patch, Unset):
            patch = UNSET
        else:
            patch = RowsFieldPatch.from_dict(_patch)

        _increment = d.pop("increment", UNSET)
        increment: RowsNumericIncrement | Unset
        if isinstance(_increment, Unset):
            increment = UNSET
        else:
            increment = RowsNumericIncrement.from_dict(_increment)

        _patch_expr = d.pop("patch_expr", UNSET)
        patch_expr: RowsExpressionAssignmentMap | Unset
        if isinstance(_patch_expr, Unset):
            patch_expr = UNSET
        else:
            patch_expr = RowsExpressionAssignmentMap.from_dict(_patch_expr)

        _increment_expr = d.pop("increment_expr", UNSET)
        increment_expr: RowsExpressionAssignmentMap | Unset
        if isinstance(_increment_expr, Unset):
            increment_expr = UNSET
        else:
            increment_expr = RowsExpressionAssignmentMap.from_dict(_increment_expr)

        returning = cast(list[str], d.pop("returning", UNSET))

        _returning_expressions = d.pop("returning_expressions", UNSET)
        returning_expressions: list[RowsExpressionProjection] | Unset = UNSET
        if _returning_expressions is not UNSET:
            returning_expressions = []
            for returning_expressions_item_data in _returning_expressions:
                returning_expressions_item = RowsExpressionProjection.from_dict(returning_expressions_item_data)

                returning_expressions.append(returning_expressions_item)

        rows_joined_mutation_source_request = cls(
            op=op,
            target_side=target_side,
            join=join,
            source_table=source_table,
            match_expression_where=match_expression_where,
            match_expression_any=match_expression_any,
            match_expression_not=match_expression_not,
            match_expression_array_contains=match_expression_array_contains,
            source_assignments=source_assignments,
            patch=patch,
            increment=increment,
            patch_expr=patch_expr,
            increment_expr=increment_expr,
            returning=returning,
            returning_expressions=returning_expressions,
        )

        return rows_joined_mutation_source_request
