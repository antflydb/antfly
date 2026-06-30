from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.rows_insert_source_request_op import RowsInsertSourceRequestOp
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_projection import RowsExpressionProjection
    from ..models.rows_insert_source_assignment import RowsInsertSourceAssignment
    from ..models.rows_on_conflict import RowsOnConflict
    from ..models.rows_query_request import RowsQueryRequest


T = TypeVar("T", bound="RowsInsertSourceRequest")


@_attrs_define
class RowsInsertSourceRequest:
    """Typed relational insert-source plan. The `source` is a read-only row
    query over `source_table` or, when omitted, the target table named in the
    path. Each selected source row is projected through `assignments` into a
    target insert row, then optional conflict handling and `RETURNING`
    projection run through the same row-batch semantics as ordinary
    inserts. Execution is fail-closed until the storage/runtime layer
    implements source-to-target routing, duplicate-target detection, and
    owner-local insert staging for this native plan.

        Attributes:
            op (RowsInsertSourceRequestOp):
            source (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
                Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
                native request shape.
            assignments (list[RowsInsertSourceAssignment]): Ordered target-field assignments used to build each proposed
                insert row from the source row.
            source_table (str | Unset): Optional source table name. Omit or set to the target table for same-table insert-
                source plans; cross-table execution requires routed source/target ownership support.
            on_conflict (RowsOnConflict | Unset): Typed conflict action for insert operations. `nothing` skips the insert
                when the target already exists. `update` applies the same typed update
                operators as ordinary row updates, with expression sources allowed to
                reference `existing` and `proposed` row images.
            returning (list[str] | Unset): Fields to return from the committed inserted or conflict-updated row image. `*`
                returns the full row.
            returning_expressions (list[RowsExpressionProjection] | Unset): Typed row-expression projections over the
                committed inserted or conflict-updated row image.
    """

    op: RowsInsertSourceRequestOp
    source: RowsQueryRequest
    assignments: list[RowsInsertSourceAssignment]
    source_table: str | Unset = UNSET
    on_conflict: RowsOnConflict | Unset = UNSET
    returning: list[str] | Unset = UNSET
    returning_expressions: list[RowsExpressionProjection] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        op = self.op.value

        source = self.source.to_dict()

        assignments = []
        for assignments_item_data in self.assignments:
            assignments_item = assignments_item_data.to_dict()
            assignments.append(assignments_item)

        source_table = self.source_table

        on_conflict: dict[str, Any] | Unset = UNSET
        if not isinstance(self.on_conflict, Unset):
            on_conflict = self.on_conflict.to_dict()

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
                "source": source,
                "assignments": assignments,
            }
        )
        if source_table is not UNSET:
            field_dict["source_table"] = source_table
        if on_conflict is not UNSET:
            field_dict["on_conflict"] = on_conflict
        if returning is not UNSET:
            field_dict["returning"] = returning
        if returning_expressions is not UNSET:
            field_dict["returning_expressions"] = returning_expressions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_projection import RowsExpressionProjection
        from ..models.rows_insert_source_assignment import RowsInsertSourceAssignment
        from ..models.rows_on_conflict import RowsOnConflict
        from ..models.rows_query_request import RowsQueryRequest

        d = dict(src_dict)
        op = RowsInsertSourceRequestOp(d.pop("op"))

        source = RowsQueryRequest.from_dict(d.pop("source"))

        assignments = []
        _assignments = d.pop("assignments")
        for assignments_item_data in _assignments:
            assignments_item = RowsInsertSourceAssignment.from_dict(assignments_item_data)

            assignments.append(assignments_item)

        source_table = d.pop("source_table", UNSET)

        _on_conflict = d.pop("on_conflict", UNSET)
        on_conflict: RowsOnConflict | Unset
        if isinstance(_on_conflict, Unset):
            on_conflict = UNSET
        else:
            on_conflict = RowsOnConflict.from_dict(_on_conflict)

        returning = cast(list[str], d.pop("returning", UNSET))

        _returning_expressions = d.pop("returning_expressions", UNSET)
        returning_expressions: list[RowsExpressionProjection] | Unset = UNSET
        if _returning_expressions is not UNSET:
            returning_expressions = []
            for returning_expressions_item_data in _returning_expressions:
                returning_expressions_item = RowsExpressionProjection.from_dict(returning_expressions_item_data)

                returning_expressions.append(returning_expressions_item)

        rows_insert_source_request = cls(
            op=op,
            source=source,
            assignments=assignments,
            source_table=source_table,
            on_conflict=on_conflict,
            returning=returning,
            returning_expressions=returning_expressions,
        )

        return rows_insert_source_request
