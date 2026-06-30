from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.rows_mutation_source_request_op import RowsMutationSourceRequestOp
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_array_update_transform import RowsArrayUpdateTransform
    from ..models.rows_expression_assignment_map import RowsExpressionAssignmentMap
    from ..models.rows_expression_projection import RowsExpressionProjection
    from ..models.rows_field_patch import RowsFieldPatch
    from ..models.rows_json_set_transform import RowsJsonSetTransform
    from ..models.rows_numeric_increment import RowsNumericIncrement
    from ..models.rows_query_request import RowsQueryRequest
    from ..models.rows_temporal_portion import RowsTemporalPortion


T = TypeVar("T", bound="RowsMutationSourceRequest")


@_attrs_define
class RowsMutationSourceRequest:
    """Typed relational mutation-source plan. The `source` is a lockable base
    row-query request with `row_claim.transaction_id` and no
    `source_cte` or `doc_key_range`; update/delete intents are staged into
    that transaction using committed-version predicates from the selected
    preimages. Claims over physical ranges, CTEs, joins, aggregates,
    windows, and lateral outputs are rejected until those stages expose an
    explicit lockable base-row contract.

        Attributes:
            op (RowsMutationSourceRequestOp):
            source (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
                Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
                native request shape.
            patch (RowsFieldPatch | Unset): Static field patch for top-level relational columns. Primary-key fields
                are rejected by the server.
            increment (RowsNumericIncrement | Unset): Numeric increment map keyed by declared numeric columns.
            patch_expr (RowsExpressionAssignmentMap | Unset): Field-to-expression assignment map over the shared row-
                expression AST.
            increment_expr (RowsExpressionAssignmentMap | Unset): Field-to-expression assignment map over the shared row-
                expression AST.
            json_set (list[RowsJsonSetTransform] | Unset): JSON path transforms for update operations.
            array_update (list[RowsArrayUpdateTransform] | Unset): Array transforms for update operations.
            temporal_portion (RowsTemporalPortion | Unset): Application-time temporal slice for update/delete mutation-
                source plans.
            returning (list[str] | Unset): Fields to return from the final update image or deleted row image. `*` returns
                the full row.
            returning_expressions (list[RowsExpressionProjection] | Unset): Typed row-expression projections over the final
                update image or deleted row image.
    """

    op: RowsMutationSourceRequestOp
    source: RowsQueryRequest
    patch: RowsFieldPatch | Unset = UNSET
    increment: RowsNumericIncrement | Unset = UNSET
    patch_expr: RowsExpressionAssignmentMap | Unset = UNSET
    increment_expr: RowsExpressionAssignmentMap | Unset = UNSET
    json_set: list[RowsJsonSetTransform] | Unset = UNSET
    array_update: list[RowsArrayUpdateTransform] | Unset = UNSET
    temporal_portion: RowsTemporalPortion | Unset = UNSET
    returning: list[str] | Unset = UNSET
    returning_expressions: list[RowsExpressionProjection] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        op = self.op.value

        source = self.source.to_dict()

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

        json_set: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.json_set, Unset):
            json_set = []
            for json_set_item_data in self.json_set:
                json_set_item = json_set_item_data.to_dict()
                json_set.append(json_set_item)

        array_update: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.array_update, Unset):
            array_update = []
            for array_update_item_data in self.array_update:
                array_update_item = array_update_item_data.to_dict()
                array_update.append(array_update_item)

        temporal_portion: dict[str, Any] | Unset = UNSET
        if not isinstance(self.temporal_portion, Unset):
            temporal_portion = self.temporal_portion.to_dict()

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
            }
        )
        if patch is not UNSET:
            field_dict["patch"] = patch
        if increment is not UNSET:
            field_dict["increment"] = increment
        if patch_expr is not UNSET:
            field_dict["patch_expr"] = patch_expr
        if increment_expr is not UNSET:
            field_dict["increment_expr"] = increment_expr
        if json_set is not UNSET:
            field_dict["json_set"] = json_set
        if array_update is not UNSET:
            field_dict["array_update"] = array_update
        if temporal_portion is not UNSET:
            field_dict["temporal_portion"] = temporal_portion
        if returning is not UNSET:
            field_dict["returning"] = returning
        if returning_expressions is not UNSET:
            field_dict["returning_expressions"] = returning_expressions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_array_update_transform import RowsArrayUpdateTransform
        from ..models.rows_expression_assignment_map import RowsExpressionAssignmentMap
        from ..models.rows_expression_projection import RowsExpressionProjection
        from ..models.rows_field_patch import RowsFieldPatch
        from ..models.rows_json_set_transform import RowsJsonSetTransform
        from ..models.rows_numeric_increment import RowsNumericIncrement
        from ..models.rows_query_request import RowsQueryRequest
        from ..models.rows_temporal_portion import RowsTemporalPortion

        d = dict(src_dict)
        op = RowsMutationSourceRequestOp(d.pop("op"))

        source = RowsQueryRequest.from_dict(d.pop("source"))

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

        _json_set = d.pop("json_set", UNSET)
        json_set: list[RowsJsonSetTransform] | Unset = UNSET
        if _json_set is not UNSET:
            json_set = []
            for json_set_item_data in _json_set:
                json_set_item = RowsJsonSetTransform.from_dict(json_set_item_data)

                json_set.append(json_set_item)

        _array_update = d.pop("array_update", UNSET)
        array_update: list[RowsArrayUpdateTransform] | Unset = UNSET
        if _array_update is not UNSET:
            array_update = []
            for array_update_item_data in _array_update:
                array_update_item = RowsArrayUpdateTransform.from_dict(array_update_item_data)

                array_update.append(array_update_item)

        _temporal_portion = d.pop("temporal_portion", UNSET)
        temporal_portion: RowsTemporalPortion | Unset
        if isinstance(_temporal_portion, Unset):
            temporal_portion = UNSET
        else:
            temporal_portion = RowsTemporalPortion.from_dict(_temporal_portion)

        returning = cast(list[str], d.pop("returning", UNSET))

        _returning_expressions = d.pop("returning_expressions", UNSET)
        returning_expressions: list[RowsExpressionProjection] | Unset = UNSET
        if _returning_expressions is not UNSET:
            returning_expressions = []
            for returning_expressions_item_data in _returning_expressions:
                returning_expressions_item = RowsExpressionProjection.from_dict(returning_expressions_item_data)

                returning_expressions.append(returning_expressions_item)

        rows_mutation_source_request = cls(
            op=op,
            source=source,
            patch=patch,
            increment=increment,
            patch_expr=patch_expr,
            increment_expr=increment_expr,
            json_set=json_set,
            array_update=array_update,
            temporal_portion=temporal_portion,
            returning=returning,
            returning_expressions=returning_expressions,
        )

        return rows_mutation_source_request
