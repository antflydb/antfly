from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.row_operation_op import RowOperationOp
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.row_selector import RowSelector
    from ..models.rows_array_update_transform import RowsArrayUpdateTransform
    from ..models.rows_expression_assignment_map import RowsExpressionAssignmentMap
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_expression_projection import RowsExpressionProjection
    from ..models.rows_field_patch import RowsFieldPatch
    from ..models.rows_json_set_transform import RowsJsonSetTransform
    from ..models.rows_numeric_increment import RowsNumericIncrement
    from ..models.rows_on_conflict import RowsOnConflict
    from ..models.rows_row_document import RowsRowDocument


T = TypeVar("T", bound="RowOperation")


@_attrs_define
class RowOperation:
    """Structured relational row mutation. `insert` fails if the primary
    identity already exists, `upsert` overwrites or creates, `update`
    applies a non-upsert patch by primary or unique identity, and `delete`
    removes by primary or unique identity. `update.patch` cannot change
    primary-key components. Missing unique selectors fail the write request
    rather than falling back to scans.

        Attributes:
            op (RowOperationOp):
            row (RowsRowDocument | Unset): Full relational row document. Keys are declared relational columns and
                values are JSON values coerced through the table schema before storage.
            where (RowSelector | Unset): Structured row selector. `primary` addresses declared primary-key
                tables directly. `unique` addresses a declared unique constraint through
                durable unique-owner rows.
            patch (RowsFieldPatch | Unset): Static field patch for top-level relational columns. Primary-key fields
                are rejected by the server.
            increment (RowsNumericIncrement | Unset): Numeric increment map keyed by declared numeric columns.
            patch_expr (RowsExpressionAssignmentMap | Unset): Field-to-expression assignment map over the shared row-
                expression AST.
            increment_expr (RowsExpressionAssignmentMap | Unset): Field-to-expression assignment map over the shared row-
                expression AST.
            json_set (list[RowsJsonSetTransform] | Unset):
            array_update (list[RowsArrayUpdateTransform] | Unset):
            on_conflict (RowsOnConflict | Unset): Typed conflict action for insert operations. `nothing` skips the insert
                when the target already exists. `update` applies the same typed update
                operators as ordinary row updates, with expression sources allowed to
                reference `existing` and `proposed` row images.
            where_expression (RowsExpressionCondition | Unset): Computed expression predicate over the shared row-expression
                AST.
            returning (list[str] | Unset): Fields to return from the committed mutation image. `*` returns the full row and
                cannot be combined with expression projections.
            returning_expressions (list[RowsExpressionProjection] | Unset): Typed row-expression projections from the
                committed mutation image.
            expected_version (int | Unset): Optional optimistic-concurrency predicate for update/delete. The predicate
                applies to the physical row resolved from primary or unique identity.
    """

    op: RowOperationOp
    row: RowsRowDocument | Unset = UNSET
    where: RowSelector | Unset = UNSET
    patch: RowsFieldPatch | Unset = UNSET
    increment: RowsNumericIncrement | Unset = UNSET
    patch_expr: RowsExpressionAssignmentMap | Unset = UNSET
    increment_expr: RowsExpressionAssignmentMap | Unset = UNSET
    json_set: list[RowsJsonSetTransform] | Unset = UNSET
    array_update: list[RowsArrayUpdateTransform] | Unset = UNSET
    on_conflict: RowsOnConflict | Unset = UNSET
    where_expression: RowsExpressionCondition | Unset = UNSET
    returning: list[str] | Unset = UNSET
    returning_expressions: list[RowsExpressionProjection] | Unset = UNSET
    expected_version: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        op = self.op.value

        row: dict[str, Any] | Unset = UNSET
        if not isinstance(self.row, Unset):
            row = self.row.to_dict()

        where: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = self.where.to_dict()

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

        on_conflict: dict[str, Any] | Unset = UNSET
        if not isinstance(self.on_conflict, Unset):
            on_conflict = self.on_conflict.to_dict()

        where_expression: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where_expression, Unset):
            where_expression = self.where_expression.to_dict()

        returning: list[str] | Unset = UNSET
        if not isinstance(self.returning, Unset):
            returning = self.returning

        returning_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.returning_expressions, Unset):
            returning_expressions = []
            for returning_expressions_item_data in self.returning_expressions:
                returning_expressions_item = returning_expressions_item_data.to_dict()
                returning_expressions.append(returning_expressions_item)

        expected_version = self.expected_version

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "op": op,
            }
        )
        if row is not UNSET:
            field_dict["row"] = row
        if where is not UNSET:
            field_dict["where"] = where
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
        if on_conflict is not UNSET:
            field_dict["on_conflict"] = on_conflict
        if where_expression is not UNSET:
            field_dict["where_expression"] = where_expression
        if returning is not UNSET:
            field_dict["returning"] = returning
        if returning_expressions is not UNSET:
            field_dict["returning_expressions"] = returning_expressions
        if expected_version is not UNSET:
            field_dict["expected_version"] = expected_version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.row_selector import RowSelector
        from ..models.rows_array_update_transform import RowsArrayUpdateTransform
        from ..models.rows_expression_assignment_map import RowsExpressionAssignmentMap
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_expression_projection import RowsExpressionProjection
        from ..models.rows_field_patch import RowsFieldPatch
        from ..models.rows_json_set_transform import RowsJsonSetTransform
        from ..models.rows_numeric_increment import RowsNumericIncrement
        from ..models.rows_on_conflict import RowsOnConflict
        from ..models.rows_row_document import RowsRowDocument

        d = dict(src_dict)
        op = RowOperationOp(d.pop("op"))

        _row = d.pop("row", UNSET)
        row: RowsRowDocument | Unset
        if isinstance(_row, Unset):
            row = UNSET
        else:
            row = RowsRowDocument.from_dict(_row)

        _where = d.pop("where", UNSET)
        where: RowSelector | Unset
        if isinstance(_where, Unset):
            where = UNSET
        else:
            where = RowSelector.from_dict(_where)

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

        _on_conflict = d.pop("on_conflict", UNSET)
        on_conflict: RowsOnConflict | Unset
        if isinstance(_on_conflict, Unset):
            on_conflict = UNSET
        else:
            on_conflict = RowsOnConflict.from_dict(_on_conflict)

        _where_expression = d.pop("where_expression", UNSET)
        where_expression: RowsExpressionCondition | Unset
        if isinstance(_where_expression, Unset):
            where_expression = UNSET
        else:
            where_expression = RowsExpressionCondition.from_dict(_where_expression)

        returning = cast(list[str], d.pop("returning", UNSET))

        _returning_expressions = d.pop("returning_expressions", UNSET)
        returning_expressions: list[RowsExpressionProjection] | Unset = UNSET
        if _returning_expressions is not UNSET:
            returning_expressions = []
            for returning_expressions_item_data in _returning_expressions:
                returning_expressions_item = RowsExpressionProjection.from_dict(returning_expressions_item_data)

                returning_expressions.append(returning_expressions_item)

        expected_version = d.pop("expected_version", UNSET)

        row_operation = cls(
            op=op,
            row=row,
            where=where,
            patch=patch,
            increment=increment,
            patch_expr=patch_expr,
            increment_expr=increment_expr,
            json_set=json_set,
            array_update=array_update,
            on_conflict=on_conflict,
            where_expression=where_expression,
            returning=returning,
            returning_expressions=returning_expressions,
            expected_version=expected_version,
        )

        row_operation.additional_properties = d
        return row_operation

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
