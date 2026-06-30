from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.rows_on_conflict_action import RowsOnConflictAction
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_array_update_transform import RowsArrayUpdateTransform
    from ..models.rows_expression_assignment_map import RowsExpressionAssignmentMap
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_field_patch import RowsFieldPatch
    from ..models.rows_json_set_transform import RowsJsonSetTransform
    from ..models.rows_numeric_increment import RowsNumericIncrement


T = TypeVar("T", bound="RowsOnConflict")


@_attrs_define
class RowsOnConflict:
    """Typed conflict action for insert operations. `nothing` skips the insert
    when the target already exists. `update` applies the same typed update
    operators as ordinary row updates, with expression sources allowed to
    reference `existing` and `proposed` row images.

        Attributes:
            target (Any): Primary-key or named unique constraint conflict target.
            action (RowsOnConflictAction):
            patch (RowsFieldPatch | Unset): Static field patch for top-level relational columns. Primary-key fields
                are rejected by the server.
            increment (RowsNumericIncrement | Unset): Numeric increment map keyed by declared numeric columns.
            patch_expr (RowsExpressionAssignmentMap | Unset): Field-to-expression assignment map over the shared row-
                expression AST.
            increment_expr (RowsExpressionAssignmentMap | Unset): Field-to-expression assignment map over the shared row-
                expression AST.
            json_set (list[RowsJsonSetTransform] | Unset):
            array_update (list[RowsArrayUpdateTransform] | Unset):
            where_expression (RowsExpressionCondition | Unset): Computed expression predicate over the shared row-expression
                AST.
    """

    target: Any
    action: RowsOnConflictAction
    patch: RowsFieldPatch | Unset = UNSET
    increment: RowsNumericIncrement | Unset = UNSET
    patch_expr: RowsExpressionAssignmentMap | Unset = UNSET
    increment_expr: RowsExpressionAssignmentMap | Unset = UNSET
    json_set: list[RowsJsonSetTransform] | Unset = UNSET
    array_update: list[RowsArrayUpdateTransform] | Unset = UNSET
    where_expression: RowsExpressionCondition | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        target: Any
        target = self.target

        action = self.action.value

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

        where_expression: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where_expression, Unset):
            where_expression = self.where_expression.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "target": target,
                "action": action,
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
        if where_expression is not UNSET:
            field_dict["where_expression"] = where_expression

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_array_update_transform import RowsArrayUpdateTransform
        from ..models.rows_expression_assignment_map import RowsExpressionAssignmentMap
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_field_patch import RowsFieldPatch
        from ..models.rows_json_set_transform import RowsJsonSetTransform
        from ..models.rows_numeric_increment import RowsNumericIncrement

        d = dict(src_dict)

        def _parse_target(data: object) -> Any:
            return cast(Any, data)

        target = _parse_target(d.pop("target"))

        action = RowsOnConflictAction(d.pop("action"))

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

        _where_expression = d.pop("where_expression", UNSET)
        where_expression: RowsExpressionCondition | Unset
        if isinstance(_where_expression, Unset):
            where_expression = UNSET
        else:
            where_expression = RowsExpressionCondition.from_dict(_where_expression)

        rows_on_conflict = cls(
            target=target,
            action=action,
            patch=patch,
            increment=increment,
            patch_expr=patch_expr,
            increment_expr=increment_expr,
            json_set=json_set,
            array_update=array_update,
            where_expression=where_expression,
        )

        return rows_on_conflict
