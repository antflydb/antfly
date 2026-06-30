from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_joined_mutation_source_assignment_side import RowsJoinedMutationSourceAssignmentSide

T = TypeVar("T", bound="RowsJoinedMutationSourceAssignment")


@_attrs_define
class RowsJoinedMutationSourceAssignment:
    """Source-side assignment for joined mutation-source updates.

    Attributes:
        target_field (str): Declared target-side relational field to assign.
        side (RowsJoinedMutationSourceAssignmentSide): Join side that supplies the source field. Must be the non-target
            side.
        field (str): Declared relational field to copy from the source side.
    """

    target_field: str
    side: RowsJoinedMutationSourceAssignmentSide
    field: str

    def to_dict(self) -> dict[str, Any]:
        target_field = self.target_field

        side = self.side.value

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "target_field": target_field,
                "side": side,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        target_field = d.pop("target_field")

        side = RowsJoinedMutationSourceAssignmentSide(d.pop("side"))

        field = d.pop("field")

        rows_joined_mutation_source_assignment = cls(
            target_field=target_field,
            side=side,
            field=field,
        )

        return rows_joined_mutation_source_assignment
