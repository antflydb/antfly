from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.relational_constraint_activation_phase import RelationalConstraintActivationPhase
from ..models.relational_constraint_validation_state import RelationalConstraintValidationState
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalConstraintRangeStatus")


@_attrs_define
class RelationalConstraintRangeStatus:
    """
    Attributes:
        group_id (str): Exact owner group identifier as decimal text.
        state (RelationalConstraintValidationState): Validation state of an existing-row constraint.
        phase (RelationalConstraintActivationPhase):
        rows_scanned (str): Exact cumulative validation row count as decimal text.
        owner (str): Opaque namespace-and-range ownership digest.
        failure (str | Unset):
    """

    group_id: str
    state: RelationalConstraintValidationState
    phase: RelationalConstraintActivationPhase
    rows_scanned: str
    owner: str
    failure: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        state = self.state.value

        phase = self.phase.value

        rows_scanned = self.rows_scanned

        owner = self.owner

        failure = self.failure

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "group_id": group_id,
                "state": state,
                "phase": phase,
                "rows_scanned": rows_scanned,
                "owner": owner,
            }
        )
        if failure is not UNSET:
            field_dict["failure"] = failure

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        group_id = d.pop("group_id")

        state = RelationalConstraintValidationState(d.pop("state"))

        phase = RelationalConstraintActivationPhase(d.pop("phase"))

        rows_scanned = d.pop("rows_scanned")

        owner = d.pop("owner")

        failure = d.pop("failure", UNSET)

        relational_constraint_range_status = cls(
            group_id=group_id,
            state=state,
            phase=phase,
            rows_scanned=rows_scanned,
            owner=owner,
            failure=failure,
        )

        relational_constraint_range_status.additional_properties = d
        return relational_constraint_range_status

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
