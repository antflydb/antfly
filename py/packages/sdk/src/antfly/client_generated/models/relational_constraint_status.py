from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.relational_constraint_status_coverage_kind import RelationalConstraintStatusCoverageKind
from ..models.relational_constraint_validation_state import RelationalConstraintValidationState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_constraint_range_status import RelationalConstraintRangeStatus
    from ..models.relational_constraint_retirement_status import RelationalConstraintRetirementStatus


T = TypeVar("T", bound="RelationalConstraintStatus")


@_attrs_define
class RelationalConstraintStatus:
    """
    Attributes:
        schema_version (int):
        coverage_kind (RelationalConstraintStatusCoverageKind): Distributed UNIQUE, foreign-key, and scalar CHECK
            coverage across every current table owner. Native local validation is not a substitute for this coordinated
            proof.
        state (RelationalConstraintValidationState): Validation state of an existing-row constraint.
        ranges (list[RelationalConstraintRangeStatus]):
        retirement (RelationalConstraintRetirementStatus | Unset):
    """

    schema_version: int
    coverage_kind: RelationalConstraintStatusCoverageKind
    state: RelationalConstraintValidationState
    ranges: list[RelationalConstraintRangeStatus]
    retirement: RelationalConstraintRetirementStatus | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        schema_version = self.schema_version

        coverage_kind = self.coverage_kind.value

        state = self.state.value

        ranges = []
        for ranges_item_data in self.ranges:
            ranges_item = ranges_item_data.to_dict()
            ranges.append(ranges_item)

        retirement: dict[str, Any] | Unset = UNSET
        if not isinstance(self.retirement, Unset):
            retirement = self.retirement.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "schema_version": schema_version,
                "coverage_kind": coverage_kind,
                "state": state,
                "ranges": ranges,
            }
        )
        if retirement is not UNSET:
            field_dict["retirement"] = retirement

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_constraint_range_status import RelationalConstraintRangeStatus
        from ..models.relational_constraint_retirement_status import RelationalConstraintRetirementStatus

        d = dict(src_dict)
        schema_version = d.pop("schema_version")

        coverage_kind = RelationalConstraintStatusCoverageKind(d.pop("coverage_kind"))

        state = RelationalConstraintValidationState(d.pop("state"))

        ranges = []
        _ranges = d.pop("ranges")
        for ranges_item_data in _ranges:
            ranges_item = RelationalConstraintRangeStatus.from_dict(ranges_item_data)

            ranges.append(ranges_item)

        _retirement = d.pop("retirement", UNSET)
        retirement: RelationalConstraintRetirementStatus | Unset
        if isinstance(_retirement, Unset):
            retirement = UNSET
        else:
            retirement = RelationalConstraintRetirementStatus.from_dict(_retirement)

        relational_constraint_status = cls(
            schema_version=schema_version,
            coverage_kind=coverage_kind,
            state=state,
            ranges=ranges,
            retirement=retirement,
        )

        relational_constraint_status.additional_properties = d
        return relational_constraint_status

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
