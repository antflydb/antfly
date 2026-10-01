from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.relational_constraint_retirement_status_phase import RelationalConstraintRetirementStatusPhase
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalConstraintRetirementStatus")


@_attrs_define
class RelationalConstraintRetirementStatus:
    """
    Attributes:
        id (str): Opaque retirement job identity.
        phase (RelationalConstraintRetirementStatusPhase):
        drop (bool):
        target_schema_version (int):
        failure (str | Unset): Durable diagnostic that pauses the job. Retry resumes the exact
            checkpoint after the cause is addressed; it does not undo a partial
            drain or permit primary mutations while retirement is active.
    """

    id: str
    phase: RelationalConstraintRetirementStatusPhase
    drop: bool
    target_schema_version: int
    failure: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        phase = self.phase.value

        drop = self.drop

        target_schema_version = self.target_schema_version

        failure = self.failure

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "phase": phase,
                "drop": drop,
                "target_schema_version": target_schema_version,
            }
        )
        if failure is not UNSET:
            field_dict["failure"] = failure

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        phase = RelationalConstraintRetirementStatusPhase(d.pop("phase"))

        drop = d.pop("drop")

        target_schema_version = d.pop("target_schema_version")

        failure = d.pop("failure", UNSET)

        relational_constraint_retirement_status = cls(
            id=id,
            phase=phase,
            drop=drop,
            target_schema_version=target_schema_version,
            failure=failure,
        )

        relational_constraint_retirement_status.additional_properties = d
        return relational_constraint_retirement_status

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
