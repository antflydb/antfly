from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.schema_validation_failure import SchemaValidationFailure
from ..models.schema_validation_phase import SchemaValidationPhase
from ..models.schema_validation_state import SchemaValidationState
from ..types import UNSET, Unset

T = TypeVar("T", bound="SchemaValidationStatus")


@_attrs_define
class SchemaValidationStatus:
    """Historical job completion, not an authorization or promise that a later DDL has not superseded this schema. Success
    requires all owner installation, metadata publication and fence release barriers. Current table ownership is checked
    atomically with the job observation.

        Attributes:
            schema_validation_token (str): Opaque immutable validation-only schema job identity. Retain it across timeouts
                or lost replies and observe its exact status without replaying DDL. It grants no authorization.
            table_id (str):
            active_schema_version (int):
            state (SchemaValidationState):
            phase (SchemaValidationPhase | Unset):
            failure (SchemaValidationFailure | Unset):
            owner_count (int | Unset):
            ready (int | Unset):
            installed (int | Unset):
            released (int | Unset):
            canceled (int | Unset):
    """

    schema_validation_token: str
    table_id: str
    active_schema_version: int
    state: SchemaValidationState
    phase: SchemaValidationPhase | Unset = UNSET
    failure: SchemaValidationFailure | Unset = UNSET
    owner_count: int | Unset = UNSET
    ready: int | Unset = UNSET
    installed: int | Unset = UNSET
    released: int | Unset = UNSET
    canceled: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        schema_validation_token = self.schema_validation_token

        table_id = self.table_id

        active_schema_version = self.active_schema_version

        state = self.state.value

        phase: str | Unset = UNSET
        if not isinstance(self.phase, Unset):
            phase = self.phase.value

        failure: str | Unset = UNSET
        if not isinstance(self.failure, Unset):
            failure = self.failure.value

        owner_count = self.owner_count

        ready = self.ready

        installed = self.installed

        released = self.released

        canceled = self.canceled

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "schema_validation_token": schema_validation_token,
                "table_id": table_id,
                "active_schema_version": active_schema_version,
                "state": state,
            }
        )
        if phase is not UNSET:
            field_dict["phase"] = phase
        if failure is not UNSET:
            field_dict["failure"] = failure
        if owner_count is not UNSET:
            field_dict["owner_count"] = owner_count
        if ready is not UNSET:
            field_dict["ready"] = ready
        if installed is not UNSET:
            field_dict["installed"] = installed
        if released is not UNSET:
            field_dict["released"] = released
        if canceled is not UNSET:
            field_dict["canceled"] = canceled

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        schema_validation_token = d.pop("schema_validation_token")

        table_id = d.pop("table_id")

        active_schema_version = d.pop("active_schema_version")

        state = SchemaValidationState(d.pop("state"))

        _phase = d.pop("phase", UNSET)
        phase: SchemaValidationPhase | Unset
        if isinstance(_phase, Unset):
            phase = UNSET
        else:
            phase = SchemaValidationPhase(_phase)

        _failure = d.pop("failure", UNSET)
        failure: SchemaValidationFailure | Unset
        if isinstance(_failure, Unset):
            failure = UNSET
        else:
            failure = SchemaValidationFailure(_failure)

        owner_count = d.pop("owner_count", UNSET)

        ready = d.pop("ready", UNSET)

        installed = d.pop("installed", UNSET)

        released = d.pop("released", UNSET)

        canceled = d.pop("canceled", UNSET)

        schema_validation_status = cls(
            schema_validation_token=schema_validation_token,
            table_id=table_id,
            active_schema_version=active_schema_version,
            state=state,
            phase=phase,
            failure=failure,
            owner_count=owner_count,
            ready=ready,
            installed=installed,
            released=released,
            canceled=canceled,
        )

        return schema_validation_status
