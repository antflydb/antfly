from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.relational_index_build_failure import RelationalIndexBuildFailure
from ..models.relational_index_build_state import RelationalIndexBuildState
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalIndexRangeStatus")


@_attrs_define
class RelationalIndexRangeStatus:
    """
    Attributes:
        group_id (str):
        generation (str): Exact uint64 generation encoded as decimal, never a floating-point number.
        slot (int):
        owner (str): Namespace and owned-range fingerprint.
        comparison (str): Executable tuple comparison fingerprint.
        progress_digest (str): Exact durable progress observation for generation-fenced maintenance.
        maintenance_epoch (str): Replicated desired maintenance ticket, separate from replica-local progress.
        state (RelationalIndexBuildState):
        rows_scanned (str):
        last_maintenance_request (str | Unset): Most recently accepted maintenance command proof for exact retry
            acknowledgement.
        failure (RelationalIndexBuildFailure | Unset):
    """

    group_id: str
    generation: str
    slot: int
    owner: str
    comparison: str
    progress_digest: str
    maintenance_epoch: str
    state: RelationalIndexBuildState
    rows_scanned: str
    last_maintenance_request: str | Unset = UNSET
    failure: RelationalIndexBuildFailure | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        generation = self.generation

        slot = self.slot

        owner = self.owner

        comparison = self.comparison

        progress_digest = self.progress_digest

        maintenance_epoch = self.maintenance_epoch

        state = self.state.value

        rows_scanned = self.rows_scanned

        last_maintenance_request = self.last_maintenance_request

        failure: str | Unset = UNSET
        if not isinstance(self.failure, Unset):
            failure = self.failure.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "group_id": group_id,
                "generation": generation,
                "slot": slot,
                "owner": owner,
                "comparison": comparison,
                "progress_digest": progress_digest,
                "maintenance_epoch": maintenance_epoch,
                "state": state,
                "rows_scanned": rows_scanned,
            }
        )
        if last_maintenance_request is not UNSET:
            field_dict["last_maintenance_request"] = last_maintenance_request
        if failure is not UNSET:
            field_dict["failure"] = failure

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        group_id = d.pop("group_id")

        generation = d.pop("generation")

        slot = d.pop("slot")

        owner = d.pop("owner")

        comparison = d.pop("comparison")

        progress_digest = d.pop("progress_digest")

        maintenance_epoch = d.pop("maintenance_epoch")

        state = RelationalIndexBuildState(d.pop("state"))

        rows_scanned = d.pop("rows_scanned")

        last_maintenance_request = d.pop("last_maintenance_request", UNSET)

        _failure = d.pop("failure", UNSET)
        failure: RelationalIndexBuildFailure | Unset
        if isinstance(_failure, Unset):
            failure = UNSET
        else:
            failure = RelationalIndexBuildFailure(_failure)

        relational_index_range_status = cls(
            group_id=group_id,
            generation=generation,
            slot=slot,
            owner=owner,
            comparison=comparison,
            progress_digest=progress_digest,
            maintenance_epoch=maintenance_epoch,
            state=state,
            rows_scanned=rows_scanned,
            last_maintenance_request=last_maintenance_request,
            failure=failure,
        )

        relational_index_range_status.additional_properties = d
        return relational_index_range_status

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
