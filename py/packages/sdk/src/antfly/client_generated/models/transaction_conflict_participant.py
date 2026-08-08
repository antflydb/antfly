from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.transaction_conflict_participant_phase import TransactionConflictParticipantPhase
from ..types import UNSET, Unset

T = TypeVar("T", bound="TransactionConflictParticipant")


@_attrs_define
class TransactionConflictParticipant:
    """Participant location and 2PC phase where the conflict occurred.

    Attributes:
        group_id (int | Unset): Raft group that reported the conflict.
        phase (TransactionConflictParticipantPhase | Unset): 2PC participant phase that reported the conflict.
    """

    group_id: int | Unset = UNSET
    phase: TransactionConflictParticipantPhase | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        phase: str | Unset = UNSET
        if not isinstance(self.phase, Unset):
            phase = self.phase.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if group_id is not UNSET:
            field_dict["group_id"] = group_id
        if phase is not UNSET:
            field_dict["phase"] = phase

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        group_id = d.pop("group_id", UNSET)

        _phase = d.pop("phase", UNSET)
        phase: TransactionConflictParticipantPhase | Unset
        if isinstance(_phase, Unset):
            phase = UNSET
        else:
            phase = TransactionConflictParticipantPhase(_phase)

        transaction_conflict_participant = cls(
            group_id=group_id,
            phase=phase,
        )

        transaction_conflict_participant.additional_properties = d
        return transaction_conflict_participant

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
