from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.transaction_session_status import TransactionSessionStatus


T = TypeVar("T", bound="TransactionSessionListResponse")


@_attrs_define
class TransactionSessionListResponse:
    """
    Attributes:
        session_count (int | Unset):
        lease_held_count (int | Unset):
        lease_expired_count (int | Unset):
        sessions (list[TransactionSessionStatus] | Unset):
    """

    session_count: int | Unset = UNSET
    lease_held_count: int | Unset = UNSET
    lease_expired_count: int | Unset = UNSET
    sessions: list[TransactionSessionStatus] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        session_count = self.session_count

        lease_held_count = self.lease_held_count

        lease_expired_count = self.lease_expired_count

        sessions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sessions, Unset):
            sessions = []
            for sessions_item_data in self.sessions:
                sessions_item = sessions_item_data.to_dict()
                sessions.append(sessions_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if session_count is not UNSET:
            field_dict["session_count"] = session_count
        if lease_held_count is not UNSET:
            field_dict["lease_held_count"] = lease_held_count
        if lease_expired_count is not UNSET:
            field_dict["lease_expired_count"] = lease_expired_count
        if sessions is not UNSET:
            field_dict["sessions"] = sessions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.transaction_session_status import TransactionSessionStatus

        d = dict(src_dict)
        session_count = d.pop("session_count", UNSET)

        lease_held_count = d.pop("lease_held_count", UNSET)

        lease_expired_count = d.pop("lease_expired_count", UNSET)

        _sessions = d.pop("sessions", UNSET)
        sessions: list[TransactionSessionStatus] | Unset = UNSET
        if _sessions is not UNSET:
            sessions = []
            for sessions_item_data in _sessions:
                sessions_item = TransactionSessionStatus.from_dict(sessions_item_data)

                sessions.append(sessions_item)

        transaction_session_list_response = cls(
            session_count=session_count,
            lease_held_count=lease_held_count,
            lease_expired_count=lease_expired_count,
            sessions=sessions,
        )

        transaction_session_list_response.additional_properties = d
        return transaction_session_list_response

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
