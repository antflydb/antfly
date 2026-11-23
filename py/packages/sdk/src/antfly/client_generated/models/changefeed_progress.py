from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.changefeed_progress_phase import ChangefeedProgressPhase
from ..types import UNSET, Unset

T = TypeVar("T", bound="ChangefeedProgress")


@_attrs_define
class ChangefeedProgress:
    """
    Attributes:
        phase (ChangefeedProgressPhase):
        acknowledged_cursor (str):
        unacknowledged_deliveries (int):
        cursor_window (int):
        cursor_byte_limit (int):
        expires_at (int):
        oldest_unacknowledged_at (int | None | Unset):
        bootstrap_expires_at (int | None | Unset):
    """

    phase: ChangefeedProgressPhase
    acknowledged_cursor: str
    unacknowledged_deliveries: int
    cursor_window: int
    cursor_byte_limit: int
    expires_at: int
    oldest_unacknowledged_at: int | None | Unset = UNSET
    bootstrap_expires_at: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        phase = self.phase.value

        acknowledged_cursor = self.acknowledged_cursor

        unacknowledged_deliveries = self.unacknowledged_deliveries

        cursor_window = self.cursor_window

        cursor_byte_limit = self.cursor_byte_limit

        expires_at = self.expires_at

        oldest_unacknowledged_at: int | None | Unset
        if isinstance(self.oldest_unacknowledged_at, Unset):
            oldest_unacknowledged_at = UNSET
        else:
            oldest_unacknowledged_at = self.oldest_unacknowledged_at

        bootstrap_expires_at: int | None | Unset
        if isinstance(self.bootstrap_expires_at, Unset):
            bootstrap_expires_at = UNSET
        else:
            bootstrap_expires_at = self.bootstrap_expires_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "phase": phase,
                "acknowledged_cursor": acknowledged_cursor,
                "unacknowledged_deliveries": unacknowledged_deliveries,
                "cursor_window": cursor_window,
                "cursor_byte_limit": cursor_byte_limit,
                "expires_at": expires_at,
            }
        )
        if oldest_unacknowledged_at is not UNSET:
            field_dict["oldest_unacknowledged_at"] = oldest_unacknowledged_at
        if bootstrap_expires_at is not UNSET:
            field_dict["bootstrap_expires_at"] = bootstrap_expires_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        phase = ChangefeedProgressPhase(d.pop("phase"))

        acknowledged_cursor = d.pop("acknowledged_cursor")

        unacknowledged_deliveries = d.pop("unacknowledged_deliveries")

        cursor_window = d.pop("cursor_window")

        cursor_byte_limit = d.pop("cursor_byte_limit")

        expires_at = d.pop("expires_at")

        def _parse_oldest_unacknowledged_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        oldest_unacknowledged_at = _parse_oldest_unacknowledged_at(d.pop("oldest_unacknowledged_at", UNSET))

        def _parse_bootstrap_expires_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        bootstrap_expires_at = _parse_bootstrap_expires_at(d.pop("bootstrap_expires_at", UNSET))

        changefeed_progress = cls(
            phase=phase,
            acknowledged_cursor=acknowledged_cursor,
            unacknowledged_deliveries=unacknowledged_deliveries,
            cursor_window=cursor_window,
            cursor_byte_limit=cursor_byte_limit,
            expires_at=expires_at,
            oldest_unacknowledged_at=oldest_unacknowledged_at,
            bootstrap_expires_at=bootstrap_expires_at,
        )

        changefeed_progress.additional_properties = d
        return changefeed_progress

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
