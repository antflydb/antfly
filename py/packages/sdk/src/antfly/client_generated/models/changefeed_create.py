from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.changefeed_create_events_item import ChangefeedCreateEventsItem
from ..models.changefeed_create_include import ChangefeedCreateInclude
from ..models.changefeed_create_start import ChangefeedCreateStart
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.changefeed_selector import ChangefeedSelector


T = TypeVar("T", bound="ChangefeedCreate")


@_attrs_define
class ChangefeedCreate:
    """
    Attributes:
        tables (list[str]):
        selectors (list[ChangefeedSelector] | Unset):
        start (ChangefeedCreateStart | Unset):  Default: ChangefeedCreateStart.NOW.
        events (list[ChangefeedCreateEventsItem] | Unset):
        include (ChangefeedCreateInclude | Unset):  Default: ChangefeedCreateInclude.KEYS_AND_VERSIONS.
        lease_seconds (int | Unset):  Default: 3600.
    """

    tables: list[str]
    selectors: list[ChangefeedSelector] | Unset = UNSET
    start: ChangefeedCreateStart | Unset = ChangefeedCreateStart.NOW
    events: list[ChangefeedCreateEventsItem] | Unset = UNSET
    include: ChangefeedCreateInclude | Unset = ChangefeedCreateInclude.KEYS_AND_VERSIONS
    lease_seconds: int | Unset = 3600
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        tables = self.tables

        selectors: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.selectors, Unset):
            selectors = []
            for selectors_item_data in self.selectors:
                selectors_item = selectors_item_data.to_dict()
                selectors.append(selectors_item)

        start: str | Unset = UNSET
        if not isinstance(self.start, Unset):
            start = self.start.value

        events: list[str] | Unset = UNSET
        if not isinstance(self.events, Unset):
            events = []
            for events_item_data in self.events:
                events_item = events_item_data.value
                events.append(events_item)

        include: str | Unset = UNSET
        if not isinstance(self.include, Unset):
            include = self.include.value

        lease_seconds = self.lease_seconds

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "tables": tables,
            }
        )
        if selectors is not UNSET:
            field_dict["selectors"] = selectors
        if start is not UNSET:
            field_dict["start"] = start
        if events is not UNSET:
            field_dict["events"] = events
        if include is not UNSET:
            field_dict["include"] = include
        if lease_seconds is not UNSET:
            field_dict["lease_seconds"] = lease_seconds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.changefeed_selector import ChangefeedSelector

        d = dict(src_dict)
        tables = cast(list[str], d.pop("tables"))

        _selectors = d.pop("selectors", UNSET)
        selectors: list[ChangefeedSelector] | Unset = UNSET
        if _selectors is not UNSET:
            selectors = []
            for selectors_item_data in _selectors:
                selectors_item = ChangefeedSelector.from_dict(selectors_item_data)

                selectors.append(selectors_item)

        _start = d.pop("start", UNSET)
        start: ChangefeedCreateStart | Unset
        if isinstance(_start, Unset):
            start = UNSET
        else:
            start = ChangefeedCreateStart(_start)

        _events = d.pop("events", UNSET)
        events: list[ChangefeedCreateEventsItem] | Unset = UNSET
        if _events is not UNSET:
            events = []
            for events_item_data in _events:
                events_item = ChangefeedCreateEventsItem(events_item_data)

                events.append(events_item)

        _include = d.pop("include", UNSET)
        include: ChangefeedCreateInclude | Unset
        if isinstance(_include, Unset):
            include = UNSET
        else:
            include = ChangefeedCreateInclude(_include)

        lease_seconds = d.pop("lease_seconds", UNSET)

        changefeed_create = cls(
            tables=tables,
            selectors=selectors,
            start=start,
            events=events,
            include=include,
            lease_seconds=lease_seconds,
        )

        changefeed_create.additional_properties = d
        return changefeed_create

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
