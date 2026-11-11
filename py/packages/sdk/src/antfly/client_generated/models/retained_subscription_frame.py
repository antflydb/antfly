from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.retained_subscription_frame_initial_history import RetainedSubscriptionFrameInitialHistory
from ..models.retained_subscription_frame_kind import RetainedSubscriptionFrameKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.retained_dataset_measure import RetainedDatasetMeasure
    from ..models.retained_subscription_frame_events_item import RetainedSubscriptionFrameEventsItem


T = TypeVar("T", bound="RetainedSubscriptionFrame")


@_attrs_define
class RetainedSubscriptionFrame:
    """
    Attributes:
        kind (RetainedSubscriptionFrameKind):
        cursor (str):
        events (list[RetainedSubscriptionFrameEventsItem]):
        initial_history (RetainedSubscriptionFrameInitialHistory | Unset):
        dimensions (list[str] | Unset):
        measures (list[RetainedDatasetMeasure] | Unset):
    """

    kind: RetainedSubscriptionFrameKind
    cursor: str
    events: list[RetainedSubscriptionFrameEventsItem]
    initial_history: RetainedSubscriptionFrameInitialHistory | Unset = UNSET
    dimensions: list[str] | Unset = UNSET
    measures: list[RetainedDatasetMeasure] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        cursor = self.cursor

        events = []
        for events_item_data in self.events:
            events_item = events_item_data.to_dict()
            events.append(events_item)

        initial_history: str | Unset = UNSET
        if not isinstance(self.initial_history, Unset):
            initial_history = self.initial_history.value

        dimensions: list[str] | Unset = UNSET
        if not isinstance(self.dimensions, Unset):
            dimensions = self.dimensions

        measures: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.measures, Unset):
            measures = []
            for measures_item_data in self.measures:
                measures_item = measures_item_data.to_dict()
                measures.append(measures_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
                "cursor": cursor,
                "events": events,
            }
        )
        if initial_history is not UNSET:
            field_dict["initial_history"] = initial_history
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if measures is not UNSET:
            field_dict["measures"] = measures

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.retained_dataset_measure import RetainedDatasetMeasure
        from ..models.retained_subscription_frame_events_item import RetainedSubscriptionFrameEventsItem

        d = dict(src_dict)
        kind = RetainedSubscriptionFrameKind(d.pop("kind"))

        cursor = d.pop("cursor")

        events = []
        _events = d.pop("events")
        for events_item_data in _events:
            events_item = RetainedSubscriptionFrameEventsItem.from_dict(events_item_data)

            events.append(events_item)

        _initial_history = d.pop("initial_history", UNSET)
        initial_history: RetainedSubscriptionFrameInitialHistory | Unset
        if isinstance(_initial_history, Unset):
            initial_history = UNSET
        else:
            initial_history = RetainedSubscriptionFrameInitialHistory(_initial_history)

        dimensions = cast(list[str], d.pop("dimensions", UNSET))

        _measures = d.pop("measures", UNSET)
        measures: list[RetainedDatasetMeasure] | Unset = UNSET
        if _measures is not UNSET:
            measures = []
            for measures_item_data in _measures:
                measures_item = RetainedDatasetMeasure.from_dict(measures_item_data)

                measures.append(measures_item)

        retained_subscription_frame = cls(
            kind=kind,
            cursor=cursor,
            events=events,
            initial_history=initial_history,
            dimensions=dimensions,
            measures=measures,
        )

        retained_subscription_frame.additional_properties = d
        return retained_subscription_frame

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
