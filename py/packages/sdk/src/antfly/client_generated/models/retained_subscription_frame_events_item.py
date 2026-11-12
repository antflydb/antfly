from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.retained_subscription_frame_events_item_type import RetainedSubscriptionFrameEventsItemType
from ..types import UNSET, Unset

T = TypeVar("T", bound="RetainedSubscriptionFrameEventsItem")


@_attrs_define
class RetainedSubscriptionFrameEventsItem:
    """
    Attributes:
        type_ (RetainedSubscriptionFrameEventsItemType):
        key (str):
        bucket_start_ns (str | Unset):
        dimensions (list[Any] | Unset):
        values (list[Any] | Unset):
    """

    type_: RetainedSubscriptionFrameEventsItemType
    key: str
    bucket_start_ns: str | Unset = UNSET
    dimensions: list[Any] | Unset = UNSET
    values: list[Any] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        key = self.key

        bucket_start_ns = self.bucket_start_ns

        dimensions: list[Any] | Unset = UNSET
        if not isinstance(self.dimensions, Unset):
            dimensions = self.dimensions

        values: list[Any] | Unset = UNSET
        if not isinstance(self.values, Unset):
            values = self.values

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "key": key,
            }
        )
        if bucket_start_ns is not UNSET:
            field_dict["bucket_start_ns"] = bucket_start_ns
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if values is not UNSET:
            field_dict["values"] = values

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = RetainedSubscriptionFrameEventsItemType(d.pop("type"))

        key = d.pop("key")

        bucket_start_ns = d.pop("bucket_start_ns", UNSET)

        dimensions = cast(list[Any], d.pop("dimensions", UNSET))

        values = cast(list[Any], d.pop("values", UNSET))

        retained_subscription_frame_events_item = cls(
            type_=type_,
            key=key,
            bucket_start_ns=bucket_start_ns,
            dimensions=dimensions,
            values=values,
        )

        retained_subscription_frame_events_item.additional_properties = d
        return retained_subscription_frame_events_item

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
