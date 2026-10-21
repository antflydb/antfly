from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.grouped_artifact_source_filter import GroupedArtifactSourceFilter


T = TypeVar("T", bound="GroupedArtifactSource")


@_attrs_define
class GroupedArtifactSource:
    """
    Attributes:
        group_by (str):
        order_by (str): Field containing an integer ordinal. Ties use the document key.
        field (str):
        segment_by (str | Unset):
        filter_ (GroupedArtifactSourceFilter | Unset): Canonical row filter evaluated on the source projection.
        max_bytes (int | Unset): UTF-8 text bytes per unit, including joining newlines. An oversized single member fails
            explicitly. Default: 32768.
        max_members (int | Unset): Maximum members per unit; larger partitions emit multiple units. Default: 1024.
        window_ordinals (int | Unset): Fixed ordinal cells bound repartitioning and incremental work for long groups.
            Default: 128.
    """

    group_by: str
    order_by: str
    field: str
    segment_by: str | Unset = UNSET
    filter_: GroupedArtifactSourceFilter | Unset = UNSET
    max_bytes: int | Unset = 32768
    max_members: int | Unset = 1024
    window_ordinals: int | Unset = 128

    def to_dict(self) -> dict[str, Any]:
        group_by = self.group_by

        order_by = self.order_by

        field = self.field

        segment_by = self.segment_by

        filter_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.filter_, Unset):
            filter_ = self.filter_.to_dict()

        max_bytes = self.max_bytes

        max_members = self.max_members

        window_ordinals = self.window_ordinals

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "group_by": group_by,
                "order_by": order_by,
                "field": field,
            }
        )
        if segment_by is not UNSET:
            field_dict["segment_by"] = segment_by
        if filter_ is not UNSET:
            field_dict["filter"] = filter_
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes
        if max_members is not UNSET:
            field_dict["max_members"] = max_members
        if window_ordinals is not UNSET:
            field_dict["window_ordinals"] = window_ordinals

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.grouped_artifact_source_filter import GroupedArtifactSourceFilter

        d = dict(src_dict)
        group_by = d.pop("group_by")

        order_by = d.pop("order_by")

        field = d.pop("field")

        segment_by = d.pop("segment_by", UNSET)

        _filter_ = d.pop("filter", UNSET)
        filter_: GroupedArtifactSourceFilter | Unset
        if isinstance(_filter_, Unset):
            filter_ = UNSET
        else:
            filter_ = GroupedArtifactSourceFilter.from_dict(_filter_)

        max_bytes = d.pop("max_bytes", UNSET)

        max_members = d.pop("max_members", UNSET)

        window_ordinals = d.pop("window_ordinals", UNSET)

        grouped_artifact_source = cls(
            group_by=group_by,
            order_by=order_by,
            field=field,
            segment_by=segment_by,
            filter_=filter_,
            max_bytes=max_bytes,
            max_members=max_members,
            window_ordinals=window_ordinals,
        )

        return grouped_artifact_source
