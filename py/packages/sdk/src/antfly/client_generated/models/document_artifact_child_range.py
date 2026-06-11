from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentArtifactChildRange")


@_attrs_define
class DocumentArtifactChildRange:
    """Parsed child-range descriptor from a derived document artifact manifest.

    Attributes:
        range_id (str): Stable range identifier within the artifact manifest generation.
        range_kind (str): Kind of children covered by this range, such as unit or chunk.
        artifact_name (str): Artifact namespace covered by this range.
        split_boundary (str): Logical boundary used for splitting this range.
        placement (str): Current placement summary for the range.
        start_key (str): Inclusive first internal child key covered by this range.
        end_key_exclusive (str): Exclusive end internal child key, or empty for the final range.
        last_key (str): Inclusive last internal child key covered by this range.
        child_count (int): Number of child records covered by this range.
        owner_group_id (int | None | Unset): Owner group for this child artifact range, when assigned.
        placement_generation (int | None | Unset): Placement generation for range ownership metadata.
        route_status (None | str | Unset): Current routing status for child writes in this range.
        split_eligible (bool | None | Unset): Whether this range may split at its configured split boundary.
        text_bytes (int | None | Unset): Approximate extracted text bytes covered by this range when available.
    """

    range_id: str
    range_kind: str
    artifact_name: str
    split_boundary: str
    placement: str
    start_key: str
    end_key_exclusive: str
    last_key: str
    child_count: int
    owner_group_id: int | None | Unset = UNSET
    placement_generation: int | None | Unset = UNSET
    route_status: None | str | Unset = UNSET
    split_eligible: bool | None | Unset = UNSET
    text_bytes: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        range_id = self.range_id

        range_kind = self.range_kind

        artifact_name = self.artifact_name

        split_boundary = self.split_boundary

        placement = self.placement

        start_key = self.start_key

        end_key_exclusive = self.end_key_exclusive

        last_key = self.last_key

        child_count = self.child_count

        owner_group_id: int | None | Unset
        if isinstance(self.owner_group_id, Unset):
            owner_group_id = UNSET
        else:
            owner_group_id = self.owner_group_id

        placement_generation: int | None | Unset
        if isinstance(self.placement_generation, Unset):
            placement_generation = UNSET
        else:
            placement_generation = self.placement_generation

        route_status: None | str | Unset
        if isinstance(self.route_status, Unset):
            route_status = UNSET
        else:
            route_status = self.route_status

        split_eligible: bool | None | Unset
        if isinstance(self.split_eligible, Unset):
            split_eligible = UNSET
        else:
            split_eligible = self.split_eligible

        text_bytes: int | None | Unset
        if isinstance(self.text_bytes, Unset):
            text_bytes = UNSET
        else:
            text_bytes = self.text_bytes

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "range_id": range_id,
                "range_kind": range_kind,
                "artifact_name": artifact_name,
                "split_boundary": split_boundary,
                "placement": placement,
                "start_key": start_key,
                "end_key_exclusive": end_key_exclusive,
                "last_key": last_key,
                "child_count": child_count,
            }
        )
        if owner_group_id is not UNSET:
            field_dict["owner_group_id"] = owner_group_id
        if placement_generation is not UNSET:
            field_dict["placement_generation"] = placement_generation
        if route_status is not UNSET:
            field_dict["route_status"] = route_status
        if split_eligible is not UNSET:
            field_dict["split_eligible"] = split_eligible
        if text_bytes is not UNSET:
            field_dict["text_bytes"] = text_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        range_id = d.pop("range_id")

        range_kind = d.pop("range_kind")

        artifact_name = d.pop("artifact_name")

        split_boundary = d.pop("split_boundary")

        placement = d.pop("placement")

        start_key = d.pop("start_key")

        end_key_exclusive = d.pop("end_key_exclusive")

        last_key = d.pop("last_key")

        child_count = d.pop("child_count")

        def _parse_owner_group_id(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        owner_group_id = _parse_owner_group_id(d.pop("owner_group_id", UNSET))

        def _parse_placement_generation(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        placement_generation = _parse_placement_generation(d.pop("placement_generation", UNSET))

        def _parse_route_status(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        route_status = _parse_route_status(d.pop("route_status", UNSET))

        def _parse_split_eligible(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        split_eligible = _parse_split_eligible(d.pop("split_eligible", UNSET))

        def _parse_text_bytes(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        text_bytes = _parse_text_bytes(d.pop("text_bytes", UNSET))

        document_artifact_child_range = cls(
            range_id=range_id,
            range_kind=range_kind,
            artifact_name=artifact_name,
            split_boundary=split_boundary,
            placement=placement,
            start_key=start_key,
            end_key_exclusive=end_key_exclusive,
            last_key=last_key,
            child_count=child_count,
            owner_group_id=owner_group_id,
            placement_generation=placement_generation,
            route_status=route_status,
            split_eligible=split_eligible,
            text_bytes=text_bytes,
        )

        document_artifact_child_range.additional_properties = d
        return document_artifact_child_range

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
