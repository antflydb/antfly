from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.artifact_repair_kind import ArtifactRepairKind
from ..models.repair_target import RepairTarget
from ..types import UNSET, Unset

T = TypeVar("T", bound="RepairIssueListRequest")


@_attrs_define
class RepairIssueListRequest:
    """Bounded request to list table repair issues.

    Attributes:
        target (RepairTarget | Unset): Repair subsystem to inspect or run.
        kind (ArtifactRepairKind | Unset): Kind of stored artifact tracked by the repair queue.
        index (str | Unset): Restrict results to one index name.
        cursor (str | Unset): Opaque cursor returned by a prior response.
        limit (int | Unset): Maximum repair records to return. Default: 50.
    """

    target: RepairTarget | Unset = UNSET
    kind: ArtifactRepairKind | Unset = UNSET
    index: str | Unset = UNSET
    cursor: str | Unset = UNSET
    limit: int | Unset = 50
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        target: str | Unset = UNSET
        if not isinstance(self.target, Unset):
            target = self.target.value

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        index = self.index

        cursor = self.cursor

        limit = self.limit

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if target is not UNSET:
            field_dict["target"] = target
        if kind is not UNSET:
            field_dict["kind"] = kind
        if index is not UNSET:
            field_dict["index"] = index
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _target = d.pop("target", UNSET)
        target: RepairTarget | Unset
        if isinstance(_target, Unset):
            target = UNSET
        else:
            target = RepairTarget(_target)

        _kind = d.pop("kind", UNSET)
        kind: ArtifactRepairKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = ArtifactRepairKind(_kind)

        index = d.pop("index", UNSET)

        cursor = d.pop("cursor", UNSET)

        limit = d.pop("limit", UNSET)

        repair_issue_list_request = cls(
            target=target,
            kind=kind,
            index=index,
            cursor=cursor,
            limit=limit,
        )

        repair_issue_list_request.additional_properties = d
        return repair_issue_list_request

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
