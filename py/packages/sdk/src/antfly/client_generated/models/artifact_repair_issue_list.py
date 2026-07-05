from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.repair_target import RepairTarget
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.artifact_repair_issue import ArtifactRepairIssue


T = TypeVar("T", bound="ArtifactRepairIssueList")


@_attrs_define
class ArtifactRepairIssueList:
    """Bounded page of artifact repair queue entries.

    Attributes:
        table (str): Table whose repair queue was listed.
        target (RepairTarget): Repair subsystem to inspect or run.
        limit (int): Effective page limit.
        scanned (int): Number of repair records scanned while building this page.
        groups_scanned (int): Number of table groups touched while building this page.
        has_more (bool): Whether another page is available.
        issues (list[ArtifactRepairIssue]):
        next_cursor (None | str | Unset): Opaque cursor for the next page.
    """

    table: str
    target: RepairTarget
    limit: int
    scanned: int
    groups_scanned: int
    has_more: bool
    issues: list[ArtifactRepairIssue]
    next_cursor: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        target = self.target.value

        limit = self.limit

        scanned = self.scanned

        groups_scanned = self.groups_scanned

        has_more = self.has_more

        issues = []
        for issues_item_data in self.issues:
            issues_item = issues_item_data.to_dict()
            issues.append(issues_item)

        next_cursor: None | str | Unset
        if isinstance(self.next_cursor, Unset):
            next_cursor = UNSET
        else:
            next_cursor = self.next_cursor

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table": table,
                "target": target,
                "limit": limit,
                "scanned": scanned,
                "groups_scanned": groups_scanned,
                "has_more": has_more,
                "issues": issues,
            }
        )
        if next_cursor is not UNSET:
            field_dict["next_cursor"] = next_cursor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.artifact_repair_issue import ArtifactRepairIssue

        d = dict(src_dict)
        table = d.pop("table")

        target = RepairTarget(d.pop("target"))

        limit = d.pop("limit")

        scanned = d.pop("scanned")

        groups_scanned = d.pop("groups_scanned")

        has_more = d.pop("has_more")

        issues = []
        _issues = d.pop("issues")
        for issues_item_data in _issues:
            issues_item = ArtifactRepairIssue.from_dict(issues_item_data)

            issues.append(issues_item)

        def _parse_next_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor", UNSET))

        artifact_repair_issue_list = cls(
            table=table,
            target=target,
            limit=limit,
            scanned=scanned,
            groups_scanned=groups_scanned,
            has_more=has_more,
            issues=issues,
            next_cursor=next_cursor,
        )

        artifact_repair_issue_list.additional_properties = d
        return artifact_repair_issue_list

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
