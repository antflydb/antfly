from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ArtifactRepairRunResult")


@_attrs_define
class ArtifactRepairRunResult:
    """Result of one bounded artifact repair pass.

    Attributes:
        scanned (int): Number of repair records attempted by this pass.
        groups_scanned (int): Number of table groups touched by this bounded repair pass.
        reprocessed (int): Number of artifacts whose source was reprocessed.
        repaired (int): Number of repair records cleared because the artifact became readable.
        missing_source_docs (int): Number of repair records whose source document no longer exists.
        failed (int): Number of supported repair attempts that failed.
        unsupported (int): Number of repair records skipped because no reprocessor exists for the artifact kind.
        unresolved (int): Number of attempted repair records that remained queued after this pass.
        indexes_rebuilt (int): Number of indexes rebuilt by this pass when target is index.
        indexes_degraded (int): Number of selected indexes that were already degraded or quarantined before repair.
        limit (int): Effective repair limit.
        has_more (bool): Whether another artifact scan page is available via next_cursor.
        debt_remaining (bool): Whether repair debt remains after this bounded pass. If true and next_cursor is absent,
            rerun repair from the beginning after addressing failed or unsupported records.
        next_cursor (None | str | Unset): Opaque cursor for the next artifact repair pass when has_more is true. Index
            repair currently repairs one named index per request and does not return a continuation cursor.
    """

    scanned: int
    groups_scanned: int
    reprocessed: int
    repaired: int
    missing_source_docs: int
    failed: int
    unsupported: int
    unresolved: int
    indexes_rebuilt: int
    indexes_degraded: int
    limit: int
    has_more: bool
    debt_remaining: bool
    next_cursor: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        scanned = self.scanned

        groups_scanned = self.groups_scanned

        reprocessed = self.reprocessed

        repaired = self.repaired

        missing_source_docs = self.missing_source_docs

        failed = self.failed

        unsupported = self.unsupported

        unresolved = self.unresolved

        indexes_rebuilt = self.indexes_rebuilt

        indexes_degraded = self.indexes_degraded

        limit = self.limit

        has_more = self.has_more

        debt_remaining = self.debt_remaining

        next_cursor: None | str | Unset
        if isinstance(self.next_cursor, Unset):
            next_cursor = UNSET
        else:
            next_cursor = self.next_cursor

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "scanned": scanned,
                "groups_scanned": groups_scanned,
                "reprocessed": reprocessed,
                "repaired": repaired,
                "missing_source_docs": missing_source_docs,
                "failed": failed,
                "unsupported": unsupported,
                "unresolved": unresolved,
                "indexes_rebuilt": indexes_rebuilt,
                "indexes_degraded": indexes_degraded,
                "limit": limit,
                "has_more": has_more,
                "debt_remaining": debt_remaining,
            }
        )
        if next_cursor is not UNSET:
            field_dict["next_cursor"] = next_cursor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        scanned = d.pop("scanned")

        groups_scanned = d.pop("groups_scanned")

        reprocessed = d.pop("reprocessed")

        repaired = d.pop("repaired")

        missing_source_docs = d.pop("missing_source_docs")

        failed = d.pop("failed")

        unsupported = d.pop("unsupported")

        unresolved = d.pop("unresolved")

        indexes_rebuilt = d.pop("indexes_rebuilt")

        indexes_degraded = d.pop("indexes_degraded")

        limit = d.pop("limit")

        has_more = d.pop("has_more")

        debt_remaining = d.pop("debt_remaining")

        def _parse_next_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor", UNSET))

        artifact_repair_run_result = cls(
            scanned=scanned,
            groups_scanned=groups_scanned,
            reprocessed=reprocessed,
            repaired=repaired,
            missing_source_docs=missing_source_docs,
            failed=failed,
            unsupported=unsupported,
            unresolved=unresolved,
            indexes_rebuilt=indexes_rebuilt,
            indexes_degraded=indexes_degraded,
            limit=limit,
            has_more=has_more,
            debt_remaining=debt_remaining,
            next_cursor=next_cursor,
        )

        artifact_repair_run_result.additional_properties = d
        return artifact_repair_run_result

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
