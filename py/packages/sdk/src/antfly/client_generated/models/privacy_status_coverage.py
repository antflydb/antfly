from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.privacy_status_coverage_categories import PrivacyStatusCoverageCategories
    from ..models.privacy_status_coverage_failures_item import PrivacyStatusCoverageFailuresItem
    from ..models.privacy_status_coverage_fields import PrivacyStatusCoverageFields


T = TypeVar("T", bound="PrivacyStatusCoverage")


@_attrs_define
class PrivacyStatusCoverage:
    """
    Attributes:
        available (bool | Unset):
        refreshing (bool | Unset): A bounded background census is building the next complete result.
        started_at_millis (int | Unset): Start of the published observation interval; counts are informational and are
            not an atomic cross-shard snapshot.
        completed_at_millis (int | Unset): Completion of the published census. Counts refresh asynchronously as
            documents change and never authorize protected serving.
        total (int | None | Unset):
        current (int | None | Unset):
        pending (int | None | Unset):
        failed (int | None | Unset):
        with_findings (int | None | Unset):
        protected_ready (int | None | Unset):
        protected_blocked (int | None | Unset):
        categories (PrivacyStatusCoverageCategories | Unset):
        fields (PrivacyStatusCoverageFields | Unset): Findings by field across current inspections; pending and failed
            sources contribute no counts.
        failed_known (int | Unset): Source-bound failures observed so far; failed remains null while other work is
            unclassified.
        failures (list[PrivacyStatusCoverageFailuresItem] | Unset):
        next_key (None | str | Unset): Reserved for bounded internal census pages; omitted from public coverage
            snapshots.
    """

    available: bool | Unset = UNSET
    refreshing: bool | Unset = UNSET
    started_at_millis: int | Unset = UNSET
    completed_at_millis: int | Unset = UNSET
    total: int | None | Unset = UNSET
    current: int | None | Unset = UNSET
    pending: int | None | Unset = UNSET
    failed: int | None | Unset = UNSET
    with_findings: int | None | Unset = UNSET
    protected_ready: int | None | Unset = UNSET
    protected_blocked: int | None | Unset = UNSET
    categories: PrivacyStatusCoverageCategories | Unset = UNSET
    fields: PrivacyStatusCoverageFields | Unset = UNSET
    failed_known: int | Unset = UNSET
    failures: list[PrivacyStatusCoverageFailuresItem] | Unset = UNSET
    next_key: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        available = self.available

        refreshing = self.refreshing

        started_at_millis = self.started_at_millis

        completed_at_millis = self.completed_at_millis

        total: int | None | Unset
        if isinstance(self.total, Unset):
            total = UNSET
        else:
            total = self.total

        current: int | None | Unset
        if isinstance(self.current, Unset):
            current = UNSET
        else:
            current = self.current

        pending: int | None | Unset
        if isinstance(self.pending, Unset):
            pending = UNSET
        else:
            pending = self.pending

        failed: int | None | Unset
        if isinstance(self.failed, Unset):
            failed = UNSET
        else:
            failed = self.failed

        with_findings: int | None | Unset
        if isinstance(self.with_findings, Unset):
            with_findings = UNSET
        else:
            with_findings = self.with_findings

        protected_ready: int | None | Unset
        if isinstance(self.protected_ready, Unset):
            protected_ready = UNSET
        else:
            protected_ready = self.protected_ready

        protected_blocked: int | None | Unset
        if isinstance(self.protected_blocked, Unset):
            protected_blocked = UNSET
        else:
            protected_blocked = self.protected_blocked

        categories: dict[str, Any] | Unset = UNSET
        if not isinstance(self.categories, Unset):
            categories = self.categories.to_dict()

        fields: dict[str, Any] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields.to_dict()

        failed_known = self.failed_known

        failures: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.failures, Unset):
            failures = []
            for failures_item_data in self.failures:
                failures_item = failures_item_data.to_dict()
                failures.append(failures_item)

        next_key: None | str | Unset
        if isinstance(self.next_key, Unset):
            next_key = UNSET
        else:
            next_key = self.next_key

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if available is not UNSET:
            field_dict["available"] = available
        if refreshing is not UNSET:
            field_dict["refreshing"] = refreshing
        if started_at_millis is not UNSET:
            field_dict["started_at_millis"] = started_at_millis
        if completed_at_millis is not UNSET:
            field_dict["completed_at_millis"] = completed_at_millis
        if total is not UNSET:
            field_dict["total"] = total
        if current is not UNSET:
            field_dict["current"] = current
        if pending is not UNSET:
            field_dict["pending"] = pending
        if failed is not UNSET:
            field_dict["failed"] = failed
        if with_findings is not UNSET:
            field_dict["with_findings"] = with_findings
        if protected_ready is not UNSET:
            field_dict["protected_ready"] = protected_ready
        if protected_blocked is not UNSET:
            field_dict["protected_blocked"] = protected_blocked
        if categories is not UNSET:
            field_dict["categories"] = categories
        if fields is not UNSET:
            field_dict["fields"] = fields
        if failed_known is not UNSET:
            field_dict["failed_known"] = failed_known
        if failures is not UNSET:
            field_dict["failures"] = failures
        if next_key is not UNSET:
            field_dict["next_key"] = next_key

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.privacy_status_coverage_categories import PrivacyStatusCoverageCategories
        from ..models.privacy_status_coverage_failures_item import PrivacyStatusCoverageFailuresItem
        from ..models.privacy_status_coverage_fields import PrivacyStatusCoverageFields

        d = dict(src_dict)
        available = d.pop("available", UNSET)

        refreshing = d.pop("refreshing", UNSET)

        started_at_millis = d.pop("started_at_millis", UNSET)

        completed_at_millis = d.pop("completed_at_millis", UNSET)

        def _parse_total(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        total = _parse_total(d.pop("total", UNSET))

        def _parse_current(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        current = _parse_current(d.pop("current", UNSET))

        def _parse_pending(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        pending = _parse_pending(d.pop("pending", UNSET))

        def _parse_failed(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        failed = _parse_failed(d.pop("failed", UNSET))

        def _parse_with_findings(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        with_findings = _parse_with_findings(d.pop("with_findings", UNSET))

        def _parse_protected_ready(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        protected_ready = _parse_protected_ready(d.pop("protected_ready", UNSET))

        def _parse_protected_blocked(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        protected_blocked = _parse_protected_blocked(d.pop("protected_blocked", UNSET))

        _categories = d.pop("categories", UNSET)
        categories: PrivacyStatusCoverageCategories | Unset
        if isinstance(_categories, Unset):
            categories = UNSET
        else:
            categories = PrivacyStatusCoverageCategories.from_dict(_categories)

        _fields = d.pop("fields", UNSET)
        fields: PrivacyStatusCoverageFields | Unset
        if isinstance(_fields, Unset):
            fields = UNSET
        else:
            fields = PrivacyStatusCoverageFields.from_dict(_fields)

        failed_known = d.pop("failed_known", UNSET)

        _failures = d.pop("failures", UNSET)
        failures: list[PrivacyStatusCoverageFailuresItem] | Unset = UNSET
        if _failures is not UNSET:
            failures = []
            for failures_item_data in _failures:
                failures_item = PrivacyStatusCoverageFailuresItem.from_dict(failures_item_data)

                failures.append(failures_item)

        def _parse_next_key(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_key = _parse_next_key(d.pop("next_key", UNSET))

        privacy_status_coverage = cls(
            available=available,
            refreshing=refreshing,
            started_at_millis=started_at_millis,
            completed_at_millis=completed_at_millis,
            total=total,
            current=current,
            pending=pending,
            failed=failed,
            with_findings=with_findings,
            protected_ready=protected_ready,
            protected_blocked=protected_blocked,
            categories=categories,
            fields=fields,
            failed_known=failed_known,
            failures=failures,
            next_key=next_key,
        )

        privacy_status_coverage.additional_properties = d
        return privacy_status_coverage

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
