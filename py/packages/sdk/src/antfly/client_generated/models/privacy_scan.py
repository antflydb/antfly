from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.privacy_scan_phase import PrivacyScanPhase
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.privacy_scan_failures_type_0_item import PrivacyScanFailuresType0Item


T = TypeVar("T", bound="PrivacyScan")


@_attrs_define
class PrivacyScan:
    """
    Attributes:
        scan_id (str):
        policy_version (str):
        phase (PrivacyScanPhase):
        scanned (int | Unset):
        reprocessed (int | Unset): Documents scheduled for inspection; processing verifies current receipts and
            publication before success.
        failed (int | Unset):
        next_key (None | str | Unset): Reserved internal checkpoint; omitted from public scan status.
        skipped (int | Unset):
        last_error (None | str | Unset):
        consecutive_errors (int | Unset):
        retry_after_millis (int | Unset):
        updated_at_millis (int | Unset):
        verification_cursor (None | str | Unset): Reserved internal checkpoint; omitted from public scan status.
        verification_current (int | Unset):
        verification_pending (int | Unset):
        verification_failed (int | Unset):
        verification_blocked (int | Unset):
        failures (list[PrivacyScanFailuresType0Item] | None | Unset): Optional failure examples, limited to 100 entries
            and 1 MiB of serialized JSON. Failure counts include rows whose identifiers exceed this detail budget.
    """

    scan_id: str
    policy_version: str
    phase: PrivacyScanPhase
    scanned: int | Unset = UNSET
    reprocessed: int | Unset = UNSET
    failed: int | Unset = UNSET
    next_key: None | str | Unset = UNSET
    skipped: int | Unset = UNSET
    last_error: None | str | Unset = UNSET
    consecutive_errors: int | Unset = UNSET
    retry_after_millis: int | Unset = UNSET
    updated_at_millis: int | Unset = UNSET
    verification_cursor: None | str | Unset = UNSET
    verification_current: int | Unset = UNSET
    verification_pending: int | Unset = UNSET
    verification_failed: int | Unset = UNSET
    verification_blocked: int | Unset = UNSET
    failures: list[PrivacyScanFailuresType0Item] | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        scan_id = self.scan_id

        policy_version = self.policy_version

        phase = self.phase.value

        scanned = self.scanned

        reprocessed = self.reprocessed

        failed = self.failed

        next_key: None | str | Unset
        if isinstance(self.next_key, Unset):
            next_key = UNSET
        else:
            next_key = self.next_key

        skipped = self.skipped

        last_error: None | str | Unset
        if isinstance(self.last_error, Unset):
            last_error = UNSET
        else:
            last_error = self.last_error

        consecutive_errors = self.consecutive_errors

        retry_after_millis = self.retry_after_millis

        updated_at_millis = self.updated_at_millis

        verification_cursor: None | str | Unset
        if isinstance(self.verification_cursor, Unset):
            verification_cursor = UNSET
        else:
            verification_cursor = self.verification_cursor

        verification_current = self.verification_current

        verification_pending = self.verification_pending

        verification_failed = self.verification_failed

        verification_blocked = self.verification_blocked

        failures: list[dict[str, Any]] | None | Unset
        if isinstance(self.failures, Unset):
            failures = UNSET
        elif isinstance(self.failures, list):
            failures = []
            for failures_type_0_item_data in self.failures:
                failures_type_0_item = failures_type_0_item_data.to_dict()
                failures.append(failures_type_0_item)

        else:
            failures = self.failures

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "scan_id": scan_id,
                "policy_version": policy_version,
                "phase": phase,
            }
        )
        if scanned is not UNSET:
            field_dict["scanned"] = scanned
        if reprocessed is not UNSET:
            field_dict["reprocessed"] = reprocessed
        if failed is not UNSET:
            field_dict["failed"] = failed
        if next_key is not UNSET:
            field_dict["next_key"] = next_key
        if skipped is not UNSET:
            field_dict["skipped"] = skipped
        if last_error is not UNSET:
            field_dict["last_error"] = last_error
        if consecutive_errors is not UNSET:
            field_dict["consecutive_errors"] = consecutive_errors
        if retry_after_millis is not UNSET:
            field_dict["retry_after_millis"] = retry_after_millis
        if updated_at_millis is not UNSET:
            field_dict["updated_at_millis"] = updated_at_millis
        if verification_cursor is not UNSET:
            field_dict["verification_cursor"] = verification_cursor
        if verification_current is not UNSET:
            field_dict["verification_current"] = verification_current
        if verification_pending is not UNSET:
            field_dict["verification_pending"] = verification_pending
        if verification_failed is not UNSET:
            field_dict["verification_failed"] = verification_failed
        if verification_blocked is not UNSET:
            field_dict["verification_blocked"] = verification_blocked
        if failures is not UNSET:
            field_dict["failures"] = failures

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.privacy_scan_failures_type_0_item import PrivacyScanFailuresType0Item

        d = dict(src_dict)
        scan_id = d.pop("scan_id")

        policy_version = d.pop("policy_version")

        phase = PrivacyScanPhase(d.pop("phase"))

        scanned = d.pop("scanned", UNSET)

        reprocessed = d.pop("reprocessed", UNSET)

        failed = d.pop("failed", UNSET)

        def _parse_next_key(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_key = _parse_next_key(d.pop("next_key", UNSET))

        skipped = d.pop("skipped", UNSET)

        def _parse_last_error(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_error = _parse_last_error(d.pop("last_error", UNSET))

        consecutive_errors = d.pop("consecutive_errors", UNSET)

        retry_after_millis = d.pop("retry_after_millis", UNSET)

        updated_at_millis = d.pop("updated_at_millis", UNSET)

        def _parse_verification_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        verification_cursor = _parse_verification_cursor(d.pop("verification_cursor", UNSET))

        verification_current = d.pop("verification_current", UNSET)

        verification_pending = d.pop("verification_pending", UNSET)

        verification_failed = d.pop("verification_failed", UNSET)

        verification_blocked = d.pop("verification_blocked", UNSET)

        def _parse_failures(data: object) -> list[PrivacyScanFailuresType0Item] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                failures_type_0 = []
                _failures_type_0 = data
                for failures_type_0_item_data in _failures_type_0:
                    failures_type_0_item = PrivacyScanFailuresType0Item.from_dict(failures_type_0_item_data)

                    failures_type_0.append(failures_type_0_item)

                return failures_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[PrivacyScanFailuresType0Item] | None | Unset, data)

        failures = _parse_failures(d.pop("failures", UNSET))

        privacy_scan = cls(
            scan_id=scan_id,
            policy_version=policy_version,
            phase=phase,
            scanned=scanned,
            reprocessed=reprocessed,
            failed=failed,
            next_key=next_key,
            skipped=skipped,
            last_error=last_error,
            consecutive_errors=consecutive_errors,
            retry_after_millis=retry_after_millis,
            updated_at_millis=updated_at_millis,
            verification_cursor=verification_cursor,
            verification_current=verification_current,
            verification_pending=verification_pending,
            verification_failed=verification_failed,
            verification_blocked=verification_blocked,
            failures=failures,
        )

        privacy_scan.additional_properties = d
        return privacy_scan

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
