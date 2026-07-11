from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.derived_coverage_status_policy import DerivedCoverageStatusPolicy

T = TypeVar("T", bound="DerivedCoverageStatus")


@_attrs_define
class DerivedCoverageStatus:
    """
    Attributes:
        policy (DerivedCoverageStatusPolicy):
        source_total (int):
        produced (int): Source documents with a durable produced outcome for this index generation.
        skipped (int): Source documents intentionally producing no indexable output.
        terminal_failed (int): Source documents whose generation failed non-retryably.
        covered (int): Terminal source outcomes counted by the configured policy.
        pending (int):
        complete (bool):
        healthy (bool):
        degraded (bool):
    """

    policy: DerivedCoverageStatusPolicy
    source_total: int
    produced: int
    skipped: int
    terminal_failed: int
    covered: int
    pending: int
    complete: bool
    healthy: bool
    degraded: bool

    def to_dict(self) -> dict[str, Any]:
        policy = self.policy.value

        source_total = self.source_total

        produced = self.produced

        skipped = self.skipped

        terminal_failed = self.terminal_failed

        covered = self.covered

        pending = self.pending

        complete = self.complete

        healthy = self.healthy

        degraded = self.degraded

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "policy": policy,
                "source_total": source_total,
                "produced": produced,
                "skipped": skipped,
                "terminal_failed": terminal_failed,
                "covered": covered,
                "pending": pending,
                "complete": complete,
                "healthy": healthy,
                "degraded": degraded,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        policy = DerivedCoverageStatusPolicy(d.pop("policy"))

        source_total = d.pop("source_total")

        produced = d.pop("produced")

        skipped = d.pop("skipped")

        terminal_failed = d.pop("terminal_failed")

        covered = d.pop("covered")

        pending = d.pop("pending")

        complete = d.pop("complete")

        healthy = d.pop("healthy")

        degraded = d.pop("degraded")

        derived_coverage_status = cls(
            policy=policy,
            source_total=source_total,
            produced=produced,
            skipped=skipped,
            terminal_failed=terminal_failed,
            covered=covered,
            pending=pending,
            complete=complete,
            healthy=healthy,
            degraded=degraded,
        )

        return derived_coverage_status
