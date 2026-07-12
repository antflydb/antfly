from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.derived_coverage_observation_incomplete_reason import DerivedCoverageObservationIncompleteReason
from ..models.derived_coverage_status_policy import DerivedCoverageStatusPolicy

T = TypeVar("T", bound="DerivedCoverageStatus")


@_attrs_define
class DerivedCoverageStatus:
    """
    Attributes:
        policy (DerivedCoverageStatusPolicy):
        observation_complete (bool): Whether every expected shard contributed a fresh, configuration-compatible
            observation with valid outcome cardinality.
        observation_incomplete_reasons (list[DerivedCoverageObservationIncompleteReason]): Empty when
            observation_complete is true; otherwise identifies every known reason the projection is incomplete.
        config_fingerprint (str): Versioned semantic configuration fingerprint encoded as fixed-width hexadecimal. Non-
            semantic execution tuning does not affect it.
        summary_ready (bool): Whether all observed shard-local coverage summaries were read atomically and completely.
        config_mismatch_group_count (int): Freshly observed shard groups reporting a different semantic configuration
            fingerprint.
        source_total (int): Source documents observed across fresh shard reports. This is the exact table total only
            when observation_complete is true; otherwise it is a lower bound and all outcome counts are partial
            observations.
        produced (int): Source documents with a durable produced outcome for this index generation.
        skipped (int): Source documents intentionally producing no indexable output.
        terminal_failed (int): Source documents whose generation failed non-retryably.
        covered (int): Raw terminal source outcomes counted by the configured policy. This may exceed source_total only
            while observation_complete is false with counter_mismatch.
        pending (int | None): Source documents without a policy-accepted terminal outcome. Null when observations are
            incomplete and the global value is unknown.
        complete (bool): Whether observations are complete, replay has reached its target, and every observed source has
            an outcome accepted by the policy.
        healthy (bool): Whether coverage is complete without terminal failures.
        degraded (bool): Whether coverage is complete under best_effort but includes terminal failures.
    """

    policy: DerivedCoverageStatusPolicy
    observation_complete: bool
    observation_incomplete_reasons: list[DerivedCoverageObservationIncompleteReason]
    config_fingerprint: str
    summary_ready: bool
    config_mismatch_group_count: int
    source_total: int
    produced: int
    skipped: int
    terminal_failed: int
    covered: int
    pending: int | None
    complete: bool
    healthy: bool
    degraded: bool

    def to_dict(self) -> dict[str, Any]:
        policy = self.policy.value

        observation_complete = self.observation_complete

        observation_incomplete_reasons = []
        for observation_incomplete_reasons_item_data in self.observation_incomplete_reasons:
            observation_incomplete_reasons_item = observation_incomplete_reasons_item_data.value
            observation_incomplete_reasons.append(observation_incomplete_reasons_item)

        config_fingerprint = self.config_fingerprint

        summary_ready = self.summary_ready

        config_mismatch_group_count = self.config_mismatch_group_count

        source_total = self.source_total

        produced = self.produced

        skipped = self.skipped

        terminal_failed = self.terminal_failed

        covered = self.covered

        pending: int | None
        pending = self.pending

        complete = self.complete

        healthy = self.healthy

        degraded = self.degraded

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "policy": policy,
                "observation_complete": observation_complete,
                "observation_incomplete_reasons": observation_incomplete_reasons,
                "config_fingerprint": config_fingerprint,
                "summary_ready": summary_ready,
                "config_mismatch_group_count": config_mismatch_group_count,
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

        observation_complete = d.pop("observation_complete")

        observation_incomplete_reasons = []
        _observation_incomplete_reasons = d.pop("observation_incomplete_reasons")
        for observation_incomplete_reasons_item_data in _observation_incomplete_reasons:
            observation_incomplete_reasons_item = DerivedCoverageObservationIncompleteReason(
                observation_incomplete_reasons_item_data
            )

            observation_incomplete_reasons.append(observation_incomplete_reasons_item)

        config_fingerprint = d.pop("config_fingerprint")

        summary_ready = d.pop("summary_ready")

        config_mismatch_group_count = d.pop("config_mismatch_group_count")

        source_total = d.pop("source_total")

        produced = d.pop("produced")

        skipped = d.pop("skipped")

        terminal_failed = d.pop("terminal_failed")

        covered = d.pop("covered")

        def _parse_pending(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        pending = _parse_pending(d.pop("pending"))

        complete = d.pop("complete")

        healthy = d.pop("healthy")

        degraded = d.pop("degraded")

        derived_coverage_status = cls(
            policy=policy,
            observation_complete=observation_complete,
            observation_incomplete_reasons=observation_incomplete_reasons,
            config_fingerprint=config_fingerprint,
            summary_ready=summary_ready,
            config_mismatch_group_count=config_mismatch_group_count,
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
