from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.derived_coverage_observation_incomplete_reason import DerivedCoverageObservationIncompleteReason
from ..models.derived_coverage_status_policy import DerivedCoverageStatusPolicy

T = TypeVar("T", bound="EmbeddingSourceCoverageStatus")


@_attrs_define
class EmbeddingSourceCoverageStatus:
    """
    Attributes:
        policy (DerivedCoverageStatusPolicy):
        observation_complete (bool): Whether total and all outcome counts are exact across the expected shards.
        observation_incomplete_reasons (list[DerivedCoverageObservationIncompleteReason]):
        config_fingerprint (str): Semantic configuration fingerprint for the observed index incarnation.
        total (int): Source documents in scope. This is a lower bound when observation_complete is false.
        pending (int | None): Sources awaiting a terminal generation decision; null when the observation is incomplete.
        covered (int): Sources that durably produced material for this index incarnation.
        skipped (int): Sources intentionally producing no material after generation evaluated them.
        failed (int): Sources whose generation reached a non-retryable failure.
        complete (bool): Whether source outcomes satisfy the configured coverage policy. Replay and publication are
            reported independently by revisions and milestones.
        healthy (bool):
        degraded (bool):
    """

    policy: DerivedCoverageStatusPolicy
    observation_complete: bool
    observation_incomplete_reasons: list[DerivedCoverageObservationIncompleteReason]
    config_fingerprint: str
    total: int
    pending: int | None
    covered: int
    skipped: int
    failed: int
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

        total = self.total

        pending: int | None
        pending = self.pending

        covered = self.covered

        skipped = self.skipped

        failed = self.failed

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
                "total": total,
                "pending": pending,
                "covered": covered,
                "skipped": skipped,
                "failed": failed,
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

        total = d.pop("total")

        def _parse_pending(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        pending = _parse_pending(d.pop("pending"))

        covered = d.pop("covered")

        skipped = d.pop("skipped")

        failed = d.pop("failed")

        complete = d.pop("complete")

        healthy = d.pop("healthy")

        degraded = d.pop("degraded")

        embedding_source_coverage_status = cls(
            policy=policy,
            observation_complete=observation_complete,
            observation_incomplete_reasons=observation_incomplete_reasons,
            config_fingerprint=config_fingerprint,
            total=total,
            pending=pending,
            covered=covered,
            skipped=skipped,
            failed=failed,
            complete=complete,
            healthy=healthy,
            degraded=degraded,
        )

        return embedding_source_coverage_status
