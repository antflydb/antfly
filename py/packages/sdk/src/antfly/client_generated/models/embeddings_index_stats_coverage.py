from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="EmbeddingsIndexStatsCoverage")


@_attrs_define
class EmbeddingsIndexStatsCoverage:
    """Source document coverage accounting for embeddings indexes.

    Attributes:
        policy (str | Unset): Coverage policy used to decide readiness.
        source_total (int | Unset): Total source documents expected for coverage.
        produced (int | Unset): Source documents with produced embeddings.
        skipped (int | Unset): Source documents intentionally skipped by coverage policy.
        terminal_failed (int | Unset): Source documents that reached a terminal enrichment failure.
        covered (int | Unset): Source documents counted as covered by the coverage policy.
        complete (bool | Unset): Whether the coverage policy considers the index complete.
    """

    policy: str | Unset = UNSET
    source_total: int | Unset = UNSET
    produced: int | Unset = UNSET
    skipped: int | Unset = UNSET
    terminal_failed: int | Unset = UNSET
    covered: int | Unset = UNSET
    complete: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        policy = self.policy

        source_total = self.source_total

        produced = self.produced

        skipped = self.skipped

        terminal_failed = self.terminal_failed

        covered = self.covered

        complete = self.complete

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if policy is not UNSET:
            field_dict["policy"] = policy
        if source_total is not UNSET:
            field_dict["source_total"] = source_total
        if produced is not UNSET:
            field_dict["produced"] = produced
        if skipped is not UNSET:
            field_dict["skipped"] = skipped
        if terminal_failed is not UNSET:
            field_dict["terminal_failed"] = terminal_failed
        if covered is not UNSET:
            field_dict["covered"] = covered
        if complete is not UNSET:
            field_dict["complete"] = complete

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        policy = d.pop("policy", UNSET)

        source_total = d.pop("source_total", UNSET)

        produced = d.pop("produced", UNSET)

        skipped = d.pop("skipped", UNSET)

        terminal_failed = d.pop("terminal_failed", UNSET)

        covered = d.pop("covered", UNSET)

        complete = d.pop("complete", UNSET)

        embeddings_index_stats_coverage = cls(
            policy=policy,
            source_total=source_total,
            produced=produced,
            skipped=skipped,
            terminal_failed=terminal_failed,
            covered=covered,
            complete=complete,
        )

        return embeddings_index_stats_coverage
