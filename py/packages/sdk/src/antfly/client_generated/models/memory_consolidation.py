from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_finding import MemoryFinding
    from ..models.memory_space_stats import MemorySpaceStats


T = TypeVar("T", bound="MemoryConsolidation")


@_attrs_define
class MemoryConsolidation:
    """
    Attributes:
        findings (list[MemoryFinding]):
        stats (MemorySpaceStats):
        scanned (int):
        omitted (int):
        complete (bool):
        revision (int):
        config_generation (int):
    """

    findings: list[MemoryFinding]
    stats: MemorySpaceStats
    scanned: int
    omitted: int
    complete: bool
    revision: int
    config_generation: int

    def to_dict(self) -> dict[str, Any]:
        findings = []
        for findings_item_data in self.findings:
            findings_item = findings_item_data.to_dict()
            findings.append(findings_item)

        stats = self.stats.to_dict()

        scanned = self.scanned

        omitted = self.omitted

        complete = self.complete

        revision = self.revision

        config_generation = self.config_generation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "findings": findings,
                "stats": stats,
                "scanned": scanned,
                "omitted": omitted,
                "complete": complete,
                "revision": revision,
                "config_generation": config_generation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_finding import MemoryFinding
        from ..models.memory_space_stats import MemorySpaceStats

        d = dict(src_dict)
        findings = []
        _findings = d.pop("findings")
        for findings_item_data in _findings:
            findings_item = MemoryFinding.from_dict(findings_item_data)

            findings.append(findings_item)

        stats = MemorySpaceStats.from_dict(d.pop("stats"))

        scanned = d.pop("scanned")

        omitted = d.pop("omitted")

        complete = d.pop("complete")

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        memory_consolidation = cls(
            findings=findings,
            stats=stats,
            scanned=scanned,
            omitted=omitted,
            complete=complete,
            revision=revision,
            config_generation=config_generation,
        )

        return memory_consolidation
