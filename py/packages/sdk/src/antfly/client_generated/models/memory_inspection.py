from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_inspection_capabilities import MemoryInspectionCapabilities
    from ..models.memory_inspection_state import MemoryInspectionState
    from ..models.memory_statistics import MemoryStatistics


T = TypeVar("T", bound="MemoryInspection")


@_attrs_define
class MemoryInspection:
    """
    Attributes:
        state (MemoryInspectionState):
        capabilities (MemoryInspectionCapabilities):
        statistics (MemoryStatistics | Unset):
    """

    state: MemoryInspectionState
    capabilities: MemoryInspectionCapabilities
    statistics: MemoryStatistics | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        state = self.state.to_dict()

        capabilities = self.capabilities.to_dict()

        statistics: dict[str, Any] | Unset = UNSET
        if not isinstance(self.statistics, Unset):
            statistics = self.statistics.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "state": state,
                "capabilities": capabilities,
            }
        )
        if statistics is not UNSET:
            field_dict["statistics"] = statistics

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_inspection_capabilities import MemoryInspectionCapabilities
        from ..models.memory_inspection_state import MemoryInspectionState
        from ..models.memory_statistics import MemoryStatistics

        d = dict(src_dict)
        state = MemoryInspectionState.from_dict(d.pop("state"))

        capabilities = MemoryInspectionCapabilities.from_dict(d.pop("capabilities"))

        _statistics = d.pop("statistics", UNSET)
        statistics: MemoryStatistics | Unset
        if isinstance(_statistics, Unset):
            statistics = UNSET
        else:
            statistics = MemoryStatistics.from_dict(_statistics)

        memory_inspection = cls(
            state=state,
            capabilities=capabilities,
            statistics=statistics,
        )

        return memory_inspection
