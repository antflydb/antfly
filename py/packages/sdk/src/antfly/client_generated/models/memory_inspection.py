from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_inspection_capabilities import MemoryInspectionCapabilities
    from ..models.memory_inspection_state import MemoryInspectionState


T = TypeVar("T", bound="MemoryInspection")


@_attrs_define
class MemoryInspection:
    """
    Attributes:
        state (MemoryInspectionState):
        capabilities (MemoryInspectionCapabilities):
    """

    state: MemoryInspectionState
    capabilities: MemoryInspectionCapabilities

    def to_dict(self) -> dict[str, Any]:
        state = self.state.to_dict()

        capabilities = self.capabilities.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "state": state,
                "capabilities": capabilities,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_inspection_capabilities import MemoryInspectionCapabilities
        from ..models.memory_inspection_state import MemoryInspectionState

        d = dict(src_dict)
        state = MemoryInspectionState.from_dict(d.pop("state"))

        capabilities = MemoryInspectionCapabilities.from_dict(d.pop("capabilities"))

        memory_inspection = cls(
            state=state,
            capabilities=capabilities,
        )

        return memory_inspection
