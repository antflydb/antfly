from enum import StrEnum


class TopologyChangedErrorError(StrEnum):
    TOPOLOGY_CHANGED = "topology_changed"

    def __str__(self) -> str:
        return str(self.value)
