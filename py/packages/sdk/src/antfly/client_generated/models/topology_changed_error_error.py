from enum import Enum


class TopologyChangedErrorError(str, Enum):
    TOPOLOGY_CHANGED = "topology_changed"

    def __str__(self) -> str:
        return str(self.value)
