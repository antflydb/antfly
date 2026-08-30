from enum import Enum


class IndexMutationServiceUnavailableErrorError(str, Enum):
    INDEX_CAPABILITY_UPGRADE_PENDING = "index_capability_upgrade_pending"
    INDEX_PROBE_UNAVAILABLE = "index_probe_unavailable"

    def __str__(self) -> str:
        return str(self.value)
