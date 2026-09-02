from enum import Enum


class DerivedCoverageObservationIncompleteReason(str, Enum):
    CONFIG_MISMATCH = "config_mismatch"
    COUNTER_MISMATCH = "counter_mismatch"
    MISSING_GROUP = "missing_group"
    REMOTE_UNKNOWN_GROUP = "remote_unknown_group"
    RUNTIME_UNAVAILABLE = "runtime_unavailable"
    STALE_GROUP = "stale_group"
    SUMMARY_UNAVAILABLE = "summary_unavailable"
    TARGET_OBSERVATION = "target_observation"
    UNKNOWN_GROUP = "unknown_group"

    def __str__(self) -> str:
        return str(self.value)
