from enum import Enum


class IndexSourceReadinessReason(str, Enum):
    INDEX_FAILED = "index_failed"
    PUBLICATION = "publication"
    RUNTIME_UNAVAILABLE = "runtime_unavailable"
    SHARD_OBSERVATION_INCOMPLETE = "shard_observation_incomplete"
    SOURCE_OBSERVATION_INCOMPLETE = "source_observation_incomplete"

    def __str__(self) -> str:
        return str(self.value)
