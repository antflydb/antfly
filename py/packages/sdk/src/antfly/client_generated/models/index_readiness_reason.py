from enum import Enum


class IndexReadinessReason(str, Enum):
    BACKFILL = "backfill"
    COVERAGE = "coverage"
    ENRICHMENT_FAILURE = "enrichment_failure"
    INCARNATION_PENDING = "incarnation_pending"
    LOAD_FAILURE = "load_failure"
    PUBLICATION = "publication"
    REPAIR = "repair"
    REPLAY = "replay"
    RUNTIME_UNAVAILABLE = "runtime_unavailable"
    SHARD_OBSERVATION_INCOMPLETE = "shard_observation_incomplete"
    SOURCE_PUBLICATION = "source_publication"

    def __str__(self) -> str:
        return str(self.value)
