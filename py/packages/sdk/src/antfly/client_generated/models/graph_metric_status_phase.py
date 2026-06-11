from enum import Enum


class GraphMetricStatusPhase(str, Enum):
    CHECK_CONVERGENCE = "check_convergence"
    CLEANUP_OLD_GENERATIONS = "cleanup_old_generations"
    COMPLETE = "complete"
    COMPUTING = "computing"
    HITS_HUB_CONTRIBUTIONS = "hits_hub_contributions"
    HITS_HUB_REDUCE_RANKS = "hits_hub_reduce_ranks"
    IDLE = "idle"
    INITIALIZE_RANKS = "initialize_ranks"
    ITERATE_CONTRIBUTIONS = "iterate_contributions"
    PREPARE_GENERATION = "prepare_generation"
    PUBLISHING = "publishing"
    PUBLISH_GENERATION = "publish_generation"
    REDUCE_RANKS = "reduce_ranks"
    SCAN_EDGES_AND_OUT_DEGREE = "scan_edges_and_out_degree"

    def __str__(self) -> str:
        return str(self.value)
