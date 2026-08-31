from enum import Enum


class GraphDistinctBudgetExceededErrorDimension(str, Enum):
    DISTINCT_IDENTITIES = "distinct_identities"
    DISTINCT_STATE_BYTES = "distinct_state_bytes"

    def __str__(self) -> str:
        return str(self.value)
