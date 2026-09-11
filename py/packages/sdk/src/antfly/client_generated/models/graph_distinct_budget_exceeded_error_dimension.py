from enum import StrEnum


class GraphDistinctBudgetExceededErrorDimension(StrEnum):
    DISTINCT_IDENTITIES = "distinct_identities"
    DISTINCT_STATE_BYTES = "distinct_state_bytes"

    def __str__(self) -> str:
        return str(self.value)
