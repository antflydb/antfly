from enum import StrEnum


class VertexSearchConfigService(StrEnum):
    AGENT_SEARCH = "agent_search"

    def __str__(self) -> str:
        return str(self.value)
