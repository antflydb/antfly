from enum import Enum


class VertexSearchConfigService(str, Enum):
    AGENT_SEARCH = "agent_search"

    def __str__(self) -> str:
        return str(self.value)
