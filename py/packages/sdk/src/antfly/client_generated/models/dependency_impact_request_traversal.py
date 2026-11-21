from enum import StrEnum


class DependencyImpactRequestTraversal(StrEnum):
    DIRECT = "direct"
    TRANSITIVE = "transitive"

    def __str__(self) -> str:
        return str(self.value)
