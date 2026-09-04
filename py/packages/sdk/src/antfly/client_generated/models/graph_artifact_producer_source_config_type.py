from enum import StrEnum


class GraphArtifactProducerSourceConfigType(StrEnum):
    FIELD = "field"
    TEMPLATE = "template"

    def __str__(self) -> str:
        return str(self.value)
