from enum import Enum


class GraphArtifactProducerSourceConfigType(str, Enum):
    FIELD = "field"
    TEMPLATE = "template"

    def __str__(self) -> str:
        return str(self.value)
