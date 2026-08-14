from enum import Enum


class InferenceRuntimeConfigModelStrategiesAdditionalProperty(str, Enum):
    BOUNDED = "bounded"
    EAGER = "eager"
    LAZY = "lazy"

    def __str__(self) -> str:
        return str(self.value)
