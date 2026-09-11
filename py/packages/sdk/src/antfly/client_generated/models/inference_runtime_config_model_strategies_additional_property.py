from enum import StrEnum


class InferenceRuntimeConfigModelStrategiesAdditionalProperty(StrEnum):
    BOUNDED = "bounded"
    EAGER = "eager"
    LAZY = "lazy"

    def __str__(self) -> str:
        return str(self.value)
