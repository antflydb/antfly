from enum import Enum


class InferenceConfigModelStrategiesAdditionalProperty(str, Enum):
    BOUNDED = "bounded"
    EAGER = "eager"
    LAZY = "lazy"

    def __str__(self) -> str:
        return str(self.value)
