from enum import Enum


class TermiteConfigModelStrategiesAdditionalProperty(str, Enum):
    BOUNDED = "bounded"
    EAGER = "eager"
    LAZY = "lazy"

    def __str__(self) -> str:
        return str(self.value)
