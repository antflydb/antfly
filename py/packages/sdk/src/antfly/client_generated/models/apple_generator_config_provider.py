from enum import StrEnum


class AppleGeneratorConfigProvider(StrEnum):
    APPLE = "apple"

    def __str__(self) -> str:
        return str(self.value)
