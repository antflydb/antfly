from enum import StrEnum


class AntflyGeneratorConfigProvider(StrEnum):
    ANTFLY = "antfly"

    def __str__(self) -> str:
        return str(self.value)
