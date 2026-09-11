from enum import StrEnum


class AntflyRerankerConfigProvider(StrEnum):
    ANTFLY = "antfly"

    def __str__(self) -> str:
        return str(self.value)
