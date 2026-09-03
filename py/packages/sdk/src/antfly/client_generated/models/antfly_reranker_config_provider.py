from enum import Enum


class AntflyRerankerConfigProvider(str, Enum):
    ANTFLY = "antfly"

    def __str__(self) -> str:
        return str(self.value)
