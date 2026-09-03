from enum import Enum


class AntflyEmbedderConfigProvider(str, Enum):
    ANTFLY = "antfly"

    def __str__(self) -> str:
        return str(self.value)
