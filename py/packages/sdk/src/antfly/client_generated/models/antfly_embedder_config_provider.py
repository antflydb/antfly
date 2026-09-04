from enum import StrEnum


class AntflyEmbedderConfigProvider(StrEnum):
    ANTFLY = "antfly"

    def __str__(self) -> str:
        return str(self.value)
