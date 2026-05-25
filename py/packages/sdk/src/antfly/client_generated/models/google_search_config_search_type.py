from enum import Enum


class GoogleSearchConfigSearchType(str, Enum):
    IMAGE = "image"
    WEB = "web"

    def __str__(self) -> str:
        return str(self.value)
