from enum import Enum


class ConnectionKind(str, Enum):
    CDC = "cdc"
    EXTERNAL_IO = "external_io"
    INFERENCE = "inference"
    WEB_SEARCH = "web_search"

    def __str__(self) -> str:
        return str(self.value)
