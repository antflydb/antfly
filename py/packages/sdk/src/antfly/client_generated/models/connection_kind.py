from enum import StrEnum


class ConnectionKind(StrEnum):
    CDC = "cdc"
    EXTERNAL_IO = "external_io"
    INFERENCE = "inference"
    WEB_SEARCH = "web_search"

    def __str__(self) -> str:
        return str(self.value)
