from enum import Enum


class ConnectionKind(str, Enum):
    CDC = "cdc"
    EXTERNAL_IO = "external_io"
    INFERENCE = "inference"

    def __str__(self) -> str:
        return str(self.value)
