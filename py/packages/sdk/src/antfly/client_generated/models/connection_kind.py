from enum import Enum


class ConnectionKind(str, Enum):
    CDC = "cdc"
    INFERENCE = "inference"
    OBJECT_STORE = "object_store"
    REMOTE_CONTENT = "remote_content"

    def __str__(self) -> str:
        return str(self.value)
