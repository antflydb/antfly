from enum import StrEnum


class ConnectionStatus(StrEnum):
    CONFIGURED = "configured"
    CONNECTED = "connected"
    ERROR = "error"
    UNSUPPORTED = "unsupported"

    def __str__(self) -> str:
        return str(self.value)
