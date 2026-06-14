from enum import Enum


class ConnectionStatus(str, Enum):
    CONFIGURED = "configured"
    CONNECTED = "connected"
    ERROR = "error"
    UNSUPPORTED = "unsupported"

    def __str__(self) -> str:
        return str(self.value)
