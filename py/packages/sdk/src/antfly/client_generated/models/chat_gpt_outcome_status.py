from enum import StrEnum


class ChatGPTOutcomeStatus(StrEnum):
    CONNECTED = "connected"
    DECLINED = "declined"
    ERROR = "error"
    EXCHANGING = "exchanging"
    EXPIRED = "expired"
    PENDING = "pending"

    def __str__(self) -> str:
        return str(self.value)
