from enum import StrEnum


class LeannIndexConfigVectorStorage(StrEnum):
    AUTO = "auto"
    PERSISTENT = "persistent"
    TRANSIENT = "transient"

    def __str__(self) -> str:
        return str(self.value)
