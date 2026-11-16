from enum import StrEnum


class LeannIndexConfigQuantizer(StrEnum):
    PQ = "pq"
    RABITQ = "rabitq"

    def __str__(self) -> str:
        return str(self.value)
