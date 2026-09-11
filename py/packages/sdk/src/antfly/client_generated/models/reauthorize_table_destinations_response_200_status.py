from enum import StrEnum


class ReauthorizeTableDestinationsResponse200Status(StrEnum):
    AUTHORIZED = "authorized"

    def __str__(self) -> str:
        return str(self.value)
