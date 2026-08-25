from enum import Enum


class ReauthorizeTableDestinationsResponse200Status(str, Enum):
    AUTHORIZED = "authorized"

    def __str__(self) -> str:
        return str(self.value)
