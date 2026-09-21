from enum import StrEnum


class LinkupSearchConfigProvider(StrEnum):
    LINKUP = "linkup"

    def __str__(self) -> str:
        return str(self.value)
