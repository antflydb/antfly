from enum import StrEnum


class DropExtensionRequestMode(StrEnum):
    CASCADE = "cascade"
    RESTRICT = "restrict"

    def __str__(self) -> str:
        return str(self.value)
