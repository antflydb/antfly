from enum import Enum


class DropExtensionRequestMode(str, Enum):
    CASCADE = "cascade"
    RESTRICT = "restrict"

    def __str__(self) -> str:
        return str(self.value)
