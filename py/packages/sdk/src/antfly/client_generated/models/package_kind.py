from enum import Enum


class PackageKind(str, Enum):
    EXTENSION = "extension"

    def __str__(self) -> str:
        return str(self.value)
