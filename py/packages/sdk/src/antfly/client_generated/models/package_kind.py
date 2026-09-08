from enum import StrEnum


class PackageKind(StrEnum):
    EXTENSION = "extension"

    def __str__(self) -> str:
        return str(self.value)
