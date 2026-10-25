from enum import StrEnum


class RelationalColumnSequenceGeneration(StrEnum):
    ALWAYS = "always"
    BY_DEFAULT = "by_default"
    SERIAL = "serial"

    def __str__(self) -> str:
        return str(self.value)
