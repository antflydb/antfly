from enum import StrEnum


class KernelJitMode(StrEnum):
    OFF = "off"
    ON = "on"
    REQUIRED = "required"
    SHADOW = "shadow"

    def __str__(self) -> str:
        return str(self.value)
