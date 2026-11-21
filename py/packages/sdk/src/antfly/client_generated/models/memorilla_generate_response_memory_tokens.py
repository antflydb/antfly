from enum import IntEnum


class MemorillaGenerateResponseMemoryTokens(IntEnum):
    VALUE_0 = 0
    VALUE_16 = 16
    VALUE_32 = 32
    VALUE_48 = 48
    VALUE_80 = 80

    def __str__(self) -> str:
        return str(self.value)
