from enum import StrEnum


class MemoryIntakeScreeningCustomRulesItemAlphabet(StrEnum):
    ALPHANUMERIC = "alphanumeric"
    BASE64URL = "base64url"
    HEX = "hex"
    NON_SEPARATOR = "non_separator"

    def __str__(self) -> str:
        return str(self.value)
