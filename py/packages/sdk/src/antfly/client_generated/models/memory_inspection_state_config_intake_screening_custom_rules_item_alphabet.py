from enum import StrEnum


class MemoryInspectionStateConfigIntakeScreeningCustomRulesItemAlphabet(StrEnum):
    ALPHANUMERIC = "alphanumeric"
    BASE64URL = "base64url"
    HEX = "hex"
    NON_SEPARATOR = "non_separator"

    def __str__(self) -> str:
        return str(self.value)
