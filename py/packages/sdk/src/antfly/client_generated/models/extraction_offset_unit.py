from enum import StrEnum


class ExtractionOffsetUnit(StrEnum):
    UNICODE_CODEPOINTS = "unicode_codepoints"
    UTF16_CODEUNITS = "utf16_codeunits"
    UTF8_BYTES = "utf8_bytes"

    def __str__(self) -> str:
        return str(self.value)
