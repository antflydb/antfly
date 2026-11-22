from enum import StrEnum


class ChangefeedKeyEncoding(StrEnum):
    BASE64URL = "base64url"
    UTF8 = "utf8"

    def __str__(self) -> str:
        return str(self.value)
