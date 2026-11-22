from enum import StrEnum


class ChangefeedCreateInclude(StrEnum):
    KEYS_AND_VERSIONS = "keys_and_versions"
    ROWS = "rows"

    def __str__(self) -> str:
        return str(self.value)
