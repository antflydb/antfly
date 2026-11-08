from enum import StrEnum


class PrivacyPolicyMode(StrEnum):
    MONITOR = "monitor"
    REDACT_BEFORE_STORAGE = "redact_before_storage"
    REDACT_FOR_SEARCH_AND_AI = "redact_for_search_and_ai"

    def __str__(self) -> str:
        return str(self.value)
