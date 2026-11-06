from enum import StrEnum


class PrivacyPolicySensitivity(StrEnum):
    BALANCED = "balanced"
    CONSERVATIVE = "conservative"
    THOROUGH = "thorough"

    def __str__(self) -> str:
        return str(self.value)
