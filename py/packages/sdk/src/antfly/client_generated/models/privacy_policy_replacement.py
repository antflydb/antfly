from enum import StrEnum


class PrivacyPolicyReplacement(StrEnum):
    CATEGORY = "category"
    MASK = "mask"
    REMOVE = "remove"

    def __str__(self) -> str:
        return str(self.value)
