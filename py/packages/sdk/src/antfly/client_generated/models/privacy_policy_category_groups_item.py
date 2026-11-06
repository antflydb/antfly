from enum import StrEnum


class PrivacyPolicyCategoryGroupsItem(StrEnum):
    CONTACT_DETAILS = "contact_details"
    CREDENTIALS = "credentials"
    FINANCIAL = "financial"
    PERSONAL_IDENTIFIERS = "personal_identifiers"

    def __str__(self) -> str:
        return str(self.value)
