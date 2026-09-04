from enum import StrEnum


class AuthSubjectKind(StrEnum):
    GROUP = "group"
    ROLE = "role"
    SUBJECT = "subject"
    USER = "user"

    def __str__(self) -> str:
        return str(self.value)
