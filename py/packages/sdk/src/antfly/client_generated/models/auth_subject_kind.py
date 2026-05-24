from enum import Enum


class AuthSubjectKind(str, Enum):
    GROUP = "group"
    ROLE = "role"
    SUBJECT = "subject"
    USER = "user"

    def __str__(self) -> str:
        return str(self.value)
