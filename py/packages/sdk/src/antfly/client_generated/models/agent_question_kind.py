from enum import StrEnum


class AgentQuestionKind(StrEnum):
    CONFIRM = "confirm"
    FIELD_POLICY = "field_policy"
    FREE_TEXT = "free_text"
    MULTI_CHOICE = "multi_choice"
    SINGLE_CHOICE = "single_choice"

    def __str__(self) -> str:
        return str(self.value)
