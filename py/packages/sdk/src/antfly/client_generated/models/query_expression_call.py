from enum import StrEnum


class QueryExpressionCall(StrEnum):
    AI_CHOICE = "ai_choice"
    AI_DECIDE = "ai_decide"
    AI_PROBABILITY = "ai_probability"
    AI_SCORE = "ai_score"

    def __str__(self) -> str:
        return str(self.value)
