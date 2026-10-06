from enum import StrEnum


class ResearchSubQuestionSourcesItem(StrEnum):
    TABLES = "tables"
    WEB = "web"

    def __str__(self) -> str:
        return str(self.value)
