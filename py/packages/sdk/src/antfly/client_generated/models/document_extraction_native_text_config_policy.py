from enum import StrEnum


class DocumentExtractionNativeTextConfigPolicy(StrEnum):
    PAGE = "page"
    SOURCE = "source"
    VISIBLE = "visible"

    def __str__(self) -> str:
        return str(self.value)
