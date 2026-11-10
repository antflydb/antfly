from enum import StrEnum


class RetainedDatasetInfoInitialHistory(StrEnum):
    CERTIFIED = "certified"
    RETAINED_SOURCE_ROWS = "retained_source_rows"

    def __str__(self) -> str:
        return str(self.value)
