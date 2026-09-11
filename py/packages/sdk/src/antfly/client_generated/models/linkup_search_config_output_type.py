from enum import StrEnum


class LinkupSearchConfigOutputType(StrEnum):
    SEARCHRESULTS = "searchResults"
    SOURCEDANSWER = "sourcedAnswer"

    def __str__(self) -> str:
        return str(self.value)
