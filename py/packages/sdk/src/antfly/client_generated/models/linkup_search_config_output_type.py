from enum import Enum


class LinkupSearchConfigOutputType(str, Enum):
    SEARCHRESULTS = "searchResults"
    SOURCEDANSWER = "sourcedAnswer"

    def __str__(self) -> str:
        return str(self.value)
