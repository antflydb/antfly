from enum import StrEnum


class SQLRangeColumnSchemaType(StrEnum):
    DATERANGE = "daterange"
    NUMRANGE = "numrange"
    TSRANGE = "tsrange"
    TSTZRANGE = "tstzrange"

    def __str__(self) -> str:
        return str(self.value)
