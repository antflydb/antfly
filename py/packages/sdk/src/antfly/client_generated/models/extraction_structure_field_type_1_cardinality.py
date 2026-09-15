from enum import StrEnum


class ExtractionStructureFieldType1Cardinality(StrEnum):
    ONE_OR_MORE = "one_or_more"
    OPTIONAL_ONE = "optional_one"
    REQUIRED_ONE = "required_one"
    ZERO_OR_MORE = "zero_or_more"

    def __str__(self) -> str:
        return str(self.value)
