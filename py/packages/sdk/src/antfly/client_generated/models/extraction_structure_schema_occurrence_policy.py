from enum import StrEnum


class ExtractionStructureSchemaOccurrencePolicy(StrEnum):
    ALL = "all"
    ERROR_ON_AMBIGUOUS = "error_on_ambiguous"
    FIRST = "first"
    LATENT_ALL = "latent_all"

    def __str__(self) -> str:
        return str(self.value)
