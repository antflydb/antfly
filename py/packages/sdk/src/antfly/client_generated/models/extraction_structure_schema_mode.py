from enum import StrEnum


class ExtractionStructureSchemaMode(StrEnum):
    ANCHORLESS = "anchorless"
    LATENT = "latent"
    NATURAL = "natural"

    def __str__(self) -> str:
        return str(self.value)
