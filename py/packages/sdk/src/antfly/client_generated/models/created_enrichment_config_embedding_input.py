from enum import StrEnum


class CreatedEnrichmentConfigEmbeddingInput(StrEnum):
    PDF_PAGE_IMAGES = "pdf_page_images"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
