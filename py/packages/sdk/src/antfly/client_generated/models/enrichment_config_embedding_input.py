from enum import StrEnum


class EnrichmentConfigEmbeddingInput(StrEnum):
    PDF_PAGE_IMAGES = "pdf_page_images"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
