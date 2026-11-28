from enum import StrEnum


class DocumentArtifactReprocessRetryStage(StrEnum):
    ENRICHMENT = "enrichment"
    OCR = "ocr"
    PDF_INSPECT = "pdf_inspect"
    PDF_PREPARE = "pdf_prepare"
    PDF_RENDER = "pdf_render"
    PUBLICATION = "publication"
    SOURCE_FETCH = "source_fetch"

    def __str__(self) -> str:
        return str(self.value)
