from enum import StrEnum


class DocumentExtractionRouteExtractorType(StrEnum):
    ARCHIVE = "archive"
    AUDIO = "audio"
    DOCX = "docx"
    EMAIL = "email"
    HTML = "html"
    IMAGE = "image"
    OCR = "ocr"
    PDF = "pdf"
    PPTX = "pptx"
    TEXT = "text"
    TRANSCRIPT = "transcript"
    UNSUPPORTED = "unsupported"
    XLSX = "xlsx"

    def __str__(self) -> str:
        return str(self.value)
