from enum import StrEnum


class DocumentArtifactRetryBacklogAdmissionBlockedBy(StrEnum):
    BACKLOG_BYTE_LIMIT = "backlog_byte_limit"
    BACKLOG_TICKET_LIMIT = "backlog_ticket_limit"
    DOCUMENT_WORKSET = "document_workset"

    def __str__(self) -> str:
        return str(self.value)
