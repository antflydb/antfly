from enum import StrEnum


class ProvenanceEnvelopeLinksItemRelation(StrEnum):
    APPLIED_TO = "applied_to"
    APPROVED_BY = "approved_by"
    DEPENDS_ON = "depends_on"
    GENERATED = "generated"
    GENERATED_BY = "generated_by"
    GOVERNED_BY = "governed_by"
    MENTIONS = "mentions"
    MOTIVATES = "motivates"
    ORIGINATED_IN = "originated_in"
    REJECTED_IN_FAVOR_OF = "rejected_in_favor_of"
    RETRIEVED = "retrieved"
    SUPERSEDES = "supersedes"
    SUPPORTED_BY = "supported_by"
    USED = "used"

    def __str__(self) -> str:
        return str(self.value)
