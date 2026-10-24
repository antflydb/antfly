from enum import StrEnum


class MemoryEntityCandidatePageBasis(StrEnum):
    NAME_MATCHES_DESCRIPTION_TIEBREAKER_NOT_IDENTITY_OR_CALIBRATED_CONFIDENCE = (
        "name_matches_description_tiebreaker_not_identity_or_calibrated_confidence"
    )

    def __str__(self) -> str:
        return str(self.value)
