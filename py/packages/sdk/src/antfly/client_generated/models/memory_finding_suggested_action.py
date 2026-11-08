from enum import StrEnum


class MemoryFindingSuggestedAction(StrEnum):
    ADD_VALID_FROM = "add_valid_from"
    CORRECT = "correct"
    CURATE_ENTITY = "curate_entity"
    FIX_LINK = "fix_link"
    PIN = "pin"
    PROPOSE_SUPERSESSION = "propose_supersession"
    REVIEW_SUPERSESSION = "review_supersession"
    VERIFY = "verify"

    def __str__(self) -> str:
        return str(self.value)
