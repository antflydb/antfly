from enum import StrEnum


class MemoryAssertionKind(StrEnum):
    ALTERNATIVE = "alternative"
    CONSTRAINT = "constraint"
    DECISION = "decision"
    EPISODIC = "episodic"
    LESSON = "lesson"
    OPEN_QUESTION = "open_question"
    PREFERENCE = "preference"
    PROCEDURAL = "procedural"
    RATIONALE = "rationale"
    SEMANTIC = "semantic"
    SUMMARY = "summary"

    def __str__(self) -> str:
        return str(self.value)
