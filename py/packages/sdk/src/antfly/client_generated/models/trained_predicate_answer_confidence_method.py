from enum import StrEnum


class TrainedPredicateAnswerConfidenceMethod(StrEnum):
    MAX_PROBABILITY = "max_probability"
    NORMALIZED_INVERSE_ENTROPY = "normalized_inverse_entropy"

    def __str__(self) -> str:
        return str(self.value)
