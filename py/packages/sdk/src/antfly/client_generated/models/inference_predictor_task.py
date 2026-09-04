from enum import StrEnum


class InferencePredictorTask(StrEnum):
    BINARY_CLASSIFICATION = "binary_classification"
    MULTICLASS = "multiclass"
    RANKING = "ranking"
    REGRESSION = "regression"

    def __str__(self) -> str:
        return str(self.value)
