from enum import Enum


class EvaluatorName(str, Enum):
    CITATION_QUALITY = "citation_quality"
    COHERENCE = "coherence"
    COMPLETENESS = "completeness"
    CORRECTNESS = "correctness"
    FAITHFULNESS = "faithfulness"
    HELPFULNESS = "helpfulness"
    MAP = "map"
    MRR = "mrr"
    NDCG = "ndcg"
    PRECISION = "precision"
    RECALL = "recall"
    RELEVANCE = "relevance"
    SAFETY = "safety"

    def __str__(self) -> str:
        return str(self.value)
