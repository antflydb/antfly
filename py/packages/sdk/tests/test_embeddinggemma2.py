"""SDK compatibility for raw similarity decisions and ordered embedding groups."""

from antfly.client_generated.models.embedding_extraction_decision import EmbeddingExtractionDecision
from antfly.client_generated.models.extraction_object import ExtractionObject
from antfly.client_generated.models.inference_decide_answer import InferenceDecideAnswer
from antfly.client_generated.models.inference_embed_request import InferenceEmbedRequest
from antfly.client_generated.models.inference_embedding_group import InferenceEmbeddingGroup
from antfly.client_generated.models.trained_extraction_decision import TrainedExtractionDecision


def test_abstained_choice_round_trips_without_probabilities():
    raw = {"type": "choice", "choice": None, "decision_method": "embedding_similarity",
           "similarities": {"account": 0.2, "billing": 0.2}, "margin": 0,
           "status": "abstained", "abstention_reason": "tie", "prototype_set_hash": "a" * 64}
    assert InferenceDecideAnswer.from_dict(raw).to_dict() == raw


def test_extraction_preserves_both_decision_contracts():
    similarity = {"name": "tags", "mode": "multi", "decision_method": "embedding_similarity",
                  "labels": ["account"], "similarities": {"account": 0.7, "billing": 0.1}, "status": "selected"}
    trained = {"name": "route", "type": "choice", "label": "account", "probabilities": [],
               "confidence": 0.9, "confidence_method": "max_probability"}
    value = ExtractionObject.from_dict({"decisions": [similarity, trained]})
    assert isinstance(value.decisions[0], EmbeddingExtractionDecision)
    assert isinstance(value.decisions[1], TrainedExtractionDecision)
    assert value.to_dict() == {"decisions": [similarity, trained]}


def test_grouped_input_keeps_order_title_dimensions_and_identity():
    raw = {"model": "embeddinggemma2", "model_identity": "a" * 64, "dimensions": 128,
           "input": [{"title": "Access", "content": [{"type": "text", "text": "Reset password"}]}]}
    value = InferenceEmbedRequest.from_dict(raw)
    assert isinstance(value.input_[0], InferenceEmbeddingGroup)
    assert value.to_dict() == raw


def test_historical_trained_model_imports_remain_compatible():
    from antfly.client_generated.models.extraction_decision import ExtractionDecision
    assert ExtractionDecision is TrainedExtractionDecision
