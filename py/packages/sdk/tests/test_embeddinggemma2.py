"""SDK compatibility for raw similarity decisions and ordered embedding groups."""

from antfly.client_generated.models.embedding_choice_answer import EmbeddingChoiceAnswer
from antfly.client_generated.models.embedding_multi_choice_answer import EmbeddingMultiChoiceAnswer
from antfly.client_generated.models.inference_decide_response import InferenceDecideResponse
from antfly.client_generated.models.inference_embed_request import InferenceEmbedRequest
from antfly.client_generated.models.inference_embedding_group import InferenceEmbeddingGroup


def test_abstained_choice_round_trips_without_probabilities():
    answer = {
        "name": "route",
        "type": "choice",
        "choice": None,
        "decision_method": "embedding_similarity",
        "similarity_metric": "cosine",
        "similarities": [{"value": "account", "similarity": 0.2}, {"value": "billing", "similarity": 0.2}],
        "margin": 0,
        "status": "abstained",
        "abstention_reason": "tie",
        "prototype_set_hash": "a" * 64,
    }
    raw = {"model": "embeddinggemma2", "answers": [answer], "usage": {"input_tokens": 2, "output_tokens": 0}}
    parsed = InferenceDecideResponse.from_dict(raw)
    assert isinstance(parsed.answers[0], EmbeddingChoiceAnswer)
    assert parsed.to_dict() == raw


def test_batch_empty_multi_choice_is_distinct_from_abstention():
    answer = {
        "name": "tags",
        "type": "multi_choice",
        "choices": [],
        "decision_method": "embedding_similarity",
        "similarity_metric": "cosine",
        "similarities": [{"value": "a", "similarity": 0.2}, {"value": "b", "similarity": 0.1}],
        "similarity_thresholds": {"a": 0.5, "b": 0.5},
        "margin": 0.3,
        "status": "empty",
        "prototype_set_hash": "a" * 64,
    }
    raw = {
        "model": "embeddinggemma2",
        "data": [{"input_index": 0, "id": "first", "answers": [answer]}],
        "usage": {"input_tokens": 2, "output_tokens": 0},
    }
    parsed = InferenceDecideResponse.from_dict(raw)
    assert isinstance(parsed.data[0].answers[0], EmbeddingMultiChoiceAnswer)
    assert parsed.to_dict() == raw


def test_grouped_input_keeps_order_title_dimensions_and_identity():
    raw = {
        "model": "embeddinggemma2",
        "model_identity": "a" * 64,
        "dimensions": 128,
        "input": [{"title": "Access", "content": [{"type": "text", "text": "Reset password"}]}],
    }
    value = InferenceEmbedRequest.from_dict(raw)
    assert isinstance(value.input_[0], InferenceEmbeddingGroup)
    assert value.to_dict() == raw
