import pytest

from antfly.client_generated.api.default.create_embedding import _get_kwargs
from antfly.client_generated.models import (
    InferenceEmbedRequest,
    InferenceEmbeddingContentInput,
    MediaContentPart,
    MediaContentPartType,
    TextContentPart,
    TextContentPartType,
)


TEXT = TextContentPart(type_=TextContentPartType.TEXT, text="ants")
AUDIO = MediaContentPart(type_=MediaContentPartType.MEDIA, data="AA==", mime_type="audio/wav")


@pytest.mark.parametrize(
    ("input_value", "wire_value", "expected_type"),
    [
        ("ants", "ants", str),
        (["ants", "bees"], ["ants", "bees"], list),
        ([TEXT, AUDIO], [TEXT.to_dict(), AUDIO.to_dict()], list),
        (
            InferenceEmbeddingContentInput(content=[TEXT, AUDIO]),
            {"content": [TEXT.to_dict(), AUDIO.to_dict()]},
            InferenceEmbeddingContentInput,
        ),
        (
            [
                InferenceEmbeddingContentInput(content=[TEXT]),
                InferenceEmbeddingContentInput(content=[AUDIO]),
            ],
            [{"content": [TEXT.to_dict()]}, {"content": [AUDIO.to_dict()]}],
            list,
        ),
    ],
)
def test_embedding_input_union_transport_and_round_trip(input_value, wire_value, expected_type) -> None:
    request = InferenceEmbedRequest(model="embeddinggemma-2", input_=input_value)
    wire = _get_kwargs(body=request)["json"]
    assert wire["input"] == wire_value

    parsed = InferenceEmbedRequest.from_dict(wire)
    assert isinstance(parsed.input_, expected_type)
    assert parsed.to_dict() == wire


@pytest.mark.parametrize(
    "invalid",
    [
        ["ants", {"content": []}],
        [{"type": "text", "text": "ants"}, {"content": []}],
        [{"type": "unknown"}],
    ],
)
def test_embedding_input_union_rejects_ambiguous_lists(invalid) -> None:
    with pytest.raises(TypeError):
        InferenceEmbedRequest.from_dict({"model": "embeddinggemma-2", "input": invalid})
