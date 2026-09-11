import json

import httpx
import pytest

from antfly import AntflyClient, AntflyException, InferenceAPIError
from antfly.client_generated.models import (
    ExtractionClassificationSchema,
    ExtractionClassificationSchemaMode,
    ExtractionDecoderOptions,
    ExtractionDecoderOptionsAlgorithm,
    ExtractionOptions,
    ExtractionOptionsWordSplitter,
    ExtractionRequest,
)


def client_with_transport(handler, **kwargs):
    client = AntflyClient("http://test", **kwargs)
    client._client.set_httpx_client(httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)))
    return client


def test_decoder_construction_preserves_model_default_and_explicit_algorithms():
    # A constructor must not turn omitted settings into an explicit native
    # selector: JointIE's model default has distinct source-compatible behavior.
    assert "algorithm" not in ExtractionDecoderOptions(beam_width=16).to_dict()
    assert ExtractionDecoderOptions.from_dict({"beam_width": 16}).to_dict() == {"beam_width": 16}
    for algorithm in ExtractionDecoderOptionsAlgorithm:
        wire = ExtractionDecoderOptions(algorithm=algorithm, beam_width=16).to_dict()
        assert wire["algorithm"] == algorithm.value
        assert ExtractionDecoderOptions.from_dict(wire).to_dict() == wire


def test_classification_construction_preserves_omitted_and_explicit_legacy_options():
    # Structured classification rejects even explicit top_k=1. Constructors
    # must leave the server default implicit so advanced schemas stay usable.
    for mode in ExtractionClassificationSchemaMode:
        schema = ExtractionClassificationSchema(name="topic", labels=["a", "b"], mode=mode)
        assert "top_k" not in schema.to_dict()
        assert "hypothesis_template" not in schema.to_dict()
        assert "multi_label" not in schema.to_dict()
        assert "top_k" not in ExtractionClassificationSchema.from_dict(schema.to_dict()).to_dict()
    for top_k in (1, 2):
        schema = ExtractionClassificationSchema(name="topic", labels=["a", "b"], top_k=top_k)
        assert schema.to_dict()["top_k"] == top_k
        assert ExtractionClassificationSchema.from_dict(schema.to_dict()).to_dict()["top_k"] == top_k
    schema = ExtractionClassificationSchema(name="topic", labels=["a", "b"], hypothesis_template="Topic: {}")
    assert schema.to_dict()["hypothesis_template"] == "Topic: {}"
    assert ExtractionClassificationSchema.from_dict(schema.to_dict()).to_dict()["hypothesis_template"] == "Topic: {}"
    for multi_label in (False, True):
        schema = ExtractionClassificationSchema(name="topic", labels=["a", "b"], multi_label=multi_label)
        assert schema.to_dict()["multi_label"] is multi_label
        assert ExtractionClassificationSchema.from_dict(schema.to_dict()).to_dict()["multi_label"] is multi_label


def test_extraction_v2_preserves_omission_null_false_and_empty_replacements():
    request = {
        "model": "m",
        "schema": {"classifications": [{"name": "t", "labels": ["a", "b"], "max_labels": None, "ordered": False}]},
        "options": {"threshold": 0, "include_spans": False, "word_splitter": "char"},
        "inputs": [{"content": "Ada", "options": {}}, {"content": "Bob"}],
    }

    def handle(http_request):
        assert http_request.url.path == "/ai/v1/extract"
        assert json.loads(http_request.read()) == {**request, "schema_version": 2}
        return httpx.Response(
            200,
            json={
                "object": "extraction",
                "model": "m",
                "schema_version": 2,
                "data": [{"offset_unit": "utf8_bytes"}, {"offset_unit": "utf8_bytes"}],
            },
        )

    client = client_with_transport(handle)
    assert len(client.extract_v2(request).data) == 2
    assert "schema_version" not in request
    # Generated from_dict keeps omission; constructor defaults remain legacy.
    assert ExtractionRequest.from_dict({**request, "schema_version": 2}).to_dict() == {**request, "schema_version": 2}
    # Constructing legacy options must not silently introduce a V2-only option.
    assert ExtractionOptions().to_dict() == {}
    assert ExtractionOptions(word_splitter=ExtractionOptionsWordSplitter.CHAR).to_dict() == {"word_splitter": "char"}


def test_extraction_v2_errors_preserve_atomic_input_zero():
    client = client_with_transport(
        lambda _: httpx.Response(
            422,
            json={
                "error": "EXTRACTION_SEARCH_EXHAUSTED",
                "message": "no accepted witness",
                "input_index": 0,
                "stage": "decode",
            },
        )
    )
    with pytest.raises(InferenceAPIError) as exc:
        client.extract_v2({"model": "m", "schema": {"entities": ["p"]}, "inputs": [{"content": "Ada"}]})
    assert exc.value.input_index == 0
    assert exc.value.stage == "decode"
    assert exc.value.code == "EXTRACTION_SEARCH_EXHAUSTED"


def test_extraction_v2_response_bound_and_atomic_count_are_enforced():
    request = {"model": "m", "schema": {"entities": ["p"]}, "inputs": [{"content": "Ada"}]}
    oversized = client_with_transport(lambda _: httpx.Response(200, content=b"x" * 100), max_json_response_bytes=32)
    with pytest.raises(AntflyException, match="extraction response exceeded"):
        oversized.extract_v2(request)
    partial = client_with_transport(
        lambda _: httpx.Response(
            200,
            json={
                "object": "extraction",
                "model": "m",
                "schema_version": 2,
                "data": [],
            },
        )
    )
    with pytest.raises(AntflyException, match="item cardinality"):
        partial.extract_v2(request)


def test_extraction_v2_preserves_long_document_and_record_solver_metadata():
    output = {
        "offset_unit": "utf8_bytes",
        "long_document": {
            "version": 1,
            "window_count": 3,
            "window_policy": "source_words_midpoint_ownership",
            "classification_aggregation": "owned_word_weighted_mean_raw_logits",
            "duplicate_score": "maximum_calibrated_score",
            "natural_record_identity": "exact_source_anchor",
            "other_record_identity": "semantic",
            "solver_optimality_scope": "retained_candidate_graph",
        },
        "solvers": {"records": {"status": "feasible", "utility": 1.25, "visited_nodes": 0, "exhausted": True}},
    }

    def handle(request):
        assert json.loads(request.read())["options"]["long_document"] == {
            "mode": "window",
            "record_identity": "semantic",
        }
        return httpx.Response(200, json={"object": "extraction", "model": "m", "schema_version": 2, "data": [output]})

    client = client_with_transport(handle)
    response = client.extract_v2(
        {
            "model": "m",
            "schema": {"entities": ["person"]},
            "inputs": [{"content": "Ada"}],
            "options": {"long_document": {"mode": "window", "record_identity": "semantic"}},
        }
    )
    assert response.data[0].to_dict() == output
    assert response.data[0].long_document.version == 1
    assert response.data[0].solvers.records.exhausted is True


def identified_request():
    return {
        "model": "m",
        "schema": {"entities": ["person"]},
        "inputs": [{"id": "a", "content": "Ada"}, {"id": "b", "content": "Bob"}],
    }


def extraction_envelope(**overrides):
    return {
        "object": "extraction",
        "model": "m",
        "schema_version": 2,
        "data": [{"id": "a", "offset_unit": "utf8_bytes"}, {"id": "b", "offset_unit": "utf8_bytes"}],
        **overrides,
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("object", "embedding"),
        ("object", None),
        ("object", 2),
        ("schema_version", 1),
        ("schema_version", "2"),
        ("schema_version", None),
        ("schema_version", 2.5),
        ("model", "other/model"),
        ("model", None),
        ("model", 4),
        ("data", None),
        ("data", {}),
        ("data", []),
        ("data", [{"id": "a"}]),
        ("data", [{"id": "a"}, {"id": "b"}, {}]),
        ("data", [None, {"id": "b"}]),
        ("data", [[], {"id": "b"}]),
        ("data", ["a", {"id": "b"}]),
        ("data", [{}, {"id": "b"}]),
        ("data", [{"id": "other"}, {"id": "b"}]),
        ("data", [{"id": "b"}, {"id": "a"}]),
        ("data", [{"id": None}, {"id": "b"}]),
        ("data", [{"id": 0}, {"id": "b"}]),
    ],
)
def test_extraction_v2_rejects_invalid_response_envelope(field, value):
    payload = extraction_envelope(**{field: value})
    client = client_with_transport(lambda _: httpx.Response(200, json=payload))
    with pytest.raises(AntflyException, match="extraction returned invalid JSON"):
        client.extract_v2(identified_request())
    with pytest.raises(AntflyException, match="extraction returned invalid JSON"):
        client.extract({**identified_request(), "schema_version": 2})


@pytest.mark.parametrize("field", ["object", "model", "schema_version", "data"])
def test_extraction_v2_rejects_missing_envelope_fields(field):
    payload = extraction_envelope()
    del payload[field]
    client = client_with_transport(lambda _: httpx.Response(200, json=payload))
    with pytest.raises(AntflyException):
        client.extract_v2(identified_request())


@pytest.mark.parametrize("payload", [None, [], "extraction", True])
def test_extraction_v2_rejects_nonobject_envelope(payload):
    client = client_with_transport(lambda _: httpx.Response(200, content=json.dumps(payload)))
    with pytest.raises(AntflyException):
        client.extract_v2(identified_request())


def test_extraction_v2_ids_are_positional_and_extensions_and_offsets_survive():
    payload = extraction_envelope(
        future={"enabled": True},
        data=[
            {
                "id": "repeat",
                "offset_unit": "utf8_bytes",
                "future_row": {"index": 0},
                "entities": [{"label": "person", "text": "Ada", "start": 0, "end": 3, "future_entity": 7}],
            },
            {"id": "repeat", "offset_unit": "utf8_bytes", "entities": [{"label": "person", "text": "Bob"}]},
            {"id": "", "offset_unit": "utf8_bytes"},
            {"offset_unit": "utf8_bytes"},
        ],
    )
    request = identified_request()
    request["inputs"] = [
        {"id": "repeat", "content": "Ada"},
        {"id": "repeat", "content": "Bob"},
        {"id": "", "content": "Eve"},
        {"content": "Max"},
    ]
    response = client_with_transport(lambda _: httpx.Response(200, json=payload)).extract_v2(request)
    assert response.to_dict() == payload
    assert response.data[0].entities[0].start == 0
    assert "start" not in response.data[1].entities[0].to_dict()
    assert "id" not in response.data[3].to_dict()


@pytest.mark.parametrize("value", ["unexpected", None, 0, False, [], {}])
def test_extraction_v2_rejects_present_anonymous_id(value):
    payload = extraction_envelope(data=[{"id": value}])
    request = {**identified_request(), "inputs": [{"content": "Ada"}]}
    client = client_with_transport(lambda _: httpx.Response(200, json=payload))
    with pytest.raises(AntflyException, match="item 0 id"):
        client.extract_v2(request)


def test_extraction_v2_explicit_empty_id_is_not_anonymous_and_legacy_is_unchanged():
    client = client_with_transport(lambda _: httpx.Response(200, json=extraction_envelope(data=[{}])))
    with pytest.raises(AntflyException, match="item 0 id"):
        client.extract_v2({**identified_request(), "inputs": [{"id": "", "content": "Ada"}]})

    legacy = {"object": "extraction", "model": "legacy-model", "data": [{"entities": []}]}
    client = client_with_transport(lambda _: httpx.Response(200, json=legacy))
    assert client.extract(identified_request()).to_dict() == legacy
