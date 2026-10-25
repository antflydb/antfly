import json
from collections.abc import Iterator

import httpx
import pytest

import antfly.client as client_module
from antfly import AntflyClient, AntflyException, InferenceAPIError, InferenceCapacityError
from antfly.client_generated.models import (
    ChunkEmbeddingContext,
    ChunkEmbeddingObject,
    ChunkEmbeddingResponse,
    InferenceChatMessage,
    InferenceGenerateChatTemplateKwargs,
    InferenceGenerateRequest,
    InferenceContextPrepareRequest,
    InferenceContextManagement,
    InferenceContextManagementMode,
    InferenceRole,
    InferenceToolChoiceType0,
)


class ChunkStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks

    def close(self) -> None:
        self.closed = True


def generation_request() -> InferenceGenerateRequest:
    return InferenceGenerateRequest(
        model="gemma",
        messages=[InferenceChatMessage(role=InferenceRole.USER, content="hello")],
    )


def test_generate_request_preserves_explicit_thinking_false() -> None:
    omitted = generation_request().to_dict()
    assert "chat_template_kwargs" not in omitted

    request = generation_request()
    request.chat_template_kwargs = InferenceGenerateChatTemplateKwargs(enable_thinking=False)
    assert request.to_dict()["chat_template_kwargs"] == {"enable_thinking": False}


def test_context_and_generation_requests_preserve_developer_role() -> None:
    wire = {
        "model": "gemma",
        "messages": [
            {"role": "developer", "content": "Preserve these instructions."},
            {"role": "user", "content": "hello"},
        ],
    }
    for request_type in (InferenceContextPrepareRequest, InferenceGenerateRequest):
        request = request_type.from_dict(wire)
        assert request.messages[0].role == InferenceRole.DEVELOPER
        assert request.to_dict()["messages"] == wire["messages"]


def test_prepare_chat_context_forwards_typed_request_and_parses_diagnostics() -> None:
    captured: list[dict[str, object]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.read()))
        return httpx.Response(
            200,
            json={
                "messages": [{"role": "user", "content": "hello"}],
                "context": {
                    "policy": "inspect",
                    "applied": False,
                    "original_input_tokens": 4,
                    "effective_input_tokens": 4,
                    "target_input_tokens": 128,
                    "reserved_output_tokens": 16,
                    "dropped_messages": 0,
                    "retained_messages": 1,
                    "retained_from_message": 0,
                    "checkpoint_applied": False,
                    "windowed": False,
                    "compacted": False,
                },
                "usage": {"prompt_tokens": 4, "completion_tokens": 0, "total_tokens": 4},
            },
        )

    client = AntflyClient("http://test")
    install_transport(client, httpx.MockTransport(handle))
    request = InferenceContextPrepareRequest(
        model="gemma",
        messages=generation_request().messages,
        max_tokens=16,
        tool_choice=InferenceToolChoiceType0.NONE,
        chat_template_kwargs=InferenceGenerateChatTemplateKwargs(enable_thinking=False),
    )
    response = client.prepare_chat_context(request)
    assert response.context.effective_input_tokens == 4
    assert captured == [request.to_dict()]


def install_transport(client: AntflyClient, handler: httpx.MockTransport) -> None:
    generated = client._client
    headers = generated.get_httpx_client().headers
    generated.set_httpx_client(httpx.Client(base_url="http://test", headers=headers, transport=handler))


def test_generate_and_stream_reuse_auth_and_parse_framed_sse() -> None:
    requests: list[dict[str, object]] = []
    stream = ChunkStream(
        [
            bytes([byte])
            for byte in (
                ': heartbeat\r\n\r\nevent: message\r\ndata: {"id":"chatcmpl-stream","object":"chat.completion.chunk",\r\ndata: "created":1,"model":"gemma","choices":[{"index":0,"delta":{"content":"hé🐜"}}]}\r\n\r\ndata: [DONE]\r\n\r\n'
            ).encode()
        ]
    )

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer secret"
        assert request.headers["accept"] in {"application/json", "text/event-stream"}
        requests.append(json.loads(request.read()))
        if requests[-1]["stream"] is True:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream; charset=utf-8"},
                stream=stream,
            )
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-json",
                "object": "chat.completion",
                "created": 1,
                "model": "gemma",
                "choices": [],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            },
        )

    client = AntflyClient("http://test", token="secret")
    install_transport(client, httpx.MockTransport(handle))
    assert client.generate(generation_request()).id == "chatcmpl-json"
    with client.generate_stream(generation_request()) as chunks:
        chunk = next(chunks)
        assert chunk.choices[0].delta.content == "hé🐜"
    assert stream.closed
    assert requests == [
        {**generation_request().to_dict(), "stream": False},
        {**generation_request().to_dict(), "stream": True},
    ]


def test_managed_generation_requires_and_preserves_context_attestation() -> None:
    request = generation_request()
    request.context_management = InferenceContextManagement(
        mode=InferenceContextManagementMode.REJECT
    )
    responses = iter(
        [
            httpx.Response(
                200,
                json={
                    "id": "missing",
                    "object": "chat.completion",
                    "created": 1,
                    "model": "gemma",
                    "choices": [],
                    "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                },
            ),
            httpx.Response(
                200,
                json={
                    "id": "wrong-policy",
                    "object": "chat.completion",
                    "created": 1,
                    "model": "gemma",
                    "choices": [],
                    "context": {
                        "policy": "window",
                        "applied": False,
                        "original_input_tokens": 1,
                        "effective_input_tokens": 1,
                        "target_input_tokens": 8,
                        "reserved_output_tokens": 1,
                        "dropped_messages": 0,
                        "retained_messages": 1,
                        "retained_from_message": 0,
                        "checkpoint_applied": False,
                        "windowed": False,
                        "compacted": False,
                    },
                    "usage": {"prompt_tokens": 1, "completion_tokens": 0, "total_tokens": 1},
                },
            ),
            httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                content=(
                    b'data: {"id":"managed","object":"chat.completion.chunk","created":1,'
                    b'"model":"gemma","choices":[],"context":{"policy":"reject",'
                    b'"applied":false,"original_input_tokens":1,"effective_input_tokens":1,'
                    b'"target_input_tokens":8,"reserved_output_tokens":1,"dropped_messages":0,'
                    b'"retained_messages":1,"retained_from_message":0,"checkpoint_applied":false,'
                    b'"windowed":false,"compacted":false}}\n\ndata: [DONE]\n\n'
                ),
            ),
        ]
    )
    client = AntflyClient("http://test")
    install_transport(client, httpx.MockTransport(lambda _: next(responses)))

    with pytest.raises(AntflyException, match="did not attest"):
        client.generate(request)
    with pytest.raises(AntflyException, match="different context policy"):
        client.generate(request)
    with client.generate_stream(request) as chunks:
        chunk = next(chunks)
        assert chunk.context.policy.value == "reject"
        with pytest.raises(StopIteration):
            next(chunks)


@pytest.mark.parametrize("policy", ["context_management", "kv_cache", "both"])
@pytest.mark.parametrize("attest", [True, False])
def test_managed_stream_accepts_role_and_content_only_after_attestation(policy: str, attest: bool) -> None:
    wire = generation_request().to_dict()
    if policy in ("context_management", "both"):
        wire["context_management"] = {"mode": "reject"}
    if policy in ("kv_cache", "both"):
        wire["kv_cache"] = {"mode": "exact_hybrid", "version": "v1"}
    request = InferenceGenerateRequest.from_dict(wire)
    envelope = {"id": "managed", "object": "chat.completion.chunk", "created": 1, "model": "gemma"}
    diagnostics = {
        "policy": "reject", "applied": False, "original_input_tokens": 1,
        "effective_input_tokens": 1, "target_input_tokens": 8, "reserved_output_tokens": 1,
        "dropped_messages": 0, "retained_messages": 1, "retained_from_message": 0,
        "checkpoint_applied": False, "windowed": False, "compacted": False,
        "kv_cache_mode": "exact_hybrid", "kv_cache_version": "v1",
    }
    events = [{**envelope, "choices": [], "context": diagnostics}] if attest else []
    events += [
        {**envelope, "choices": [{"index": 0, "delta": {"role": "assistant"}}]},
        {**envelope, "choices": [{"index": 0, "delta": {"content": "hello"}}]},
    ]
    stream = ChunkStream([*(f"data: {json.dumps(event)}\n\n".encode() for event in events), b"data: [DONE]\n\n"])
    client = AntflyClient("http://test")
    install_transport(client, httpx.MockTransport(lambda _: httpx.Response(
        200, headers={"Content-Type": "text/event-stream"}, stream=stream,
    )))
    if attest:
        with client.generate_stream(request) as chunks:
            received = list(chunks)
        assert received[0].context.policy.value == "reject"
        assert received[1].choices[0].delta.role.value == "assistant"
        assert received[2].choices[0].delta.content == "hello"
    else:
        with pytest.raises(AntflyException, match="before attesting"):
            with client.generate_stream(request) as chunks:
                list(chunks)
    assert stream.closed


def test_generate_stream_returns_typed_507_and_requires_done() -> None:
    responses = iter(
        [
            httpx.Response(
                507,
                json={
                    "error": "MEMORY_BUDGET_EXCEEDED",
                    "message": "model needs 8 GiB",
                    "retryable": True,
                },
            ),
            httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                content=b'data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gemma","choices":[]}\n\n',
            ),
        ]
    )
    client = AntflyClient("http://test")
    install_transport(client, httpx.MockTransport(lambda _: next(responses)))

    with pytest.raises(InferenceAPIError) as exc_info:
        with client.generate_stream(generation_request()):
            pass
    assert exc_info.value.status_code == 507
    assert exc_info.value.code == "MEMORY_BUDGET_EXCEEDED"
    assert exc_info.value.retryable is True
    assert exc_info.value.detail == "model needs 8 GiB (MEMORY_BUDGET_EXCEEDED)"

    with pytest.raises(AntflyException, match=r"ended before \[DONE\]"):
        with client.generate_stream(generation_request()) as chunks:
            list(chunks)


@pytest.mark.parametrize("reason", ["inference_capacity", "inference_admission", "request_queue"])
def test_generate_returns_typed_capacity_error(reason: str) -> None:
    client = AntflyClient("http://test")
    install_transport(
        client,
        httpx.MockTransport(
            lambda _: httpx.Response(
                503,
                json={
                    "error": "MODEL_RESOURCE_BUSY",
                    "message": "model resources are temporarily busy",
                    "reason": reason,
                    "retryable": True,
                    "retry_after_ms": 1000,
                },
            )
        ),
    )

    with pytest.raises(InferenceCapacityError) as exc_info:
        client.generate(generation_request())
    assert exc_info.value.status_code == 503
    assert exc_info.value.code == "MODEL_RESOURCE_BUSY"
    assert exc_info.value.reason == reason
    assert exc_info.value.retry_after_ms == 1000
    assert exc_info.value.retryable is True


def test_generate_stream_bounds_sse_lines(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "MAX_GENERATION_SSE_LINE_BYTES", 32)
    stream = ChunkStream([b":" + (b"x" * 20), b"x" * 13, b"\n\n"])
    client = AntflyClient("http://test")
    install_transport(
        client,
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=stream,
            )
        ),
    )

    with pytest.raises(AntflyException, match="generation SSE line exceeded 32 bytes"):
        with client.generate_stream(generation_request()) as chunks:
            list(chunks)
    assert stream.closed


def test_generate_bounds_success_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "MAX_GENERATION_RESPONSE_BYTES", 32)
    stream = ChunkStream([b"{" + (b"x" * 20), b"x" * 20 + b"}"])
    client = AntflyClient("http://test")
    install_transport(
        client,
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"Content-Type": "application/json"},
                stream=stream,
            )
        ),
    )

    with pytest.raises(AntflyException, match="generation response exceeded 32 bytes"):
        client.generate(generation_request())
    assert stream.closed


def decision_request() -> dict:
    return {
        "model": "decision-model",
        "input": "Refund the duplicate charge. 🐜",
        "questions": [
            {
                "name": "route",
                "type": "choice",
                "instructions": "Which team?",
                "choices": [
                    {"value": "billing", "description": "Charges"},
                    {"value": "support", "description": "Product"},
                ],
            },
            {
                "name": "urgency",
                "type": "score",
                "instructions": "How urgent?",
                "levels": [{"label": "Routine"}, {"label": "Soon"}, {"label": "Immediate"}],
            },
            {"name": "refund", "type": "predicate", "instructions": "Refund requested?"},
        ],
    }


def decision_response() -> dict:
    return {
        "model": "decision-model",
        "answers": [
            {
                "name": "route",
                "type": "choice",
                "decision_method": "typed",
                "choice": "billing",
                "confidence": 0.5,
                "confidence_method": "normalized_inverse_entropy",
                "act_probability": 0.8,
                "probabilities": [{"value": "billing", "probability": 0.9}, {"value": "support", "probability": 0.1}],
            },
            {
                "name": "urgency",
                "type": "score",
                "decision_method": "typed",
                "score": 1.1,
                "confidence": 0.3,
                "confidence_method": "normalized_inverse_entropy",
                "probabilities": [
                    {"value": 0, "label": "Routine", "probability": 0.1},
                    {"value": 1, "label": "Soon", "probability": 0.7},
                    {"value": 2, "label": "Immediate", "probability": 0.2},
                ],
            },
            {"name": "refund", "type": "predicate", "decision_method": "typed", "probability": 0.95},
        ],
        "usage": {"input_tokens": 20, "output_tokens": 0},
    }


@pytest.mark.parametrize("typed", [False, True])
def test_decide_preserves_requests_auth_and_all_answer_types(typed: bool) -> None:
    from antfly.client_generated.models import InferenceDecideRequest, InferenceDecideResponse

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/ai/v1/decisions"
        assert request.headers["authorization"] == "Bearer secret"
        assert request.headers["accept"] == "application/json"
        assert json.loads(request.read()) == decision_request()
        return httpx.Response(200, json=decision_response())

    client = AntflyClient("http://test", token="secret")
    install_transport(client, httpx.MockTransport(handle))
    body = InferenceDecideRequest.from_dict(decision_request()) if typed else decision_request()
    result = client.decide(body)
    assert isinstance(result, InferenceDecideResponse)
    assert result.to_dict() == decision_response()


@pytest.mark.parametrize(
    "status,code", [(400, "INVALID_REQUEST"), (404, "MODEL_NOT_FOUND"), (413, "REQUEST_TOO_LARGE")]
)
def test_decide_surfaces_api_errors(status: int, code: str) -> None:
    client = AntflyClient("http://test")
    install_transport(
        client, httpx.MockTransport(lambda _: httpx.Response(status, json={"error": code, "message": "detail"}))
    )
    with pytest.raises(InferenceAPIError) as caught:
        client.decide(decision_request())
    assert caught.value.status_code == status
    assert caught.value.code == code


def test_decide_preserves_capacity_retry_metadata() -> None:
    client = AntflyClient("http://test")
    install_transport(
        client,
        httpx.MockTransport(
            lambda _: httpx.Response(
                503,
                headers={"Retry-After": "1"},
                json={
                    "error": "MODEL_RESOURCE_BUSY",
                    "message": "busy",
                    "reason": "inference_capacity",
                    "retryable": True,
                    "retry_after_ms": 1000,
                },
            )
        ),
    )
    with pytest.raises(InferenceCapacityError) as caught:
        client.decide(decision_request())
    assert caught.value.retryable is True
    assert caught.value.retry_after_ms == 1000


@pytest.mark.parametrize("payload", [[], {}, {"model": "m", "answers": None, "usage": {}}])
def test_decide_rejects_malformed_success_responses(payload) -> None:
    client = AntflyClient("http://test")
    install_transport(client, httpx.MockTransport(lambda _: httpx.Response(200, json=payload)))
    with pytest.raises(AntflyException, match="decision returned invalid JSON"):
        client.decide(decision_request())


def test_decide_bounds_response_bytes() -> None:
    client = AntflyClient("http://test", max_json_response_bytes=16)
    install_transport(client, httpx.MockTransport(lambda _: httpx.Response(200, json=decision_response())))
    with pytest.raises(AntflyException, match="decision response exceeded 16 bytes"):
        client.decide(decision_request())


@pytest.mark.parametrize("terminal", [True, False])
def test_chunk_embedding_stream_preserves_provenance_and_requires_terminal(terminal: bool) -> None:
    plan = "1" * 64
    item = {
        "descriptor": {
            "id": f"{plan}:0",
            "ordinal": 0,
            "coordinates": {"kind": "text", "start_byte": 0, "end_byte": 7},
            "input_tokens": 4,
        },
        "mime_type": "text/plain",
        "text": "hé🐜",
        "embedding": [0.6, 0.8],
    }
    coverage = {
        "object": "chunk.embedding.list",
        "data": [],
        "status": "partial",
        "source_fingerprint": "2" * 64,
        "source_version": "v1",
        "plan_id": plan,
        "model": "model",
        "model_identity": "3" * 64,
        "model_revision": "4" * 64,
        "task_type": "RETRIEVAL_DOCUMENT",
        "dimensions": 2,
        "policy": {},
        "start_ordinal": 0,
        "covered_through_ordinal": 1,
        "next_continuation": f"{plan}:1",
        "gaps": [],
    }
    events = [{"object": "chunk.embedding.start", "context": coverage}, {"object": "chunk.embedding", "data": item}]
    if terminal:
        events.append({"object": "chunk.embedding.complete", "response": coverage})
    wire = ("\n".join(json.dumps(value, ensure_ascii=False) for value in events) + "\n").encode()
    stream = ChunkStream([bytes([byte]) for byte in wire])

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer secret"
        assert json.loads(request.read())["stream"] is True
        return httpx.Response(200, headers={"Content-Type": "application/x-ndjson"}, stream=stream)

    client = AntflyClient("http://test", token="secret")
    install_transport(client, httpx.MockTransport(handle))
    with client.chunk_and_embed_stream({"input": "hé🐜", "embedding": {"model": "model"}}) as results:
        header = next(results)
        assert isinstance(header, ChunkEmbeddingContext)
        assert header.model_identity == coverage["model_identity"]
        first = next(results)
        assert isinstance(first, ChunkEmbeddingObject)
        assert first.to_dict() == item
        if terminal:
            complete = next(results)
            assert isinstance(complete, ChunkEmbeddingResponse)
            assert complete.to_dict() == coverage
            with pytest.raises(StopIteration):
                next(results)
        else:
            with pytest.raises(AntflyException, match="before terminal coverage"):
                next(results)
    assert stream.closed


@pytest.mark.parametrize("usage", [None, {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 5}])
def test_generated_generation_response_preserves_unknown_and_measured_usage(usage) -> None:
    from antfly.client_generated.models import InferenceGenerateResponse

    body = {
        "id": "generation",
        "object": "chat.completion",
        "created": 1,
        "model": "google",
        "choices": [],
        "usage": usage,
    }
    assert InferenceGenerateResponse.from_dict(body).to_dict()["usage"] == usage


@pytest.mark.parametrize("extraction", [False, True])
def test_generated_request_models_preserve_nested_configuration(extraction: bool) -> None:
    from antfly.client_generated.models import GeneratedDecideRequest, GeneratedExtractionRequest

    config = {"provider": "antfly", "model": "registered"}
    body = (
        {
            "method": "generated",
            "generator": config,
            "probabilities": "verdict_only",
            "inputs": [{"content": "source"}],
            "schema": {"entities": ["person"]},
        }
        if extraction
        else {
            "decision_method": "generated",
            "generator": config,
            "probabilities": "verdict_only",
            "input": "source",
            "questions": [{"name": "answer", "type": "predicate", "instructions": "Is it present?"}],
        }
    )
    request_model = GeneratedExtractionRequest if extraction else GeneratedDecideRequest
    request = request_model.from_dict(body)
    encoded = request.to_dict()
    assert "model" not in encoded
    assert encoded["generator"] == config

    def handle(incoming: httpx.Request) -> httpx.Response:
        assert incoming.url.path == ("/ai/v1/extract" if extraction else "/ai/v1/decisions")
        assert json.loads(incoming.read()) == encoded
        return httpx.Response(
            200,
            json=(
                {"object": "extraction", "model": "registered", "data": []}
                if extraction
                else {"model": "registered", "answers": [], "usage": {"input_tokens": 0, "output_tokens": 0}}
            ),
        )

    client = AntflyClient("http://test")
    install_transport(client, httpx.MockTransport(handle))
    if extraction:
        assert isinstance(request, GeneratedExtractionRequest)
        result = client.extract(request)
    else:
        assert isinstance(request, GeneratedDecideRequest)
        result = client.decide(request)
    assert result.model == "registered"


def test_tool_call_preserves_provider_metadata_for_replay() -> None:
    from antfly.client_generated.models import ToolCall

    payload = {
        "id": "call-google",
        "type": "function",
        "function": {"name": "lookup", "arguments": '{"id":7}'},
        "extra_content": {"google": {"thought_signature": "opaque-signature"}},
    }
    assert ToolCall.from_dict(payload).to_dict() == payload


def test_anthropic_config_and_signed_assistant_history_roundtrip() -> None:
    from antfly.client_generated.models import (
        AnthropicGeneratorConfig,
        InferenceGenerateMessage,
    )

    config = {"provider": "anthropic", "model": "future-claude", "workspace_id": "workspace"}
    assert AnthropicGeneratorConfig.from_dict(config).to_dict() == config
    payload = {
        "role": "assistant",
        "content": "",
        "tool_calls": [{"id": "call", "type": "function", "function": {"name": "lookup", "arguments": '{"id":7}'}}],
        "extra_content": {
            "anthropic": {
                "content": [
                    {"type": "thinking", "thinking": "private", "signature": "signed"},
                    {"type": "tool_use", "id": "call", "name": "lookup", "input": {"id": 7}},
                ]
            }
        },
    }
    assert InferenceGenerateMessage.from_dict(payload).to_dict() == payload
    assert InferenceChatMessage.from_dict(payload).to_dict() == payload
