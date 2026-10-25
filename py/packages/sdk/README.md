# Antfly Python SDK

[![PyPI version](https://badge.fury.io/py/antfly-sdk.svg)](https://badge.fury.io/py/antfly-sdk)
[![Python](https://img.shields.io/pypi/pyversions/antfly.svg)](https://pypi.org/project/antfly-sdk/)

Python SDK for [Antfly](https://github.com/antflydb/antfly) - a distributed key-value store and search engine.

Supported Python versions: 3.11 through 3.14.

The examples here target the pinned development source below. The inspected
PyPI `0.0.2.dev2` wheel lacks `AntflyClient.query`; TestPyPI `0.0.1` has `query`
but lacks current read-requirement helpers and media chunking. TestPyPI is not a
fallback for this API surface (package contents checked 2026-10-22).

## Installation

```bash
pip install "antfly-sdk @ git+https://github.com/antflydb/antfly.git@ddba6584b38b59c62e261ef63e0374dc1f711617#subdirectory=py/packages/sdk"
```

## Quick Start

```python
from antfly import AntflyClient

# Initialize client
client = AntflyClient(
    base_url="http://localhost:8080",
    username="admin",
    password="password"
)

# Create a table
client.create_table(
    name="users",
    num_shards=4,
    schema={
        "default_type": "user",
        "document_schemas": {
            "user": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"},
                        "age": {"type": "integer"},
                        "created_at": {"type": "string", "format": "date-time"}
                    }
                }
            }
        }
    }
)

# Insert data
client.batch(
    table="users",
    inserts={
        "user:1": {
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30
        },
        "user:2": {
            "name": "Jane Smith",
            "email": "jane@example.com",
            "age": 25
        }
    }
)

# Query data
results = client.query(
    table="users",
    full_text_search={"query": "John"},
    limit=10
)

# Get specific record
user = client.get(table="users", key="user:1")

client.close()
```

Reuse a client across requests to share its HTTP connection pool. A context
manager closes the pool when the scope exits, including when an exception occurs:

```python
with AntflyClient(base_url="http://localhost:8080") as client:
    tables = client.list_tables()
```

Each client admits up to 100 HTTP requests at once and retains up to 100 waiting
callers in FIFO order. Local waiting is limited to one second and the remaining
request timeout. Configure these limits for the application's concurrency:

```python
with AntflyClient(
    base_url="http://localhost:8080",
    max_concurrent_requests=16,
    max_queued_requests=32,
    max_queue_wait=0.25,
    timeout=10,
) as client:
    tables = client.list_tables()
```

Set `max_queued_requests=0` or `max_queue_wait=0` for immediate refusal when all
request slots are occupied. `ClientCapacityError.reason` identifies `queue_full`,
`wait_timeout`, or `deadline_exceeded`; `request_sent` is `False`. An admitted
request is sent once. Queue waiting and deferred write encoding reduce the
remaining HTTPX I/O timeouts. These phase timeouts also apply while reading a response; server query
deadlines and cancellation have their own lifecycle.

A response holds its request slot through body consumption and cleanup.
`chunk_and_embed()` and `chunk_and_embed_stream()` share the same admission owner
as database and other inference operations. Keep `generate_stream()` and
`chunk_and_embed_stream()` contexts open only while consuming their streams.
Closing the client wakes waiting callers and closes the shared connection pool. These limits
apply to one client instance; the server enforces its own execution policy.

## Features

- **Simple API**: Intuitive interface for all Antfly operations
- **Type Safety**: Full type hints for better IDE support
- **Authentication**: Built-in support for basic authentication
- **Error Handling**: Comprehensive error handling with custom exceptions
- **Auto-generated Client**: Based on OpenAPI specification for accuracy

## Native KV checkpoints

The native Gemma 4 E4B CUDA route can export a binary checkpoint and resume it
with the same model revision, tokenizer, template, and KV layout. The streaming
helpers keep checkpoint transfers out of Python's full-response buffers and
publish a downloaded file only after integrity checks succeed.

FP16, FP8 and rotated4 checkpoints passed the bounded E4B Q4_0/L4/eager-CUDA
production qualification at 8K and 32K. The
[qualification record](../../../docs/design/inference/native-kv-compaction.md#bounded-qualification-2026-10-23)
documents the tested model revision, answer-quality coverage and latency limits.

Configure the serving runtime and inference memory budgets using the
[native checkpoint serving guide](../../../zig/pkg/inference/OPENAI.md#portable-native-kv-checkpoints).
The default scratch budget is insufficient for preparation.

```python
from pathlib import Path
from antfly import AntflyClient
from antfly.client_generated.models import InferenceGenerateRequest

client = AntflyClient("http://localhost:8080", timeout=1800)
request = InferenceGenerateRequest.from_dict({
    "model": "google/gemma-4-E4B-it-qat-q4_0-gguf",
    "messages": [{"role": "user", "content": Path("history.txt").read_text()}],
    "max_tokens": 64,
    "temperature": 0,
    "chat_template_kwargs": {"enable_thinking": False},
})
checkpoint = client.prepare_native_kv_checkpoint(request, "history.kv")  # lossless f16 storage
# Optional fitted-history FP8 disk encoding with validated f16 fallback:
# checkpoint = client.prepare_native_kv_checkpoint(request, "history-fp8.kv", encoding="fp8")
# Optional V4 compressed CUDA state; no experimental process switch required:
# checkpoint = client.prepare_native_kv_checkpoint(request, "history-rotated4.kv", encoding="rotated4")
# Use encoding="f32" for legacy V2 readers.
print(checkpoint.metadata["outcome"], checkpoint.metadata["global_savings_ratio"])

# The server can be restarted before this call, using the same model revision.
result = client.resume_native_kv_checkpoint(request, "history.kv")
print(result.choices[0].message.content)
```

Preparation generates no answer. `unchanged_insufficient_savings` means the file
contains the exact original cache because fitting did not meet the savings
threshold. Resume requests supply the full messages; their rendered tokens must
extend the saved prefix exactly. A different model revision or rewritten prefix
is rejected. `resume_native_kv_checkpoint_stream` is a context manager yielding
SSE chunks; exiting its context closes both the upload file and response. Set
the client timeout to cover original prompt ingestion and compaction.

## Development

### Setup

1. Clone the repository
2. Install development dependencies:

```bash
uv sync
```


### Generate Client

The SDK uses an auto-generated client based on the Antfly OpenAPI specification:

```bash
make generate
```

Run `make check-generation-completeness` to fail on any endpoint or schema the generator omits.
The normal generation target remains available while existing upstream incompatibilities are resolved.

### Run Tests

```bash
make test
```

### Build Package

```bash
make build
```

## Documentation

`make docs` will generate the documentation in the `docs/` folder.


## License

Apache License 2.0

## Warning

The strict completeness check currently reports models that
[cannot be autogenerated from the spec](https://github.com/openapi-generators/openapi-python-client/issues/1123).

## Model-aware media chunks

`client.chunk_and_embed` accepts text, PDF, images, GIF, audio and video and
returns typed descriptor/vector pairs:

```python
response = client.chunk_and_embed({
    "input": "A long document…",
    "embedding": {"model": "embeddinggemma2", "task_type": "RETRIEVAL_DOCUMENT"},
    "policy": {"target_tokens": 500, "overlap_tokens": 50},
    "include_content": True,
})
for chunk in response.data:
    print(chunk.descriptor.coordinates, chunk.embedding)
```

Check `status`, `gaps` and `next_continuation` before declaring complete coverage.
Resume with the same request plus `continuation`. For bounded delivery, use
`with client.chunk_and_embed_stream(request) as events:` and consume start
context, chunk objects and the final response. Exiting the context closes the
connection; EOF without terminal coverage raises. Retain the start context's
model identity with vectors. Set `embedding.execute` to `False` for planning only.
Inline media uses `{"type": "media", "mime_type": ..., "data": ...}` with base64
data; immutable HTTP/S3 sources use a version-pinned source reference. See the
[shared contract](../../../docs/plans/model-aware-media-chunking.md) for policies,
security, limits and database integration.
