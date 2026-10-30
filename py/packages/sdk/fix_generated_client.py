#!/usr/bin/env python3
"""Fail-closed fixes for known openapi-python-client 0.28 multi-body output."""

from __future__ import annotations

import re
import sys
from pathlib import Path

FILES = {
    Path("api/query_operations/global_query.py"): 5,
    Path("api/query_operations/query_table.py"): 5,
    Path("api/query_operations/query_namespace_table.py"): 5,
}
# The public HTTP operations intentionally expose the stateful compatibility
# envelope, while the SDK's primary QueryRequest model remains canonical.
# Keep this post-generation check tied to the endpoint body contract rather
# than the canonical model's historical name.
REQUIRED_BODY = re.compile(
    r"(body:\s+(?:GlobalStatefulQueryRequest|StatefulQueryRequest)\s+\|\s+File)"
    r"\s+\|\s+Unset\s*=\s*UNSET"
)
SCOPED_MERGE = Path("api/data_operations/linear_merge.py")
SCOPED_MERGE_BODY = re.compile(
    r"(body:\s+LinearMergeRequest\s+\|\s+ScopedLinearMergeRequest)"
    r"\s+\|\s+Unset\s*=\s*UNSET"
)
SCOPED_MERGE_HEADER = 'headers["Content-Type"] = "application/vnd.antfly.scoped-merge.v1+json"'
NDJSON_HEADER = 'headers["Content-Type"] = "application/x-ndjson"'
RELATIONAL_QUERY = Path("api/data_operations/query_relational_rows.py")
SQL_OPERATIONS = [
    Path(f"api/data_operations/{operation}.py")
    for operation in ("execute_sql", "prepare_sql", "execute_prepared_sql", "close_prepared_sql")
]
NATIVE_CHECKPOINT_RESUME_OPERATIONS = [
    Path("api/default/resume_native_kv_checkpoint.py"),
    Path("api/default/resume_native_kv_checkpoint_batch.py"),
]
EMBED_REQUEST = Path("models/inference_embed_request.py")
NDJSON_RESPONSE = "response_200 = cast(str, response.content)"


def _fix_embedding_input_union(source: str) -> str:
    """Replace ambiguous generated list-union dispatch with discriminated parsing."""
    to_dict_start = source.index("    def to_dict(self) -> dict[str, Any]:")
    from_dict_start = source.index("    @classmethod\n    def from_dict", to_dict_start)
    generated_to_dict = source[to_dict_start:from_dict_start]
    if generated_to_dict.count("if isinstance(self.input_, list):") not in (3, 4):
        raise RuntimeError(f"unexpected generated shape in {EMBED_REQUEST}: to_dict list union")

    input_start = generated_to_dict.index("        input_: dict[str, Any]")
    encoding_start = generated_to_dict.index("        encoding_format:", input_start)
    fixed_input = """        input_: dict[str, Any] | list[dict[str, Any]] | list[str] | str
        if isinstance(self.input_, str):
            input_ = self.input_
        elif isinstance(self.input_, InferenceEmbeddingContentInput):
            input_ = self.input_.to_dict()
        elif isinstance(self.input_, list):
            if all(isinstance(item, str) for item in self.input_):
                input_ = list(self.input_)
            elif all(isinstance(item, (InferenceEmbeddingContentInput, InferenceEmbeddingGroup)) for item in self.input_):
                input_ = [item.to_dict() for item in self.input_]
            elif all(isinstance(item, (TextContentPart, ImageURLContentPart, MediaContentPart)) for item in self.input_):
                input_ = [item.to_dict() for item in self.input_]
            else:
                raise TypeError("input list must contain only strings, content parts, or ordered content inputs")
        else:
            raise TypeError("input must be a string, list, or ordered content input")

        model_identity = self.model_identity

"""
    generated_to_dict = generated_to_dict[:input_start] + fixed_input + generated_to_dict[encoding_start:]
    generated_to_dict = generated_to_dict.replace(
        "        from ..models.inference_embedding_content_input import InferenceEmbeddingContentInput\n",
        "        from ..models.inference_embedding_content_input import InferenceEmbeddingContentInput\n"
        "        from ..models.media_content_part import MediaContentPart\n"
        "        from ..models.inference_embedding_group import InferenceEmbeddingGroup\n",
        1,
    )
    source = source[:to_dict_start] + generated_to_dict + source[from_dict_start:]

    from_dict_start = source.index("    @classmethod\n    def from_dict", to_dict_start)

    parser_start = source.index("        def _parse_input_(", from_dict_start)
    parser_end = source.index("\n        input_ = _parse_input_", parser_start)
    generated_parser = source[parser_start:parser_end]
    if generated_parser.count("if not isinstance(data, list):") not in (3, 4):
        raise RuntimeError(f"unexpected generated shape in {EMBED_REQUEST}: from_dict list union")
    fixed_parser = """        def _parse_input_(
            data: object,
        ) -> (
            InferenceEmbeddingContentInput
            | list[ImageURLContentPart | MediaContentPart | TextContentPart]
            | list[InferenceEmbeddingContentInput | InferenceEmbeddingGroup]
            | list[str]
            | str
        ):
            if isinstance(data, str):
                return data
            if isinstance(data, dict):
                if "content" not in data:
                    raise TypeError("ordered embedding input must contain content")
                return InferenceEmbeddingContentInput.from_dict(data)
            if not isinstance(data, list):
                raise TypeError("input must be a string, list, or ordered content input")
            if all(isinstance(item, str) for item in data):
                return cast(list[str], data)
            if not all(isinstance(item, dict) for item in data):
                raise TypeError("input list must contain only strings or objects")
            object_items = cast(list[dict[str, Any]], data)
            ordered = ["content" in item for item in object_items]
            if any(ordered):
                if not all(ordered):
                    raise TypeError("ordered content inputs cannot be mixed with legacy content parts")
                return [
                    InferenceEmbeddingGroup.from_dict(item) if "title" in item else InferenceEmbeddingContentInput.from_dict(item)
                    for item in object_items
                ]

            content_parts: list[ImageURLContentPart | MediaContentPart | TextContentPart] = []
            for item in object_items:
                kind = item.get("type")
                if kind == "text":
                    content_parts.append(TextContentPart.from_dict(item))
                elif kind == "image_url":
                    content_parts.append(ImageURLContentPart.from_dict(item))
                elif kind == "media":
                    content_parts.append(MediaContentPart.from_dict(item))
                else:
                    raise TypeError(f"unsupported content part type: {kind!r}")
            return content_parts
"""
    return source[:parser_start] + fixed_parser + source[parser_end:]


def fix_generated_client(root: Path) -> None:
    updates: dict[Path, str] = {}
    for relative, expected in FILES.items():
        path = root / relative
        source = path.read_text(encoding="utf-8")
        count = len(REQUIRED_BODY.findall(source))
        if count != expected or source.count(NDJSON_HEADER) != 1:
            raise RuntimeError(
                f"unexpected generated shape in {relative}: "
                f"required-body signatures={count}, NDJSON headers={source.count(NDJSON_HEADER)}"
            )
        updates[path] = REQUIRED_BODY.sub(r"\1", source)

    path = root / SCOPED_MERGE
    source = path.read_text(encoding="utf-8")
    if len(SCOPED_MERGE_BODY.findall(source)) != 5 or source.count(SCOPED_MERGE_HEADER) != 1:
        raise RuntimeError(f"unexpected generated shape in {SCOPED_MERGE}: required scoped merge body")
    updates[path] = SCOPED_MERGE_BODY.sub(r"\1", source)

    # The generator treats unknown NDJSON media as binary while annotating
    # the schema as str. Decode text, without JSON parsing or integer coercion.
    path = root / RELATIONAL_QUERY
    source = path.read_text(encoding="utf-8")
    if source.count(NDJSON_RESPONSE) != 1:
        raise RuntimeError(f"unexpected generated shape in {RELATIONAL_QUERY}: NDJSON response")
    updates[path] = source.replace(NDJSON_RESPONSE, "response_200 = response.text")

    # Keep raw generated SQL entry points bounded as well. Match exactly once
    # per sync/async call and fail generation if upstream changes this shape.
    for operation in SQL_OPERATIONS:
        path = root / operation
        source = path.read_text(encoding="utf-8")
        for original, replacement in (
            (
                "from ...client import",
                "from ....sql_transport import sql_request, sql_request_async\nfrom ...client import",
            ),
            ("response = client.get_httpx_client().request(", "response = sql_request(client.get_httpx_client(),"),
            (
                "response = await client.get_async_httpx_client().request(",
                "response = await sql_request_async(client.get_async_httpx_client(),",
            ),
        ):
            if source.count(original) != 1:
                raise RuntimeError(f"unexpected generated shape in {operation}: {original}")
            source = source.replace(original, replacement)
        updates[path] = source

    # HTTPX AsyncClient cannot consume the generated synchronous File stream.
    # Keep native checkpoint uploads bounded through the handwritten adapter.
    for operation in NATIVE_CHECKPOINT_RESUME_OPERATIONS:
        path = root / operation
        source = path.read_text(encoding="utf-8")
        if source.count('_kwargs["content"] = body.payload') != 1:
            raise RuntimeError(f"unexpected generated shape in {operation}: binary file body")
        for original, replacement in (
            (
                "from ...client import",
                "from ....native_kv_transport import native_kv_request_async\nfrom ...client import",
            ),
            (
                "response = await client.get_async_httpx_client().request(",
                "response = await native_kv_request_async(client.get_async_httpx_client(),",
            ),
        ):
            if source.count(original) != 1:
                raise RuntimeError(f"unexpected generated shape in {operation}: {original}")
            source = source.replace(original, replacement)
        updates[path] = source

    path = root / EMBED_REQUEST
    updates[path] = _fix_embedding_input_union(path.read_text(encoding="utf-8"))

    for path, source in updates.items():
        path.write_text(source, encoding="utf-8")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} GENERATED_CLIENT_ROOT")
    fix_generated_client(Path(sys.argv[1]))
