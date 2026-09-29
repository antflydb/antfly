#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reject ELv2 source dependencies in the Apache embedded engine owners."""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

from asset_licenses import check_asset_records, check_embedded_asset
from license_headers import (
    APACHE_FILES,
    ROOT,
    apply_header,
    excluded,
    group_for,
    read_header,
)

SOURCE_ROOT = "zig/pkg/antfly/src/"
ENTRYPOINTS = (
    "runtime_lite_kernel_root.zig",
    "runtime_inference_root.zig",
    "runtime_inference_main.zig",
    "runtime_enrichment_compute_root.zig",
    "lite_main.zig",
    "embedded_root.zig",
    "runtime_memory_abi.zig",
    "runtime_failure_abi.zig",
    "runtime_failure_identity.zig",
    "storage/kernel_owner_abi.zig",
    "storage/local_query_client.zig",
    "storage/enrichment_compute_abi.zig",
)
PACKAGE_ENTRYPOINTS = (
    "zig/pkg/antfly-embedded/src/root.zig",
    "zig/pkg/antfly-embedded/src/engine/root.zig",
    "zig/examples/antfly_wasm.zig",
    "zig/pkg/inference/src/main.zig",
    "zig/pkg/inference/src/wasm_entry_wasm32.zig",
    "zig/pkg/inference/src/wasm_entry_wasm64.zig",
)
# Source modules are resolved to checked-in entrypoints, never accepted as
# opaque names. Multiple roots conservatively cover a module's source variants.
# Generated data modules resolve to their producers. Only Zig's standard library
# and build-generated configuration constants terminate traversal.
SOURCE_MODULES = {
    "antfly_admin_openapi": (
        "zig/pkg/antfly-server-api/src/openapi/generated/antfly_admin_openapi/root.zig",
    ),
    "antfly_internal_openapi": (
        "zig/pkg/antfly-server-api/src/openapi/generated/antfly_internal_openapi/root.zig",
    ),
    "antfly_cancellation": ("zig/lib/runtime/src/cancellation.zig",),
    "antfly_cache_budget": ("zig/lib/runtime/src/cache_budget.zig",),
    "antfly_runtime_abi": ("zig/lib/runtime/src/root.zig",),
    "antfly_private_error_diagnostics": ("zig/lib/runtime/src/private_error_diagnostics.zig",),
    "antfly_inference_bridge": ("zig/pkg/inference/src/host/bridge.zig",),
    "antfly_inference_provider_failure": ("zig/pkg/inference/src/host/provider_failure.zig",),
    "antfly_runtime_fs": ("zig/lib/runtime/src/fs.zig",),
    "antfly_provision_contract": ("zig/pkg/antfly/src/metadata/provision_contract.zig",),
    "antfly_read_state_observer": ("zig/lib/raft/src/read_state_observer.zig",),
    "antfly_inference_worker_wire": ("zig/pkg/inference/src/host/worker_wire.zig",),
    "antfly_public_limits": ("zig/pkg/antfly-embedded/src/api/public_limits.zig",),
    "antfly_template_content": ("zig/lib/template/src/content_part.zig",),
    "antfly_sparse_embedding": ("zig/pkg/inference/src/host/sparse_embedding.zig",),
    "antfly_data_uri": ("zig/lib/scraping/src/data_uri.zig",),
    "antfly_websearch_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_websearch_openapi/root.zig",
    ),
    "antfly_usermgr_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_usermgr_openapi/root.zig",
    ),
    "antfly_generating_api_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_generating_api_openapi/root.zig",
    ),
    "antfly_eval_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_eval_openapi/root.zig",
    ),
    "antfly_graph_identifier_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_graph_identifier_openapi/root.zig",
    ),
    "antfly-client": ("zig/pkg/antfly-client/src/root.zig",),
    "antfly-json": ("zig/lib/json/src/mod.zig",),
    "antfly_audio_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_audio_openapi/root.zig",
    ),
    "antfly_casbin": ("zig/lib/casbin/src/mod.zig",),
    "antfly_chunking": ("zig/lib/chunking/src/mod.zig",),
    "antfly_chunking_api_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_chunking_api_openapi/root.zig",
    ),
    "antfly_chunking_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_chunking_openapi/root.zig",
    ),
    "antfly_client_openapi": (
        "zig/pkg/antfly-client/src/openapi/generated/antfly_client_openapi/root.zig",
    ),
    "antfly_common_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_common_openapi/root.zig",
    ),
    "antfly_credentials": ("zig/lib/credentials/src/root.zig",),
    "antfly_embedded_api": ("zig/pkg/antfly-embedded/src/api.zig",),
    "antfly_embedded_db": ("zig/pkg/antfly-embedded/src/db.zig",),
    "antfly_embeddings": ("zig/lib/embeddings/src/mod.zig",),
    "antfly_embeddings_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_embeddings_openapi/root.zig",
    ),
    "antfly_extracting": ("zig/lib/extracting/src/mod.zig",),
    "antfly_extraction_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_extraction_openapi/root.zig",
    ),
    "antfly_font": ("zig/lib/font/src/mod.zig",),
    "antfly_fst": ("zig/lib/fst/src/mod.zig",),
    "antfly_generating": ("zig/lib/generating/src/mod.zig",),
    "antfly_generating_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_generating_openapi/root.zig",
    ),
    "antfly_google": ("zig/lib/google/src/root.zig",),
    "antfly_hash": ("zig/lib/hash/src/mod.zig",),
    "antfly_image": ("zig/lib/image/src/mod.zig",),
    "antfly_indexes_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_indexes_openapi/root.zig",
    ),
    "antfly_inference_config_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_inference_config_openapi/root.zig",
    ),
    "antfly_inference_worker_rpc": ("zig/pkg/inference/src/host/worker_rpc.zig",),
    "antfly_inference_embedding_wire": ("zig/pkg/inference/src/host/embedding_wire.zig",),
    "antfly_inference_types": ("zig/pkg/inference/src/host/types.zig",),
    "antfly_inference_work": ("zig/pkg/inference/src/host/work.zig",),
    "antfly_inference_openai": ("zig/pkg/antfly-embedded/src/inference/providers/openai.zig",),
    "antfly_inference_provider_defaults": ("zig/pkg/antfly-embedded/src/inference/providers/provider_defaults.zig",),
    "antfly_inference_bedrock": ("zig/pkg/antfly-embedded/src/inference/providers/bedrock.zig",),
    "antfly_inference_local": ("zig/pkg/antfly-embedded/src/inference/providers/local.zig",),
    "antfly_inference_list_models": ("zig/pkg/antfly-embedded/src/inference/providers/list_models.zig",),
    "antfly_inference_vertex": ("zig/pkg/antfly-embedded/src/inference/providers/vertex.zig",),
    "antfly_inference_remote_capabilities": ("zig/pkg/antfly-embedded/src/inference/remote_capabilities.zig",),
    "antfly_inference_execution_control": ("zig/pkg/inference/src/host/execution_control.zig",),
    "antfly_inference_execution_context": ("zig/pkg/antfly-embedded/src/inference/execution_context.zig",),
    "antfly_inference_request_types": ("zig/pkg/inference/src/host/request_types.zig",),
    "antfly_inference_runtime_paths": ("zig/pkg/inference/src/host/runtime_paths.zig",),
    "antfly_inference_host": ("zig/pkg/inference/src/host/host.zig",),
    "antfly_inference_query_embedding_cache": ("zig/pkg/antfly-embedded/src/inference/providers/query_embedding_cache.zig",),
    "antfly_jsonschema": ("zig/lib/jsonschema/src/mod.zig",),
    "antfly_logging_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_logging_openapi/root.zig",
    ),
    "antfly_matcher": ("zig/lib/matcher/src/mod.zig",),
    "antfly_metadata_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_metadata_openapi/root.zig",
    ),
    "antfly_middleware_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_middleware_openapi/root.zig",
    ),
    "antfly_pdf": ("zig/lib/pdf/src/mod.zig",),
    "antfly_platform": ("zig/lib/platform/src/root.zig",),
    "antfly_provider_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_provider_openapi/root.zig",
    ),
    "antfly_public_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_public_openapi/root.zig",
    ),
    "antfly_query_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_query_openapi/root.zig",
    ),
    "antfly_reader_config": ("zig/lib/readers/src/config.zig",),
    "antfly_readers": ("zig/lib/readers/src/mod.zig",),
    "antfly_regex": ("zig/lib/regex/src/mod.zig",),
    "antfly_reranking": ("zig/lib/reranking/src/mod.zig",),
    "antfly_reranking_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_reranking_openapi/root.zig",
    ),
    "antfly_resolver": ("zig/lib/resolver/src/mod.zig",),
    "antfly_root": ("zig/pkg/antfly/src/lite_capi_root.zig",),
    "antfly_s3_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_s3_openapi/root.zig",
    ),
    "antfly_schema_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_schema_openapi/root.zig",
    ),
    "antfly_scraping": ("zig/lib/scraping/src/mod.zig",),
    "antfly_scraping_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_scraping_openapi/root.zig",
    ),
    "antfly_sort_openapi": (
        "zig/pkg/antfly-embedded/src/openapi/generated/antfly_sort_openapi/root.zig",
    ),
    "antfly_source_root": ("zig/pkg/antfly/src/source_owner_lite.zig",),
    "antfly_storage_root": ("zig/pkg/antfly/src/lite_capi_root.zig",),
    "antfly_synthesizing": ("zig/lib/synthesizing/src/mod.zig",),
    "antfly_transcribing": ("zig/lib/transcribing/src/mod.zig",),
    "antfly_vector": ("zig/lib/vector/src/mod.zig",),
    "antfly_vectorindex": ("zig/lib/vectorindex/src/mod.zig",),
    "bloom": ("zig/lib/bloom/src/mod.zig",),
    "build_info": ("zig/lib/build_info/src/root.zig",),
    "cuda_jit_identity": ("zig/pkg/inference/tools/jit_identity.zig",),
    "embedded_api_surface": ("zig/pkg/antfly-embedded/src/engine/api.zig",),
    "embedded_db_surface": ("zig/pkg/antfly-embedded/src/engine/db.zig",),
    "embedded_support": ("zig/pkg/antfly/src/embedded_root.zig",),
    "embedded_surface": ("zig/pkg/antfly-embedded/src/engine/root.zig",),
    "enrichment_compute_abi": (
        "zig/pkg/antfly/src/storage/enrichment_compute_abi.zig",
    ),
    "handlebars": ("zig/lib/handlebars/src/handlebars.zig",),
    "httpx": ("zig/lib/httpx/src/httpx.zig",),
    "inference": ("zig/pkg/inference/src/inference_internal.zig",),
    "inference_api": ("zig/pkg/inference/src/api/generated/inference_api/root.zig",),
    "inference_audio": ("zig/lib/audio/src/mod.zig",),
    "inference_chunker": ("zig/lib/chunker/src/mod.zig",),
    "inference_client": ("zig/pkg/inference-client/src/root.zig",),
    "inference_finetune_assets": (
        "zig/pkg/inference/src/finetune_assets_colqwen2.zig",
        "zig/pkg/inference/src/finetune_assets_entity_cleanup.zig",
        "zig/pkg/inference/src/finetune_assets_gemma4.zig",
        "zig/pkg/inference/src/finetune_assets_gliner2.zig",
        "zig/pkg/inference/src/finetune_assets_gliner2_run_validation.zig",
        "zig/pkg/inference/src/finetune_assets_layoutlmv3.zig",
        "zig/pkg/inference/src/finetune_assets_manifest.zig",
        "zig/pkg/inference/src/finetune_assets_peft.zig",
        "zig/pkg/inference/src/finetune_assets_reranker_head.zig",
        "zig/pkg/inference/src/finetune_assets_reranker_lora.zig",
    ),
    "inference_finetune_data": ("zig/pkg/inference/src/finetune_data_root.zig",),
    "inference_fixed_tokenizer_data": ("zig/lib/tokenizer/build_support.zig",),
    "inference_hf_tokenizer": ("zig/lib/tokenizer/src/hf_root.zig",),
    "inference_internal": ("zig/pkg/inference/src/inference_internal.zig",),
    "inference_linalg": ("zig/lib/linalg/src/mod.zig",),
    "inference_runtime": ("zig/pkg/inference/src/wasm_entry.zig",),
    "inference_server": ("zig/pkg/inference/src/inference.zig",),
    "inference_tokenizer": ("zig/lib/tokenizer/src/tokenizer.zig",),
    "jinja": ("zig/lib/jinja/src/jinja.zig",),
    "kernel_error_identity": ("zig/pkg/antfly/src/runtime_failure_identity.zig",),
    "kernel_owner_abi": ("zig/pkg/antfly/src/storage/kernel_owner_abi.zig",),
    "lmdb_engine": ("zig/lib/lmdb/src/root.zig",),
    "local_query_client": ("zig/pkg/antfly/src/storage/local_query_client.zig",),
    "metal_jit_identity": ("zig/pkg/inference/tools/jit_identity.zig",),
    "ml": ("zig/lib/ml/src/root.zig",),
    "ml_tabular": ("zig/lib/ml/tabular/src/root.zig",),
    "objectstore": ("zig/lib/objectstore/src/root.zig",),
    "onnx_data": ("zig/lib/onnx/src/data.zig",),
    "onnx_graph": ("zig/lib/onnx/src/root.zig",),
    "openai_api": ("zig/pkg/antfly-embedded/src/openapi/generated/openai_api/root.zig",),
    "pdf_standard_fonts": ("zig/pdf_standard_fonts.zig",),
    "pjrt": ("zig/lib/pjrt/src/root.zig",),
    "prometheus": ("zig/lib/prometheus/src/root.zig",),
    "protobuf": ("zig/lib/protobuf/src/root.zig",),
    "raft_engine": ("zig/lib/raft/src/root.zig",),
    "runtime_failure_abi": ("zig/pkg/antfly/src/runtime_failure_abi.zig",),
    "runtime_failure_identity": ("zig/pkg/antfly/src/runtime_failure_identity.zig",),
    "runtime_memory_abi": ("zig/pkg/antfly/src/runtime_memory_abi.zig",),
    "sentencepiece_proto": (
        "zig/lib/protobuf/src/codegen_main.zig",
        "zig/lib/tokenizer/tools/patch_sentencepiece_proto.zig",
    ),
    "snowball": ("zig/pkg/antfly/src/search/snowball/generated/root.zig",),
    "structlog": ("zig/lib/structlog/src/root.zig",),
    "usermgr_storage": ("zig/pkg/antfly/src/usermgr/storage_imports.zig",),
    "vopr": ("zig/lib/vopr/src/root.zig",),
    "xla_proto": ("zig/lib/pjrt/proto/xla_proto_stub.zig",),
}
CONFIGURATION_IMPORTS = {
    "standalone_runtime_options",
    "antfly_lite_options",
    "build_options",
    "capi_build_options",
    "storage_source_options",
    "std",
    "builtin",
}
SERVER_ENTRYPOINTS = (
    "main.zig",
    "runtime_distributed_root.zig",
    "runtime_artifact_main.zig",
    "runtime_api_kernel_root.zig",
    "runtime_serverless_root.zig",
    "standalone/runtime.zig",
    "metadata/reconciler.zig",
    "data/runtime.zig",
)


class ZigToken(NamedTuple):
    kind: str
    text: str
    offset: int


class Dependency(NamedTuple):
    kind: str
    path: str | None
    offset: int


def zig_tokens(text: str) -> Iterator[ZigToken]:
    """Scan once; comments and literal contents never become code tokens."""
    index = 0
    while index < len(text):
        start = index
        char = text[index]
        if char.isspace():
            index += 1
            continue
        if text.startswith("//", index):
            end = text.find("\n", index)
            index = len(text) if end == -1 else end + 1
            continue
        if text.startswith("\\\\", index):
            end = text.find("\n", index)
            index = len(text) if end == -1 else end
            yield ZigToken("multiline", text[start:index], start)
            continue
        if char in ('"', "'"):
            index += 1
            while index < len(text) and text[index] not in (char, "\n"):
                if text[index] == "\\":
                    index += 1  # Escape contents cannot terminate this literal.
                index += 1
            terminated = index < len(text) and text[index] == char
            if terminated:
                index += 1
            kind = "string" if char == '"' and terminated else "literal"
            yield ZigToken(kind, text[start:index], start)
            continue
        if char == "@":
            index += 1
            while index < len(text) and (
                text[index].isascii() and (text[index].isalnum() or text[index] == "_")
            ):
                index += 1
            yield ZigToken("builtin", text[start:index], start)
            continue
        if char.isalnum() or char == "_":
            index += 1
            while index < len(text) and (text[index].isalnum() or text[index] == "_"):
                index += 1
            yield ZigToken("code", text[start:index], start)
            continue
        index += 1
        yield ZigToken("code", char, start)


def source_dependencies(text: str) -> Iterator[Dependency]:
    tokens = iter(zig_tokens(text))
    for token in tokens:
        if token.kind != "builtin" or token.text not in ("@import", "@embedFile"):
            continue
        kind = token.text[1:]
        opening = next(tokens, None)
        path = None
        if opening is not None and opening.text == "(":
            argument = next(tokens, None)
            closing = next(tokens, None)
            # Zig accepts a trailing comma after the sole argument.
            if closing is not None and closing.text == ",":
                closing = next(tokens, None)
            if (
                argument is not None
                and argument.kind == "string"
                and closing is not None
                and closing.text == ")"
                and "\\" not in argument.text
                and len(argument.text) > 2
            ):
                path = argument.text[1:-1]
        # Unsupported expressions or escaped paths remain fail-closed.
        yield Dependency(kind, path, token.offset)


def source_imports(text: str) -> list[str]:
    return [
        dependency.path
        for dependency in source_dependencies(text)
        if dependency.kind == "import" and dependency.path is not None
    ]


def source_embeds(text: str) -> list[str]:
    return [
        dependency.path
        for dependency in source_dependencies(text)
        if dependency.kind == "embedFile" and dependency.path is not None
    ]


def has_elv2_notice(source: str) -> bool:
    return bool(
        re.search(
            r"(?:Licensed under|SPDX-License-Identifier:)[^\n]*(?:Elastic|ELv2)",
            "\n".join(source.splitlines()[:40]),
        )
    )


def check_sources(root: Path = ROOT) -> tuple[set[str], list[str]]:
    root = root.resolve()
    pending = [(SOURCE_ROOT + name, None) for name in ENTRYPOINTS]
    pending.extend((name, None) for name in PACKAGE_ENTRYPOINTS)
    seen: set[str] = set()
    errors: list[str] = []
    while pending:
        name, parent = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        path = (root / name).resolve()
        if not path.is_relative_to(root):
            errors.append(f"source outside product tree: {name} (imported by {parent})")
            continue
        if not path.is_file():
            errors.append(f"missing source: {name} (imported by {parent})")
            continue
        if name.startswith("zig/pkg/antfly-server-api/"):
            errors.append(f"embedded product imports server-only API: {name} (imported by {parent})")
            continue
        third_party = name.startswith(SOURCE_ROOT + "search/snowball/generated/")
        if not third_party and group_for(name, "all") != "apache":
            errors.append(f"non-Apache dependency: {name} (imported by {parent})")
            continue
        source = path.read_text()
        if not third_party and has_elv2_notice(source):
            errors.append(f"conflicting ELv2 notice in Apache source: {name}")
        dependencies = list(source_dependencies(source))
        for dependency in dependencies:
            if dependency.path is None:
                line = source.count("\n", 0, dependency.offset) + 1
                errors.append(
                    f"unresolved @{dependency.kind} dependency: {name}:{line}; use a literal path"
                )
        for embedded in (
            dependency.path
            for dependency in dependencies
            if dependency.kind == "embedFile" and dependency.path is not None
        ):
            asset = (path.parent / embedded).resolve()
            if not asset.is_relative_to(root) or not asset.is_file():
                errors.append(f"missing or external embedded asset: {name}: {embedded}")
                continue
            asset_name = asset.relative_to(root).as_posix()
            error = check_embedded_asset(
                asset_name, group_for(asset_name, "all") == "apache", root
            )
            if error:
                errors.append(f"{error} (embedded by {name})")
        for imported in (
            dependency.path
            for dependency in dependencies
            if dependency.kind == "import" and dependency.path is not None
        ):
            if imported.endswith(".zig"):
                dependency = (path.parent / imported).resolve()
                if not dependency.is_relative_to(root.resolve()):
                    errors.append(f"source outside product tree: {name}: {imported}")
                else:
                    pending.append(
                        (dependency.relative_to(root.resolve()).as_posix(), name)
                    )
            elif imported in SOURCE_MODULES:
                pending.extend((source, name) for source in SOURCE_MODULES[imported])
            elif imported not in CONFIGURATION_IMPORTS:
                errors.append(f"unreviewed module dependency: {name}: {imported}")
    return seen, errors


def main() -> int:
    seen, errors = check_sources()
    errors.extend(check_asset_records())
    for name in SERVER_ENTRYPOINTS:
        path = SOURCE_ROOT + name
        if group_for(path, "all") != "elv2":
            errors.append(f"server entry point must remain ELv2: {path}")
    apache_header = read_header("apache")
    for name in sorted(APACHE_FILES):
        path = ROOT / name
        if not path.is_file():
            errors.append(f"stale Apache license entry: {name}")
        elif not excluded(name):
            source = path.read_text()
            if apply_header(source, path, apache_header) != source:
                errors.append(f"missing or stale Apache license header: {name}")
            if has_elv2_notice(source):
                errors.append(f"conflicting ELv2 notice in Apache source: {name}")
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print(
        f"Apache product boundary verified: {len(seen)} source files; server entry points remain ELv2"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
