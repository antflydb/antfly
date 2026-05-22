// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Inference client abstraction layer.
//
// Provides a provider-neutral interface for ML inference (embeddings,
// chat/generation, reranking) with implementations for:
//   - Termite (local ONNX inference, binary embedding format)
//   - Gemini / Vertex AI (native multimodal embedding APIs)
//   - OpenAI (also works with Ollama, vLLM, and any OpenAI-compatible API)

const std = @import("std");

pub const types = @import("types.zig");
pub const termite = @import("termite.zig");
pub const gemini = @import("gemini.zig");
pub const openai = @import("openai.zig");
pub const managed_embedder = @import("managed_embedder.zig");

pub const Embedder = types.Embedder;
pub const Generator = types.Generator;
pub const Reranker = types.Reranker;
pub const EmbedResult = types.EmbedResult;
pub const SparseEmbedResult = types.SparseEmbedResult;
pub const GenerateResult = types.GenerateResult;
pub const RerankResult = types.RerankResult;
pub const ChatMessage = types.ChatMessage;
pub const Role = types.Role;

test "inference module compiles" {
    _ = types;
    _ = termite;
    _ = gemini;
    _ = openai;
    _ = managed_embedder;
    std.testing.refAllDecls(gemini);
}

test "gemini native embedding request helpers" {
    try gemini.testRequestBodyPreservesTextAndInlineBinaryParts();
    try gemini.testEndpointUrlNormalizesModelResourceNames();
}

test "gemini native embedding provider embeds through mock server" {
    try gemini.testProviderEmbedsThroughMockServer();
}

test "managed embedder parses google embedding providers" {
    try managed_embedder.testParseGeminiApiEntry();
    try managed_embedder.testParseExplicitVertexEntryWithCredentialsPath();
}

test "managed embedder resolves file-backed api key rotation at request time" {
    try managed_embedder.testFileBackedApiKeyRotation();
}
